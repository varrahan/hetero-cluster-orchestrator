package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

const (
	annotationPrefix          = "orchestration.gputpu.io/"
	inventoryAnnotation       = annotationPrefix + "inventory-v1"
	inventoryHashAnnotation   = annotationPrefix + "inventory-hash"
	inventoryBootIDAnnotation = annotationPrefix + "inventory-boot-id"
	recoveryIncident          = annotationPrefix + "recovery-incident"
	recoveryPhase             = annotationPrefix + "recovery-phase"
	rebootRequest             = annotationPrefix + "reboot-request"
	rebootAck                 = annotationPrefix + "reboot-ack"
	hardwareDegraded          = "HardwareDegraded"
	defaultSocket             = "/run/gputpu-watchdog/watchdog.sock"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("watchdog stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	socket := cmp.Or(os.Getenv("WATCHDOG_SOCKET"), defaultSocket)
	if len(args) > 0 && args[0] == "probe" {
		return runProbe(socket)
	}
	if len(args) != 0 {
		return fmt.Errorf("usage: watchdog-daemon [probe]")
	}
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return errors.New("NODE_NAME is required")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	events := &kernelEventState{}
	go events.watch(ctx, "/dev/kmsg")
	checker, err := newHealthChecker(client, nodeName, events)
	if err != nil {
		return err
	}
	go checker.run(ctx)
	go watchReboot(ctx, client, nodeName)
	return serve(ctx, checker, socket)
}

func serve(ctx context.Context, checker *healthChecker, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen on watchdog socket: %w", err)
	}
	defer listener.Close()
	if err := os.Chmod(path, 0660); err != nil {
		return fmt.Errorf("set watchdog socket permissions: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, checker.result())
	})
	mux.HandleFunc("GET /v1/inventory", func(response http.ResponseWriter, _ *http.Request) {
		result := checker.result()
		writeJSON(response, map[string]string{"bootID": result.BootID, "inventory": result.Inventory, "hash": result.InventoryHash})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8 << 10}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runProbe(path string) error {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "unix", path)
	}}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	response, err := client.Get("http://watchdog/v1/health")
	if err != nil {
		fmt.Fprintln(os.Stdout, "watchdog unavailable")
		os.Exit(2)
	}
	defer response.Body.Close()
	var result healthResult
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&result) != nil {
		fmt.Fprintln(os.Stdout, "watchdog response invalid")
		os.Exit(2)
	}
	fmt.Fprintln(os.Stdout, result.Reason)
	switch result.Status {
	case healthHealthy:
		return nil
	case healthUnhealthy:
		os.Exit(1)
	default:
		os.Exit(2)
	}
	return nil
}

func watchReboot(ctx context.Context, client kubernetes.Interface, nodeName string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := rebootOnce(ctx, client, nodeName); err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("reboot check failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func rebootOnce(ctx context.Context, client kubernetes.Interface, nodeName string) error {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	request := node.Annotations[rebootRequest]
	if request == "" || request == node.Annotations[rebootAck] || request != node.Annotations[recoveryIncident] || node.Annotations[recoveryPhase] != "RebootRequested" || !conditionTrue(node.Status.Conditions, hardwareDegraded) {
		return nil
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName, LabelSelector: "app.kubernetes.io/component=slurmd,app.kubernetes.io/managed-by=slurm-operator"})
	if err != nil {
		return fmt.Errorf("list worker Pods: %w", err)
	}
	if len(pods.Items) != 0 {
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.Annotations[rebootRequest] != request || current.Annotations[rebootAck] == request {
			return nil
		}
		copy := current.DeepCopy()
		copy.Annotations[rebootAck] = request
		_, err = client.CoreV1().Nodes().Update(ctx, copy, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("acknowledge reboot: %w", err)
	}
	if strings.EqualFold(os.Getenv("WATCHDOG_REBOOT_DRY_RUN"), "true") {
		slog.Info("reboot acknowledged in dry-run mode", "incident", request)
		return nil
	}
	unix.Sync()
	if err := unix.Reboot(unix.LINUX_REBOOT_CMD_RESTART); err != nil {
		return fmt.Errorf("reboot host: %w", err)
	}
	return nil
}

func conditionTrue(conditions []corev1.NodeCondition, conditionType string) bool {
	for _, condition := range conditions {
		if string(condition.Type) == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func writeJSON(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
}
