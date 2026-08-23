package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/containerd/nri/pkg/stub"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

const (
	driverName = "orchestration.gputpu.io"
	stateDir   = "/var/lib/kubelet/plugins/orchestration.gputpu.io/state"
	cdiDir     = "/var/run/cdi"
)

func Run() error {
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

	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read node %q: %w", nodeName, err)
	}
	inv, err := discoverInventory("/sys", nodeName, node.Annotations)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cdiDir, 0755); err != nil {
		return fmt.Errorf("create CDI directory: %w", err)
	}
	if err := writeOpenTPUCDI(filepath.Join(cdiDir, "orchestration-opentpu.json"), inv); err != nil {
		return err
	}
	if inv.hasKind(kindGPU) {
		if err := generateNVIDIACDI(filepath.Join(cdiDir, "orchestration-nvidia.yaml"), inv); err != nil {
			slog.Warn("NVIDIA inventory withheld because CDI generation failed", "error", err)
			inv.removeKind(kindGPU)
		}
	}

	plugin, err := newNodePlugin(stateDir, inv)
	if err != nil {
		return err
	}
	if err := plugin.recover(ctx, client); err != nil {
		return err
	}
	helper, err := kubeletplugin.Start(ctx, plugin,
		kubeletplugin.DriverName(driverName),
		kubeletplugin.KubeClient(client),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.NodeV1(true),
		kubeletplugin.NodeV1beta1(false),
		kubeletplugin.GRPCVerbosity(2),
	)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	defer helper.Stop()
	if err := helper.PublishResources(ctx, inv.resources()); err != nil {
		return fmt.Errorf("publish resources: %w", err)
	}

	nriPlugin, err := stub.New(&nriDriver{state: plugin},
		stub.WithPluginName("orchestration-dra"),
		stub.WithPluginIdx("10"),
		stub.WithSocketPath("/var/run/nri/nri.sock"),
	)
	if err != nil {
		return fmt.Errorf("create NRI plugin: %w", err)
	}
	nriError := make(chan error, 1)
	go func() { nriError <- nriPlugin.Run(ctx) }()

	slog.Info("DRA driver ready", "node", nodeName, "devices", len(inv.devices))
	select {
	case <-ctx.Done():
		return nil
	case err := <-nriError:
		return fmt.Errorf("run NRI plugin: %w", err)
	}
}
