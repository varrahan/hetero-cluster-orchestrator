package driver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

const (
	driverName = "orchestration.optical.gputpu.io"
	stateDir   = "/var/lib/kubelet/plugins/orchestration.optical.gputpu.io/state"
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
	inv, err := discoverInventory(node.Annotations[opticalTopologyAnnotation], nodeName)
	if err != nil {
		return err
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
	if err := publishInventory(ctx, client, nodeName, inv); err != nil {
		return err
	}

	slog.Info("optical DRA driver ready", "node", nodeName, "devices", len(inv.devices))
	<-ctx.Done()
	return nil
}

func publishInventory(ctx context.Context, client kubernetes.Interface, nodeName string, inv *inventory) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read node for inventory publication: %w", err)
		}
		_, encoded, hash, err := inv.summary(node.Status.NodeInfo.BootID)
		if err != nil {
			return err
		}
		if node.Annotations[opticalInventoryBootID] == node.Status.NodeInfo.BootID && node.Annotations[opticalInventoryHash] != "" && node.Annotations[opticalInventoryHash] != hash {
			return fmt.Errorf("DRA inventory changed within boot %q", node.Status.NodeInfo.BootID)
		}
		if node.Annotations[opticalInventoryBootID] == node.Status.NodeInfo.BootID && node.Annotations[opticalInventoryHash] == hash && node.Annotations[opticalInventoryAnnotation] == encoded {
			return nil
		}
		copy := node.DeepCopy()
		if copy.Annotations == nil {
			copy.Annotations = map[string]string{}
		}
		copy.Annotations[opticalInventoryAnnotation] = encoded
		copy.Annotations[opticalInventoryHash] = hash
		copy.Annotations[opticalInventoryBootID] = node.Status.NodeInfo.BootID
		if _, err := client.CoreV1().Nodes().Update(ctx, copy, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("publish optical DRA inventory: %w", err)
		}
		return nil
	})
}
