package driver

import (
	"context"
	"log/slog"

	"k8s.io/client-go/kubernetes"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/draplugin"
)

const (
	driverName = "orchestration.optical.gputpu.io"
	stateDir   = "/var/lib/kubelet/plugins/orchestration.optical.gputpu.io/state"
)

func Run() error {
	environment, err := draplugin.InCluster()
	if err != nil {
		return err
	}
	defer environment.Cancel()
	inv, err := discoverInventory(environment.Node.Annotations[opticalTopologyAnnotation], environment.NodeName)
	if err != nil {
		return err
	}
	plugin, err := newNodePlugin(stateDir, inv)
	if err != nil {
		return err
	}
	if err := plugin.Recover(environment.Context, environment.Client); err != nil {
		return err
	}
	helper, err := draplugin.Start(environment.Context, plugin, environment.Client, environment.NodeName, driverName, inv.resources())
	if err != nil {
		return err
	}
	defer helper.Stop()
	if err := publishInventory(environment.Context, environment.Client, environment.NodeName, inv); err != nil {
		return err
	}

	slog.Info("optical DRA driver ready", "node", environment.NodeName, "devices", len(inv.devices))
	<-environment.Context.Done()
	return nil
}

func publishInventory(ctx context.Context, client kubernetes.Interface, nodeName string, inv *inventory) error {
	return draplugin.PublishInventory(ctx, client, nodeName, draplugin.AnnotationKeys{
		Inventory: opticalInventoryAnnotation,
		Hash:      opticalInventoryHash,
		BootID:    opticalInventoryBootID,
	}, inv.summary)
}
