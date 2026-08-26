package driver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/containerd/nri/pkg/stub"
	"k8s.io/client-go/kubernetes"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/draplugin"
)

const (
	driverName = "orchestration.gputpu.io"
	stateDir   = "/var/lib/kubelet/plugins/orchestration.gputpu.io/state"
	cdiDir     = "/var/run/cdi"
)

func Run() error {
	environment, err := draplugin.InCluster()
	if err != nil {
		return err
	}
	defer environment.Cancel()
	inv, err := discoverInventory("/sys", environment.NodeName, environment.Node.Annotations)
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

	nriPlugin, err := stub.New(&nriDriver{state: plugin},
		stub.WithPluginName("orchestration-dra"),
		stub.WithPluginIdx("10"),
		stub.WithSocketPath("/var/run/nri/nri.sock"),
	)
	if err != nil {
		return fmt.Errorf("create NRI plugin: %w", err)
	}
	nriError := make(chan error, 1)
	go func() { nriError <- nriPlugin.Run(environment.Context) }()

	slog.Info("DRA driver ready", "node", environment.NodeName, "devices", len(inv.devices))
	select {
	case <-environment.Context.Done():
		return nil
	case err := <-nriError:
		return fmt.Errorf("run NRI plugin: %w", err)
	}
}

func publishInventory(ctx context.Context, client kubernetes.Interface, nodeName string, inv *inventory) error {
	return draplugin.PublishInventory(ctx, client, nodeName, draplugin.AnnotationKeys{
		Inventory: inventoryAnnotation,
		Hash:      inventoryHashAnnotation,
		BootID:    inventoryBootIDAnnotation,
	}, inv.summary)
}
