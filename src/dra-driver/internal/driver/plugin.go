package driver

import (
	"errors"
	"log/slog"
	"os"
	"slices"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/draplugin"
)

type nodePlugin struct {
	*draplugin.Plugin
}

func newNodePlugin(dir string, inv *inventory) (*nodePlugin, error) {
	plugin, err := draplugin.New(dir, draplugin.Config{
		Version:    2,
		DriverName: driverName,
		PoolName:   inv.nodeName,
		HasDevice: func(name string) bool {
			_, exists := inv.devices[name]
			return exists
		},
		InventoryHash: func(names []string) string {
			devices := make([]localDevice, 0, len(names))
			for _, name := range names {
				devices = append(devices, inv.devices[name])
			}
			return draplugin.JSONHash(devices)
		},
		Prepare: func(names []string, record *draplugin.PreparedClaim) error {
			record.NUMA = -1
			for _, name := range names {
				device := inv.devices[name]
				if record.NUMA == -1 {
					record.NUMA = device.NUMA
				} else if record.NUMA != device.NUMA {
					return errors.New("allocated devices do not share one NUMA node")
				}
				if device.Kind == kindCPU {
					record.CPUs = append(record.CPUs, device.CPU)
				}
			}
			if len(record.CPUs) == 0 {
				return errors.New("claim has no CPU allocation")
			}
			slices.Sort(record.CPUs)
			return nil
		},
		ClaimDevices: func(results []resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device {
			return cdiDevices(inv, results)
		},
	})
	if err != nil {
		return nil, err
	}
	return &nodePlugin{Plugin: plugin}, nil
}

func cdiDevices(inv *inventory, results []resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device {
	output := make([]kubeletplugin.Device, 0, len(results))
	for _, allocation := range results {
		device, exists := inv.devices[allocation.Device]
		if !exists || (device.Kind != kindGPU && device.Kind != kindOpenTPU) {
			continue
		}
		id := driverName + "/opentpu=" + device.Name
		if device.Kind == kindGPU {
			id = driverName + "/nvidia=" + device.UUID
		}
		output = append(output, kubeletplugin.Device{Requests: []string{allocation.Request}, PoolName: allocation.Pool, DeviceName: allocation.Device, CDIDeviceIDs: []string{id}})
	}
	return output
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}
