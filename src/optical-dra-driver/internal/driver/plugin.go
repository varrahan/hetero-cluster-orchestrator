package driver

import (
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/draplugin"
)

type nodePlugin struct {
	*draplugin.Plugin
}

func newNodePlugin(dir string, inv *inventory) (*nodePlugin, error) {
	plugin, err := draplugin.New(dir, draplugin.Config{
		Version:    1,
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
		ClaimDevices: func(results []resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device {
			return claimedDevices(inv, results)
		},
	})
	if err != nil {
		return nil, err
	}
	return &nodePlugin{Plugin: plugin}, nil
}

func claimedDevices(inv *inventory, results []resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device {
	output := make([]kubeletplugin.Device, 0, len(results))
	for _, allocation := range results {
		device, exists := inv.devices[allocation.Device]
		if !exists {
			continue
		}
		output = append(output, kubeletplugin.Device{Requests: []string{allocation.Request}, PoolName: allocation.Pool, DeviceName: device.Name})
	}
	return output
}
