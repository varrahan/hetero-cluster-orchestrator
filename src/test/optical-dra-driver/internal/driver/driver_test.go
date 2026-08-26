package driver

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

func TestOpticalInventoryAndClaimLifecycle(t *testing.T) {
	inv, err := discoverInventory(topologyJSON(t, validOpticalDevices()), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.devices) != 3 {
		t.Fatalf("optical devices = %d, want 3", len(inv.devices))
	}
	resources := inv.resources()
	if len(resources.Pools["worker-a"].Slices) != 1 || len(resources.Pools["worker-a"].Slices[0].Devices) != 3 {
		t.Fatalf("unexpected resources: %#v", resources)
	}
	switchDevice := inv.devices["switch-a"]
	if switchDevice.Vendor != "lumentum" || switchDevice.PartNumber != "R300" || switchDevice.Management != "gnmi" || switchDevice.SourceID != "r300-a" {
		t.Fatalf("switch product metadata = %#v", switchDevice)
	}
	cpoDevice := inv.devices["cpo-a"]
	if cpoDevice.ComponentRole != "external_laser_source" || cpoDevice.OutputPowerDBm != 24 || cpoDevice.FormFactor != "elsfp" {
		t.Fatalf("CPO product metadata = %#v", cpoDevice)
	}
	adapterDevice := inv.devices["adapter-a"]
	if adapterDevice.FormFactor != "osfp" || adapterDevice.Protocol != "ethernet" || adapterDevice.Management != "cmis_5_3" || adapterDevice.Lanes != 8 || adapterDevice.ReachMeters != 500 || adapterDevice.LinkID != "link-a" {
		t.Fatalf("adapter product metadata = %#v", adapterDevice)
	}
	cpo := inv.devices["cpo-a"].resourceDevice().Attributes[attributeDomain+"wavelengthNm"]
	if cpo.IntValue == nil || *cpo.IntValue != 1311 {
		t.Fatalf("CPO wavelength = %#v", cpo)
	}
	adapter := inv.devices["adapter-a"].resourceDevice().Attributes[attributeDomain+"fullDuplex"]
	if adapter.BoolValue == nil || !*adapter.BoolValue {
		t.Fatalf("adapter duplex = %#v", adapter)
	}
	outputPower := inv.devices["cpo-a"].resourceDevice().Attributes[attributeDomain+"outputPowerDbm"]
	if outputPower.IntValue == nil || *outputPower.IntValue != 24 {
		t.Fatalf("CPO output power = %#v", outputPower)
	}
	encoded, _, _, err := inv.summary("boot-a")
	if err != nil || !strings.Contains(encoded, `"vendor":"lumentum"`) || !strings.Contains(encoded, `"managementInterface":"cmis_5_3"`) {
		t.Fatalf("vendor summary = %q, err=%v", encoded, err)
	}

	stateDir := t.TempDir()
	plugin, err := newNodePlugin(stateDir, inv)
	if err != nil {
		t.Fatal(err)
	}
	claim := opticalClaim(inv, "claim-a", "uid-a")
	result, err := plugin.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	if err != nil || result[claim.UID].Err != nil || len(result[claim.UID].Devices) != 3 {
		t.Fatalf("prepare: result=%#v err=%v", result, err)
	}
	if _, err := newNodePlugin(stateDir, inv); err != nil {
		t.Fatalf("recover unchanged inventory: %v", err)
	}
	changed := *inv
	changed.devices = maps.Clone(inv.devices)
	device := changed.devices["adapter-a"]
	device.Management = "cmis_6"
	changed.devices[device.Name] = device
	if _, err := newNodePlugin(stateDir, &changed); err == nil {
		t.Fatal("changed prepared inventory was accepted")
	}
	duplicate := opticalClaim(inv, "claim-b", "uid-b")
	result, _ = plugin.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{duplicate})
	if result[duplicate.UID].Err == nil {
		t.Fatal("duplicate optical preparation succeeded")
	}
	if _, err := plugin.UnprepareResourceClaims(context.Background(), []kubeletplugin.NamespacedObject{{UID: claim.UID, NamespacedName: types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}}}); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsInvalidOpticalTopology(t *testing.T) {
	if _, err := discoverInventory("{", "worker-a"); err == nil {
		t.Fatal("malformed topology was accepted")
	}
	valid := validOpticalDevices()
	unsupported := valid[0]
	unsupported.Kind = "ethernet"
	unnamed := valid[0]
	unnamed.Name = ""
	missingTopology := valid[0]
	missingTopology.Topology = ""
	missingEndpoint := valid[0]
	missingEndpoint.Location = ""
	invalidWavelength := valid[1]
	invalidWavelength.WavelengthNM = 0
	halfDuplex := valid[2]
	halfDuplex.FullDuplex = false
	duplicate := valid[1]
	duplicate.Name = valid[0].Name
	negativeCapacity := valid[0]
	negativeCapacity.BandwidthGbps = -1
	malformedVendor := valid[0]
	malformedVendor.Vendor = "---"
	negativeMetadata := valid[2]
	negativeMetadata.ReachMeters = -1

	cases := map[string]opticalTopology{
		"version":            {Version: 2, Devices: valid},
		"unsupported kind":   {Version: 1, Devices: []opticalDevice{unsupported}},
		"missing name":       {Version: 1, Devices: []opticalDevice{unnamed}},
		"duplicate name":     {Version: 1, Devices: []opticalDevice{valid[0], duplicate}},
		"missing topology":   {Version: 1, Devices: []opticalDevice{missingTopology}},
		"missing endpoint":   {Version: 1, Devices: []opticalDevice{missingEndpoint}},
		"invalid wavelength": {Version: 1, Devices: []opticalDevice{invalidWavelength}},
		"half duplex":        {Version: 1, Devices: []opticalDevice{halfDuplex}},
		"negative capacity":  {Version: 1, Devices: []opticalDevice{negativeCapacity}},
		"malformed vendor":   {Version: 1, Devices: []opticalDevice{malformedVendor}},
		"negative metadata":  {Version: 1, Devices: []opticalDevice{negativeMetadata}},
	}
	for name, topology := range cases {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(topology)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := discoverInventory(string(encoded), "worker-a"); err == nil {
				t.Fatal("invalid topology was accepted")
			}
		})
	}
	legacy := []opticalDevice{
		{Kind: kindSwitch, Name: "switch", Topology: "fabric-a", Location: "port-1"},
		{Kind: kindCPOPhotonic, Name: "cpo", Topology: "fabric-a", WavelengthNM: 1310},
		{Kind: kindPhysicalAsic, Name: "adapter", Topology: "fabric-a", FullDuplex: true},
	}
	if _, err := discoverInventory(topologyJSON(t, legacy), "worker-a"); err != nil {
		t.Fatalf("legacy v1 topology: %v", err)
	}
}

func TestCoherentAOCProfile(t *testing.T) {
	inv, err := discoverInventory(topologyJSON(t, []opticalDevice{{
		Kind:          kindPhysicalAsic,
		Name:          "aoc-a",
		Model:         "C.wire",
		Vendor:        "Coherent",
		FormFactor:    "CXP",
		Protocol:      "Any",
		ComponentRole: "Active Optical Cable Endpoint",
		LinkID:        "aoc-link-a",
		Topology:      "fabric-a",
		Location:      "host-a/cxp-1",
		BandwidthGbps: 150,
		FullDuplex:    true,
		Lanes:         12,
	}}), "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	device := inv.devices["aoc-a"]
	if device.Vendor != "coherent" || device.ComponentRole != "active_optical_cable_endpoint" || device.LinkID != "aoc-link-a" {
		t.Fatalf("Coherent AOC metadata = %#v", device)
	}
}

func validOpticalDevices() []opticalDevice {
	return []opticalDevice{
		{Kind: kindSwitch, Name: "switch-a", NUMA: 0, Model: "R300", Vendor: "Lumentum", PartNumber: "R300", FormFactor: "Chassis Port", Protocol: "Any", Management: "gNMI", SourceID: "r300-a", Topology: "fabric-a", Location: "rack-a/port-1", Ports: 1},
		{Kind: kindCPOPhotonic, Name: "cpo-a", NUMA: 0, Model: "ELSFP-350", Vendor: "Lumentum", PartNumber: "ELSFP-350", FormFactor: "ELSFP", Protocol: "Any", ComponentRole: "External Laser Source", SourceID: "elsfp-a", Topology: "fabric-a", Location: "package-0", WavelengthNM: 1311, OutputPowerDBm: 24},
		{Kind: kindPhysicalAsic, Name: "adapter-a", NUMA: 0, Model: "1.6T 2xDR4 TRO OSFP", Vendor: "Lumentum", FormFactor: "OSFP", Protocol: "Ethernet", Management: "CMIS-5.3", LinkID: "link-a", Topology: "fabric-a", Location: "host-a/osfp-1", Ports: 1, BandwidthGbps: 1600, FullDuplex: true, Lanes: 8, ReachMeters: 500},
	}
}

func topologyJSON(t *testing.T, devices []opticalDevice) string {
	t.Helper()
	encoded, err := json.Marshal(opticalTopology{Version: 1, Devices: devices})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func opticalClaim(inv *inventory, name string, uid types.UID) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: uid}}
	for _, allocation := range []resourceapi.DeviceRequestAllocationResult{
		{Request: "switch", Driver: driverName, Pool: inv.nodeName, Device: "switch-a"},
		{Request: "cpo", Driver: driverName, Pool: inv.nodeName, Device: "cpo-a"},
		{Request: "adapter", Driver: driverName, Pool: inv.nodeName, Device: "adapter-a"},
	} {
		if claim.Status.Allocation == nil {
			claim.Status.Allocation = &resourceapi.AllocationResult{}
		}
		claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results, allocation)
	}
	return claim
}
