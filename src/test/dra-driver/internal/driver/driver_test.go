package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "github.com/containerd/nri/pkg/api"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

func TestInventoryAndClaimLifecycle(t *testing.T) {
	sys := t.TempDir()
	for cpu, core := range []int{0, 0, 1, 1} {
		base := filepath.Join(sys, "devices/system/cpu", fmt.Sprintf("cpu%d", cpu))
		writeTestFile(t, filepath.Join(base, "topology/physical_package_id"), "0\n")
		writeTestFile(t, filepath.Join(base, "topology/core_id"), fmt.Sprintf("%d\n", core))
		if err := os.MkdirAll(filepath.Join(base, "node0"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, filepath.Join(sys, "devices/system/node/node0/meminfo"), "Node 0 MemTotal:       4194304 kB\n")
	writeTestFile(t, filepath.Join(sys, "bus/pci/devices/0000:01:00.0/numa_node"), "0\n")
	nvidiaSMI := filepath.Join(sys, "nvidia-smi")
	writeTestFile(t, nvidiaSMI, "#!/bin/sh\nprintf '0, GPU-test, NVIDIA GeForce RTX 4050 Laptop GPU, 6144, 0000:01:00.0\\n'\n")
	if err := os.Chmod(nvidiaSMI, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NVIDIA_SMI", nvidiaSMI)
	inv, err := discoverInventory(sys, "worker-a", map[string]string{
		annotationPrefix + "opentpu-profiles": `{"version":1,"profiles":[{"name":"m8","matrixSize":8,"numaNode":0,"slots":1,"cpuCores":2,"memory":"1Gi","sharedMemory":"512Mi"}]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := countKind(inv, kindCPU); got != 1 {
		t.Fatalf("physical CPU devices = %d, want 1 after SMT dedupe and headroom", got)
	}
	if got := countKind(inv, kindMemory); got != 3 {
		t.Fatalf("memory devices = %d, want 3", got)
	}
	if got := countKind(inv, kindOpenTPU); got != 1 {
		t.Fatalf("OpenTPU devices = %d, want 1", got)
	}
	if got := countKind(inv, kindGPU); got != 1 {
		t.Fatalf("NVIDIA devices = %d, want 1", got)
	}
	resources := inv.resources()
	if len(resources.Pools["worker-a"].Slices) != 1 {
		t.Fatalf("unexpected slices: %#v", resources)
	}
	for _, device := range resources.Pools["worker-a"].Slices[0].Devices {
		for name := range device.Attributes {
			if errors := validation.IsCIdentifier(string(name)[strings.LastIndex(string(name), "/")+1:]); len(errors) != 0 {
				t.Fatalf("invalid attribute %q: %v", name, errors)
			}
		}
		for name := range device.Capacity {
			if errors := validation.IsCIdentifier(string(name)[strings.LastIndex(string(name), "/")+1:]); len(errors) != 0 {
				t.Fatalf("invalid capacity %q: %v", name, errors)
			}
		}
	}

	stateDir := t.TempDir()
	plugin, err := newNodePlugin(stateDir, inv)
	if err != nil {
		t.Fatal(err)
	}
	claim := testClaim(inv, "claim-a", "uid-a")
	result, err := plugin.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	if err != nil || result[claim.UID].Err != nil {
		t.Fatalf("prepare: result=%#v err=%v", result, err)
	}
	if len(result[claim.UID].Devices) != 1 || len(result[claim.UID].Devices[0].CDIDeviceIDs) != 1 {
		t.Fatalf("OpenTPU CDI result = %#v", result[claim.UID])
	}
	if _, err := newNodePlugin(stateDir, inv); err != nil {
		t.Fatalf("unchanged inventory recovery: %v", err)
	}
	changed := &inventory{nodeName: inv.nodeName, devices: map[string]localDevice{}}
	for name, device := range inv.devices {
		changed.devices[name] = device
	}
	for _, name := range plugin.prepared[claim.UID].Devices {
		device := changed.devices[name]
		if device.Kind == kindMemory {
			device.UnitBytes *= 2
			changed.devices[name] = device
			break
		}
	}
	if _, err := newNodePlugin(stateDir, changed); err == nil {
		t.Fatal("changed prepared inventory was accepted")
	}

	duplicate := testClaim(inv, "claim-b", "uid-b")
	result, _ = plugin.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{duplicate})
	if result[duplicate.UID].Err == nil {
		t.Fatal("duplicate device preparation succeeded")
	}
	nri := &nriDriver{state: plugin}
	adjustment, _, err := nri.CreateContainer(context.Background(), &api.PodSandbox{Namespace: "default", Annotations: map[string]string{claimAnnotation: "claim-a"}}, &api.Container{})
	if err != nil || adjustment == nil {
		t.Fatalf("NRI adjustment = %#v, err=%v", adjustment, err)
	}
	if _, err := plugin.UnprepareResourceClaims(context.Background(), []kubeletplugin.NamespacedObject{{UID: claim.UID, NamespacedName: types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}}}); err != nil {
		t.Fatal(err)
	}
}

func TestBootScopedInventoryPublication(t *testing.T) {
	inv := &inventory{nodeName: "worker-a", devices: map[string]localDevice{
		"cpu": {Name: "cpu", Kind: kindCPU, NUMA: 0, CPU: 1},
		"mem": {Name: "mem", Kind: kindMemory, NUMA: 0, UnitBytes: 1 << 30},
		"gpu": {Name: "gpu", Kind: kindGPU, NUMA: 0, UUID: "GPU-a", Model: "rtx_4050", PCI: "0000:01:00.0"},
	}}
	_, _, hashA, err := inv.summary("boot-a")
	if err != nil {
		t.Fatal(err)
	}
	_, _, hashB, err := inv.summary("boot-b")
	if err != nil || hashA != hashB {
		t.Fatalf("inventory identity changed across boots: %q != %q (err=%v)", hashA, hashB, err)
	}
	client := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-a"}}})
	if err := publishInventory(context.Background(), client, "worker-a", inv); err != nil {
		t.Fatal(err)
	}
	node, err := client.CoreV1().Nodes().Get(context.Background(), "worker-a", metav1.GetOptions{})
	if err != nil || node.Annotations[inventoryHashAnnotation] == "" || node.Annotations[inventoryBootIDAnnotation] != "boot-a" {
		t.Fatalf("published Node=%#v err=%v", node, err)
	}
	inv.devices["cpu-2"] = localDevice{Name: "cpu-2", Kind: kindCPU, NUMA: 0, CPU: 2}
	if err := publishInventory(context.Background(), client, "worker-a", inv); err == nil {
		t.Fatal("changed inventory was accepted within one boot")
	}
	node.Status.NodeInfo.BootID = "boot-b"
	if _, err := client.CoreV1().Nodes().UpdateStatus(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := publishInventory(context.Background(), client, "worker-a", inv); err != nil {
		t.Fatalf("new boot inventory: %v", err)
	}
	node, _ = client.CoreV1().Nodes().Get(context.Background(), "worker-a", metav1.GetOptions{})
	if node.Annotations[inventoryBootIDAnnotation] != "boot-b" {
		t.Fatalf("inventory boot = %q, want boot-b", node.Annotations[inventoryBootIDAnnotation])
	}
}

func testClaim(inv *inventory, name string, uid types.UID) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: uid}}
	for _, kind := range []string{kindCPU, kindMemory, kindOpenTPU} {
		for _, device := range inv.devices {
			if device.Kind == kind {
				claim.Status.Allocation = allocationWith(claim.Status.Allocation, resourceapi.DeviceRequestAllocationResult{Request: kind, Driver: driverName, Pool: inv.nodeName, Device: device.Name})
				break
			}
		}
	}
	return claim
}

func allocationWith(allocation *resourceapi.AllocationResult, result resourceapi.DeviceRequestAllocationResult) *resourceapi.AllocationResult {
	if allocation == nil {
		allocation = &resourceapi.AllocationResult{}
	}
	allocation.Devices.Results = append(allocation.Devices.Results, result)
	return allocation
}

func countKind(inv *inventory, kind string) int {
	count := 0
	for _, device := range inv.devices {
		if device.Kind == kind {
			count++
		}
	}
	return count
}

func writeTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0644); err != nil {
		t.Fatal(err)
	}
}
