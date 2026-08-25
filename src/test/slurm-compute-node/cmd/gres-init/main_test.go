package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestValidateExactNUMACgroup(t *testing.T) {
	status := filepath.Join(t.TempDir(), "status")
	if err := os.WriteFile(status, []byte("Cpus_allowed_list:\t2-3\nMems_allowed_list:\t1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	devices := []resourceapi.Device{
		device("cpu-a", "cpu", 1, map[string]int64{"logicalCPU": 2}),
		device("cpu-b", "cpu", 1, map[string]int64{"logicalCPU": 3}),
		device("memory-a", "memory", 1, nil),
	}
	if err := validate(devices, shape{CPUs: 2, MemoryBytes: 1 << 30, GRES: map[string]int64{}}, status); err != nil {
		t.Fatal(err)
	}
	devices[2].Attributes[resourceapi.QualifiedName(driverName+"/numaNode")] = intValue(0)
	if err := validate(devices, shape{CPUs: 2, MemoryBytes: 1 << 30, GRES: map[string]int64{}}, status); err == nil {
		t.Fatal("cross-NUMA allocation accepted")
	}
}

func TestAllocatedDevicesUsesLatestGeneration(t *testing.T) {
	node := "node-a"
	old := &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "a-old"}, Spec: resourceapi.ResourceSliceSpec{
		Driver: driverName, NodeName: &node,
		Pool:    resourceapi.ResourcePool{Name: node, Generation: 1, ResourceSliceCount: 1},
		Devices: []resourceapi.Device{device("old", "memory", 0, nil)},
	}}
	current := &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "z-current"}, Spec: resourceapi.ResourceSliceSpec{
		Driver: driverName, NodeName: &node,
		Pool: resourceapi.ResourcePool{Name: node, Generation: 2, ResourceSliceCount: 1},
		Devices: []resourceapi.Device{
			device("cpu-a", "cpu", 0, map[string]int64{"logicalCPU": 1}),
			device("memory-a", "memory", 0, nil),
		},
	}}
	claim := &resourceapi.ResourceClaim{}
	claim.Status.Allocation = &resourceapi.AllocationResult{}
	claim.Status.Allocation.Devices.Results = []resourceapi.DeviceRequestAllocationResult{
		{Driver: driverName, Pool: node, Device: "cpu-a"},
		{Driver: driverName, Pool: node, Device: "memory-a"},
	}
	devices, err := allocatedDevices(context.Background(), fake.NewSimpleClientset(old, current), &corev1.Pod{Spec: corev1.PodSpec{NodeName: node}}, claim)
	if err != nil || len(devices) != 2 {
		t.Fatalf("allocated devices = %v, err=%v", devices, err)
	}
}

func TestWorkerWeightPreservesAccelerators(t *testing.T) {
	if workerWeight(shape{GRES: map[string]int64{}}) >= workerWeight(shape{GRES: map[string]int64{"tpu:opentpu_m8": 1}}) {
		t.Fatal("CPU-only workers must be preferred for unconstrained work")
	}
}

func device(name, kind string, numa int64, ints map[string]int64) resourceapi.Device {
	device := resourceapi.Device{Name: name, Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		resourceapi.QualifiedName(driverName + "/kind"):     stringValue(kind),
		resourceapi.QualifiedName(driverName + "/numaNode"): intValue(numa),
	}}
	for name, value := range ints {
		device.Attributes[resourceapi.QualifiedName(driverName+"/"+name)] = intValue(value)
	}
	return device
}

func stringValue(value string) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{StringValue: &value}
}
func intValue(value int64) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{IntValue: &value}
}
