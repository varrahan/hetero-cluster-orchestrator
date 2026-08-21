package controllers

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	resourceplan "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/resources"
)

func TestResourceClaimExactSameNUMA(t *testing.T) {
	pool := orchestrationv1alpha1.WorkerPoolSpec{Name: "gpu", MemoryUnit: "2Gi", Scaling: orchestrationv1alpha1.ScalingSpec{MaxWorkers: 2, IdleTimeout: metav1.Duration{}}, Profiles: []orchestrationv1alpha1.WorkerProfile{{Name: "gpu", Gres: "gpu:rtx_4050", DeviceClassName: "nvidia.orchestration.gputpu.io"}}}
	claim, err := resourceClaim("default", "worker-resources", resourceplan.Shape{CPUs: 4, MemoryBytes: 4 << 30, GRES: map[string]int64{"gpu:rtx_4050": 1}}, pool, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Spec.Devices.Requests) != 3 || len(claim.Spec.Devices.Constraints) != 1 || claim.Spec.Devices.Constraints[0].MatchAttribute == nil {
		t.Fatalf("claim = %#v", claim.Spec)
	}
	if got := string(*claim.Spec.Devices.Constraints[0].MatchAttribute); got != driverName+"/numaNode" {
		t.Fatalf("NUMA constraint = %q", got)
	}
	if claim.Spec.Devices.Requests[1].Exactly.Count != 2 {
		t.Fatalf("memory unit count = %d", claim.Spec.Devices.Requests[1].Exactly.Count)
	}

	_, err = resourceClaim("default", "too-large", resourceplan.Shape{CPUs: 16, MemoryBytes: 17 << 30, GRES: map[string]int64{}}, orchestrationv1alpha1.WorkerPoolSpec{Name: "cpu", MemoryUnit: "1Gi"}, 0)
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("32-device ceiling error = %v", err)
	}
}

func TestCreateWorkerPodAndClaim(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "worker")
	cluster := validCluster(namespace)
	if err := testClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	reconciler := &ClusterReconciler{Client: testClient, Reader: testClient, Scheme: testScheme, WorkerImage: "worker:test", WorkerMemoryHeadroom: 256 << 20}
	demand := resourceplan.Demand{JobID: 7, GroupID: 7, PoolName: "strict", Partition: "compute", Count: 1, Shape: resourceplan.Shape{CPUs: 2, MemoryBytes: 2 << 30, GRES: map[string]int64{"gpu:rtx_4050": 1}}}
	pod, err := reconciler.createWorker(ctx, cluster, cluster.Spec.WorkerPools[0], demand)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.NodeName != "" || pod.Spec.Hostname != pod.Name {
		t.Fatalf("scheduler binding or hostname is wrong: %#v", pod.Spec)
	}
	if !reflect.DeepEqual(pod.Spec.InitContainers[0].Resources.Requests, pod.Spec.InitContainers[0].Resources.Limits) || !reflect.DeepEqual(pod.Spec.Containers[0].Resources.Requests, pod.Spec.Containers[0].Resources.Limits) {
		t.Fatal("worker Pod is not Guaranteed")
	}
	if pod.Spec.Containers[0].SecurityContext == nil || pod.Spec.Containers[0].SecurityContext.Privileged == nil || !*pod.Spec.Containers[0].SecurityContext.Privileged {
		t.Fatal("slurmd container is not privileged")
	}
	var claim resourceapi.ResourceClaim
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pod.Annotations[workerClaimAnnotation]}, &claim); err != nil {
		t.Fatal(err)
	}
	if len(claim.Spec.Devices.Requests) != 3 || len(claim.Spec.Devices.Constraints) != 1 {
		t.Fatalf("claim = %#v", claim.Spec)
	}
	if len(claim.OwnerReferences) != 1 || claim.OwnerReferences[0].UID != pod.UID {
		t.Fatalf("claim owner = %#v", claim.OwnerReferences)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restart policy = %s", pod.Spec.RestartPolicy)
	}
}

func TestCompleteDriverSlices(t *testing.T) {
	slices := []resourceapi.ResourceSlice{
		{ObjectMeta: metav1.ObjectMeta{Name: "old"}, Spec: resourceapi.ResourceSliceSpec{Driver: driverName, Pool: resourceapi.ResourcePool{Name: "node-a", Generation: 1, ResourceSliceCount: 1}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "new-a"}, Spec: resourceapi.ResourceSliceSpec{Driver: driverName, Pool: resourceapi.ResourcePool{Name: "node-a", Generation: 2, ResourceSliceCount: 2}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "new-b"}, Spec: resourceapi.ResourceSliceSpec{Driver: driverName, Pool: resourceapi.ResourcePool{Name: "node-a", Generation: 2, ResourceSliceCount: 2}}},
	}
	selected, err := completeDriverSlices(slices)
	if err != nil || len(selected) != 2 || selected[0].Spec.Pool.Generation != 2 {
		t.Fatalf("selected = %#v, error = %v", selected, err)
	}
	if _, err := completeDriverSlices(slices[:2]); err == nil {
		t.Fatal("incomplete current generation was accepted")
	}
}
