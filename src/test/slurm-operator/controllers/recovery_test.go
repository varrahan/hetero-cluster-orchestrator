package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/hardware"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

func TestNewestCheckpointTimeIsClusterScoped(t *testing.T) {
	old := time.Unix(100, 0)
	newer := time.Unix(200, 0)
	got := newestCheckpointTime([]minio.ObjectInfo{
		{LastModified: newer, UserMetadata: minio.StringMap{"cluster-uid": "other"}},
		{LastModified: old, UserMetadata: minio.StringMap{"cluster-uid": "wanted"}},
	}, "wanted")
	if got == nil || !got.Time.Equal(old) {
		t.Fatalf("newest checkpoint = %v, want %v", got, old)
	}
}

func TestAffectedJobSelection(t *testing.T) {
	state := &recoveryState{Workers: []recoveryWorker{{ClusterNamespace: "slurm-system", ClusterName: "research", Name: "failed-worker"}}}
	snapshots := map[string]clusterSnapshot{"slurm-system/research": {jobs: []slurm.Job{
		{ID: 10, State: []string{"RUNNING"}, Nodes: []string{"failed-worker", "healthy-worker"}},
		{ID: 11, State: []string{"RUNNING"}, Nodes: []string{"healthy-worker"}},
		{ID: 21, HetJobID: 20, State: []string{"RUNNING"}, Nodes: []string{"failed-worker"}},
	}}}
	if !addAffectedJobs(state, snapshots) {
		t.Fatal("affected jobs were not added")
	}
	if len(state.Jobs) != 2 || state.Jobs[0].ID != 10 || state.Jobs[1].ID != 20 {
		t.Fatalf("affected jobs = %#v", state.Jobs)
	}
	if addAffectedJobs(state, snapshots) {
		t.Fatal("affected jobs were duplicated on restart")
	}
	if jobEvacuated(state.Jobs[0], state, snapshots["slurm-system/research"].jobs) {
		t.Fatal("running affected job was considered evacuated")
	}
	pending := []slurm.Job{{ID: 10, State: []string{"PENDING"}}}
	if !jobEvacuated(state.Jobs[0], state, pending) {
		t.Fatal("pending job without affected allocation was not evacuated")
	}
	healthy := []slurm.Job{{ID: 10, State: []string{"RUNNING"}, Nodes: []string{"healthy-worker"}}}
	if jobEvacuated(state.Jobs[0], state, healthy) {
		t.Fatal("job running elsewhere before requeue was considered evacuated")
	}
	state.Jobs[0].RequeueSent = true
	if !jobEvacuated(state.Jobs[0], state, healthy) {
		t.Fatal("requeued job running on healthy capacity was not considered evacuated")
	}
}

func TestRecoveryStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "recovery-state")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "recovery-state-node-", Labels: map[string]string{"orchestration.gputpu.io/compute": "true"}}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-a"}}}
	if err := testClient.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testClient.Delete(context.Background(), node) })
	state, err := newRecoveryState(node, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	state.Jobs = []recoveryJob{{ClusterNamespace: namespace, ClusterName: "research", ID: 42, SignalSent: true}}
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Client: testClient, Reader: testClient, Scheme: testScheme}, Namespace: namespace}
	object, err := reconciler.saveState(ctx, node, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testClient.Delete(context.Background(), object) })
	loaded, _, err := reconciler.loadState(ctx, node)
	if err != nil || loaded.Incident != state.Incident || len(loaded.Jobs) != 1 || !loaded.Jobs[0].SignalSent {
		t.Fatalf("loaded state=%#v err=%v", loaded, err)
	}
}

func TestVerifierObjectsStayOnQuarantinedNode(t *testing.T) {
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "slurm-system", UID: types.UID("state-uid")}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}}
	state := &recoveryState{Incident: "11111111-2222-4333-8444-555555555555"}
	cell := hardware.Cell{NUMA: 1, CPUs: 4, MemoryUnits: 4, MemoryUnitBytes: 1 << 30, GPUs: []hardware.GPU{{UUID: "GPU-a"}}, OpenTPU: []hardware.OpenTPU{{Profile: "opentpu_m8", Count: 2, MatrixSize: 8, CPUCores: 2, MemoryBytes: 1 << 30, SharedMemory: 512 << 20}}}
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Scheme: testScheme, WorkerImage: "worker:test"}, Namespace: "slurm-system"}
	claim, job, err := reconciler.verifierObjects(node, owner, state, cell)
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Spec.Devices.Requests) != 4 {
		t.Fatalf("verifier requests = %d, want CPU, memory, GPU, and OpenTPU", len(claim.Spec.Devices.Requests))
	}
	pod := job.Spec.Template.Spec
	if pod.NodeSelector["kubernetes.io/hostname"] != node.Name || len(pod.Tolerations) != 2 || pod.Containers[0].Image != "worker:test" {
		t.Fatalf("unexpected verifier Pod: %#v", pod)
	}
}

func TestSetHealthyDoesNotRemoveOtherTaints(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "healthy-node-", Labels: map[string]string{"orchestration.gputpu.io/compute": "true"}}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "keep", Effect: corev1.TaintEffectNoSchedule}, {Key: hardwareDegradedTaint, Value: "true", Effect: corev1.TaintEffectNoSchedule}}}}
	if err := testClient.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testClient.Delete(context.Background(), node) })
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Client: testClient}, Now: func() time.Time { return time.Unix(100, 0) }}
	if err := reconciler.setHealthy(ctx, node); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(node), node); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool { return taint.Key == "keep" }) || slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool { return taint.Key == hardwareDegradedTaint }) {
		t.Fatalf("taints after recovery = %#v", node.Spec.Taints)
	}
}

func TestRawFaultIsIsolatedDurably(t *testing.T) {
	ctx := context.Background()
	namespace := "recovery-isolation"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "recovery-isolation", UID: "node-isolation", Labels: map[string]string{"orchestration.gputpu.io/compute": "true"}}, Status: corev1.NodeStatus{
		NodeInfo:   corev1.NodeSystemInfo{BootID: "boot-a"},
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeConditionType(hardwareFaultCondition), Status: corev1.ConditionTrue}},
	}}
	storage := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(node).WithIndex(&corev1.Pod{}, workerNodeIndex, func(object client.Object) []string {
		return []string{object.(*corev1.Pod).Spec.NodeName}
	}).WithStatusSubresource(node).Build()
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Client: storage, Reader: storage, Scheme: testScheme}, Namespace: namespace, RebootTimeout: time.Minute}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: node.Name}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Get(ctx, client.ObjectKeyFromObject(node), node); err != nil {
		t.Fatal(err)
	}
	state, object, err := reconciler.loadState(ctx, node)
	if err != nil || state == nil || object == nil || state.Phase != phaseDraining {
		t.Fatalf("state=%#v object=%#v err=%v", state, object, err)
	}
	if node.Annotations[recoveryIncidentKey] != state.Incident || !slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool { return taint.Key == hardwareDegradedTaint }) || nodeConditionStatus(node.Status.Conditions, hardwareDegradedCondition) != corev1.ConditionTrue {
		t.Fatalf("fault was not isolated: %#v", node)
	}
}

func TestSecondBootEntersManualRepair(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "recovery-reboot")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "recovery-reboot-"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-c"}}}
	if err := testClient.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Status().Update(ctx, node); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testClient.Delete(context.Background(), node) })
	now := time.Unix(100, 0)
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Client: testClient, Reader: testClient, Scheme: testScheme}, Namespace: namespace, RebootTimeout: time.Minute, Now: func() time.Time { return now }}
	state, err := newRecoveryState(node, now.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state.PreBootID, state.NewBootID, state.Phase = "boot-a", "boot-b", phaseRebooting
	object, err := reconciler.saveState(ctx, node, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileReboot(ctx, node, object, state); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := reconciler.loadState(ctx, node)
	if err != nil || loaded.Phase != phaseManualRepair {
		t.Fatalf("state=%#v err=%v", loaded, err)
	}
}

func TestSlurmOutageKeepsWorkerAndStopsRecovery(t *testing.T) {
	ctx := context.Background()
	namespace := "recovery-outage"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "recovery-outage", UID: "node-outage"}}
	cluster := validCluster(namespace)
	cluster.UID = "cluster-outage"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.Authentication.JWTKeySecretRef, Namespace: namespace}, Data: map[string][]byte{jwtKey: []byte("01234567890123456789012345678901")}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/component": workerComponent, "app.kubernetes.io/managed-by": "slurm-operator"}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, orchestrationv1alpha1.GroupVersion.WithKind("HeterogeneousCluster"))}}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "slurmd", Image: "test"}}}}
	storage := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(node, cluster, secret, pod).WithIndex(&corev1.Pod{}, workerNodeIndex, func(object client.Object) []string {
		return []string{object.(*corev1.Pod).Spec.NodeName}
	}).Build()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	reconciler := &RecoveryReconciler{ClusterReconciler: &ClusterReconciler{Client: storage, Reader: storage, Scheme: testScheme, HTTPClient: server.Client(), RESTBaseURL: func(*orchestrationv1alpha1.HeterogeneousCluster) string { return server.URL }}, Namespace: namespace}
	state, err := newRecoveryState(node, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	object, err := reconciler.saveState(ctx, node, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.reconcileEvacuation(ctx, node, object, state)
	if err != nil || result.RequeueAfter == 0 || state.Phase != phaseIsolated {
		t.Fatalf("result=%#v state=%#v err=%v", result, state, err)
	}
	if err := storage.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatalf("worker was removed during Slurm outage: %v", err)
	}
}
