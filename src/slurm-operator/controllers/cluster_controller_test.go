package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

var (
	testClient client.Client
	testScheme *runtime.Scheme
)

func TestMain(m *testing.M) {
	testScheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(testScheme); err != nil {
		panic(err)
	}
	if err := orchestrationv1alpha1.AddToScheme(testScheme); err != nil {
		panic(err)
	}
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "manifests", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	if err != nil {
		panic(err)
	}
	testClient, err = client.New(config, client.Options{Scheme: testScheme})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	if err := environment.Stop(); err != nil {
		panic(err)
	}
	os.Exit(code)
}

func TestCRDDefaultsAndValidation(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "api")
	object := validUnstructured(namespace)
	if err := testClient.Create(ctx, object); err != nil {
		t.Fatal(err)
	}
	replicas, found, err := unstructured.NestedInt64(object.Object, "spec", "controlPlane", "login", "replicas")
	if err != nil || !found || replicas != 1 {
		t.Fatalf("login replicas default = %d, found=%v, err=%v", replicas, found, err)
	}
	pools, _, _ := unstructured.NestedSlice(object.Object, "spec", "workerPools")
	pool := pools[0].(map[string]any)
	if pool["memoryUnit"] != "1Gi" || pool["scaling"].(map[string]any)["idleTimeout"] != "5m" {
		t.Fatalf("worker pool defaults not applied: %#v", pool)
	}

	invalidMemory := validCluster(namespace)
	invalidMemory.Name = "invalid-memory"
	invalidMemory.Spec.WorkerPools[0].MemoryUnit = "1.5Gi"
	if err := testClient.Create(ctx, invalidMemory); err == nil {
		t.Fatal("fractional memoryUnit accepted")
	}

	updated := object.DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, "another-claim", "spec", "controlPlane", "controllers", "stateSaveClaim"); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Update(ctx, updated); err == nil {
		t.Fatal("stateSaveClaim mutation accepted")
	}
}

func TestValidateSpecRejectsCrossPoolGRESConflict(t *testing.T) {
	spec := validCluster("unused").Spec
	spec.WorkerPools = append(spec.WorkerPools, orchestrationv1alpha1.WorkerPoolSpec{
		Name: "other", Partition: "other", MemoryUnit: "1Gi",
		Scaling:  orchestrationv1alpha1.ScalingSpec{MaxWorkers: 1, IdleTimeout: metav1.Duration{Duration: time.Minute}},
		Profiles: []orchestrationv1alpha1.WorkerProfile{{Name: "duplicate", Gres: "gpu:rtx_4050", DeviceClassName: "nvidia.gputpu.io/gpu"}},
	})
	if err := validateSpec(spec); err == nil {
		t.Fatal("duplicate cross-pool GRES mapping accepted")
	}
}

func TestReconcileRequiresJWTBeforeCreatingWorkloads(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "jwt")
	cluster := validCluster(namespace)
	createStateClaim(t, cluster)
	if err := testClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	reconciler := &ClusterReconciler{Client: testClient, Scheme: testScheme}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}); err != nil {
		t.Fatal(err)
	}
	var config corev1.ConfigMap
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "research-config"}, &config); !apierrors.IsNotFound(err) {
		t.Fatalf("configuration created without JWT Secret: %v", err)
	}
}

func TestReconcileCreateUpdateRestartAndRepair(t *testing.T) {
	ctx := context.Background()
	namespace := createNamespace(t, "reconcile")
	cluster := validCluster(namespace)
	createPrerequisites(t, cluster)
	if err := testClient.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/slurm/v0.0.44/jobs/":
			_, _ = w.Write([]byte(`{"jobs":[{"job_id":1,"partition":"compute","state":{"current":["PENDING"]}}]}`))
		case "/slurmdb/v0.0.44/clusters/":
			_, _ = w.Write([]byte(`{"clusters":[{"name":"research"}]}`))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	reconciler := &ClusterReconciler{Client: testClient, Scheme: testScheme, HTTPClient: server.Client(), RESTBaseURL: func(*orchestrationv1alpha1.HeterogeneousCluster) string { return server.URL }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: cluster.Name}}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}

	var controllers appsv1.StatefulSet
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "research-slurmctld"}, &controllers); err != nil {
		t.Fatal(err)
	}
	if !hasVolume(controllers.Spec.Template.Spec.Volumes, "munge-key") || !hasVolume(controllers.Spec.Template.Spec.Volumes, "jwt") {
		t.Fatal("slurmctld is missing required authentication material")
	}
	if got := controllers.Spec.Template.Spec.InitContainers; len(got) != 1 || got[0].Name != "state-permissions" || !hasVolumeMount(got[0].VolumeMounts, "state") {
		t.Fatal("slurmctld does not initialize shared state ownership")
	}
	originalUID := controllers.UID
	controllers.Status.Replicas = 2
	controllers.Status.ReadyReplicas = 2
	if err := testClient.Status().Update(ctx, &controllers); err != nil {
		t.Fatal(err)
	}
	for component, replicas := range map[string]int32{"slurmdbd": 1, "slurmrestd": 2, "login": 1} {
		var deployment appsv1.Deployment
		if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "research-" + component}, &deployment); err != nil {
			t.Fatal(err)
		}
		if got, want := hasVolume(deployment.Spec.Template.Spec.Volumes, "jwt"), component == "slurmdbd"; got != want {
			t.Fatalf("%s JWT volume present = %v, want %v", component, got, want)
		}
		if got, want := hasVolume(deployment.Spec.Template.Spec.Volumes, "munge-key"), component != "slurmrestd"; got != want {
			t.Fatalf("%s MUNGE volume present = %v, want %v", component, got, want)
		}
		deployment.Status.Replicas = replicas
		deployment.Status.ReadyReplicas = replicas
		if err := testClient.Status().Update(ctx, &deployment); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Get(ctx, request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, orchestrationv1alpha1.ConditionControlPlaneReady) || !meta.IsStatusConditionTrue(cluster.Status.Conditions, orchestrationv1alpha1.ConditionAccountingReady) {
		t.Fatalf("unexpected conditions: %#v", cluster.Status.Conditions)
	}
	if cluster.Status.ObservedGeneration != cluster.Generation {
		t.Fatalf("observed generation = %d, want %d", cluster.Status.ObservedGeneration, cluster.Generation)
	}

	var service corev1.Service
	serviceKey := types.NamespacedName{Namespace: namespace, Name: "research-slurmrestd"}
	if err := testClient.Get(ctx, serviceKey, &service); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Delete(ctx, &service); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Get(ctx, serviceKey, &service); err != nil {
		t.Fatalf("partial deletion was not repaired: %v", err)
	}

	restarted := &ClusterReconciler{Client: testClient, Scheme: testScheme, HTTPClient: server.Client(), RESTBaseURL: reconciler.RESTBaseURL}
	if _, err := restarted.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "research-slurmctld"}, &controllers); err != nil {
		t.Fatal(err)
	}
	if controllers.UID != originalUID {
		t.Fatal("operator restart replaced the controller StatefulSet")
	}

	if err := testClient.Get(ctx, request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.ControlPlane.Controllers.Image = "slurm:test-2"
	if err := testClient.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "research-slurmctld"}, &controllers); err != nil {
		t.Fatal(err)
	}
	if controllers.Spec.Template.Spec.Containers[0].Image != "slurm:test-2" || controllers.Spec.Template.Spec.Containers[1].Image != "slurm:test-2" {
		t.Fatal("image update was not reconciled")
	}
}

func createNamespace(t *testing.T, prefix string) string {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "phase1-" + prefix + "-"}}
	if err := testClient.Create(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	return namespace.Name
}

func createPrerequisites(t *testing.T, cluster *orchestrationv1alpha1.HeterogeneousCluster) {
	t.Helper()
	createStateClaim(t, cluster)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.Authentication.JWTKeySecretRef, Namespace: cluster.Namespace}, Data: map[string][]byte{jwtKey: []byte("01234567890123456789012345678901")}}
	if err := testClient.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
}

func createStateClaim(t *testing.T, cluster *orchestrationv1alpha1.HeterogeneousCluster) {
	t.Helper()
	ctx := context.Background()
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.ControlPlane.Controllers.StateSaveClaim, Namespace: cluster.Namespace}, Spec: corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
	}}
	if err := testClient.Create(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func validCluster(namespace string) *orchestrationv1alpha1.HeterogeneousCluster {
	return &orchestrationv1alpha1.HeterogeneousCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: orchestrationv1alpha1.GroupVersion.String(), Kind: "HeterogeneousCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "research", Namespace: namespace},
		Spec: orchestrationv1alpha1.HeterogeneousClusterSpec{
			ControlPlane: orchestrationv1alpha1.ControlPlaneSpec{
				Controllers: orchestrationv1alpha1.ControllersSpec{Image: "slurm:test", StateSaveClaim: "state"},
				Accounting:  orchestrationv1alpha1.AccountingSpec{DatabaseSecretRef: "database"},
				Login:       orchestrationv1alpha1.LoginSpec{Replicas: 1},
			},
			Authentication: orchestrationv1alpha1.AuthenticationSpec{MungeKeySecretRef: "munge", JWTKeySecretRef: "jwt"},
			WorkerPools: []orchestrationv1alpha1.WorkerPoolSpec{{
				Name: "strict", Partition: "compute", MemoryUnit: "1Gi",
				Scaling:  orchestrationv1alpha1.ScalingSpec{MaxWorkers: 4, IdleTimeout: metav1.Duration{Duration: 5 * time.Minute}},
				Profiles: []orchestrationv1alpha1.WorkerProfile{{Name: "gpu", Gres: "gpu:rtx_4050", DeviceClassName: "nvidia.gputpu.io/gpu"}},
			}},
		},
	}
}

func validUnstructured(namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": orchestrationv1alpha1.GroupVersion.String(),
		"kind":       "HeterogeneousCluster",
		"metadata":   map[string]any{"name": "defaults", "namespace": namespace},
		"spec": map[string]any{
			"controlPlane": map[string]any{
				"controllers": map[string]any{"image": "slurm:test", "stateSaveClaim": "state"},
				"accounting":  map[string]any{"databaseSecretRef": "database"},
				"login":       map[string]any{},
			},
			"authentication": map[string]any{"mungeKeySecretRef": "munge", "jwtKeySecretRef": "jwt"},
			"workerPools": []any{map[string]any{
				"name": "strict", "partition": "compute", "scaling": map[string]any{"maxWorkers": int64(4)},
			}},
		},
	}}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	return slices.ContainsFunc(volumes, func(volume corev1.Volume) bool { return volume.Name == name })
}

func hasVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	return slices.ContainsFunc(mounts, func(mount corev1.VolumeMount) bool { return mount.Name == name })
}
