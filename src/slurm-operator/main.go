package main

import (
	"flag"
	"fmt"
	"os"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/controllers"
)

// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;create;update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func main() {
	var metricsAddress, probeAddress string
	flag.StringVar(&metricsAddress, "metrics-bind-address", ":8080", "metrics listen address")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "health probe listen address")
	logging := zap.Options{Development: false}
	logging.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logging)))

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		exit(fmt.Errorf("POD_NAMESPACE is required"))
	}
	workerImage := os.Getenv("WORKER_IMAGE")
	if workerImage == "" {
		exit(fmt.Errorf("WORKER_IMAGE is required"))
	}
	headroom, err := resource.ParseQuantity(envOr("WORKER_MEMORY_HEADROOM", "256Mi"))
	if err != nil || headroom.Value() < 0 {
		exit(fmt.Errorf("WORKER_MEMORY_HEADROOM must be a non-negative Kubernetes quantity"))
	}

	scheme := clientgoscheme.Scheme
	if err := resourceapi.AddToScheme(scheme); err != nil {
		exit(err)
	}
	if err := orchestrationv1alpha1.AddToScheme(scheme); err != nil {
		exit(err)
	}
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddress},
		HealthProbeBindAddress:  probeAddress,
		LeaderElection:          true,
		LeaderElectionID:        "slurm-operator.orchestration.gputpu.io",
		LeaderElectionNamespace: namespace,
		Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{
			namespace: {},
		}},
	})
	if err != nil {
		exit(err)
	}
	if err := (&controllers.ClusterReconciler{Client: manager.GetClient(), Reader: manager.GetAPIReader(), Scheme: manager.GetScheme(), Recorder: manager.GetEventRecorderFor("slurm-operator"), WorkerImage: workerImage, WorkerMemoryHeadroom: headroom.Value()}).SetupWithManager(manager); err != nil {
		exit(err)
	}
	if err := manager.Add(&cloudBurstServer{reader: manager.GetAPIReader(), client: manager.GetClient()}); err != nil {
		exit(err)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		exit(err)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		exit(err)
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		exit(err)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func exit(err error) {
	ctrl.Log.Error(err, "operator stopped")
	os.Exit(1)
}
