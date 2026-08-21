package controllers

import (
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/render"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

const (
	requeueInterval      = 5 * time.Second
	controlPlaneReplicas = int32(2)
	mungeKey             = "munge.key"
	jwtKey               = "jwt_hs256.key"
)

// +kubebuilder:rbac:groups=orchestration.gputpu.io,resources=heterogeneousclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=orchestration.gputpu.io,resources=heterogeneousclusters/status,verbs=get;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update

type ClusterReconciler struct {
	client.Client
	Reader      client.Reader
	Scheme      *runtime.Scheme
	HTTPClient  *http.Client
	RESTBaseURL func(*orchestrationv1alpha1.HeterogeneousCluster) string
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	var cluster orchestrationv1alpha1.HeterogeneousCluster
	if err := r.Get(ctx, request.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if len(cluster.Name) > 50 || len(validation.IsDNS1035Label(cluster.Name)) != 0 {
		return r.notReady(ctx, &cluster, "InvalidSpec", "cluster name must be a DNS label starting with a letter and contain at most 50 characters", nil)
	}
	if err := validateSpec(cluster.Spec); err != nil {
		return r.notReady(ctx, &cluster, "InvalidSpec", err.Error(), nil)
	}
	if err := r.validateStateClaim(ctx, &cluster); err != nil {
		return r.notReady(ctx, &cluster, "InvalidStateClaim", err.Error(), nil)
	}
	key, err := r.jwtKey(ctx, &cluster)
	if err != nil {
		return r.notReady(ctx, &cluster, "JWTUnavailable", err.Error(), nil)
	}

	files := render.Slurm(&cluster)
	configHash := hash(files.SlurmConf, files.GRESConf)
	for _, reconcile := range []func() error{
		func() error { return r.reconcileConfig(ctx, &cluster, files) },
		func() error { return r.reconcileControllerService(ctx, &cluster) },
		func() error { return r.reconcileControllers(ctx, &cluster, configHash) },
		func() error { return r.reconcileControllerPDB(ctx, &cluster) },
		func() error { return r.reconcileDatabaseService(ctx, &cluster) },
		func() error { return r.reconcileDatabase(ctx, &cluster, configHash) },
		func() error { return r.reconcileRESTService(ctx, &cluster) },
		func() error { return r.reconcileREST(ctx, &cluster, configHash) },
		func() error { return r.reconcileLogin(ctx, &cluster, configHash) },
	} {
		if err := reconcile(); err != nil {
			return r.notReady(ctx, &cluster, "ReconcileFailed", err.Error(), err)
		}
	}

	ready, err := r.workloadsReady(ctx, &cluster)
	if err != nil {
		return r.notReady(ctx, &cluster, "ReadinessCheckFailed", err.Error(), err)
	}
	if !ready {
		return r.notReady(ctx, &cluster, "ResourcesNotReady", "Slurm control-plane workloads are not ready", nil)
	}

	restClient, err := slurm.NewClient(r.restURL(&cluster), "slurm", key, r.HTTPClient)
	if err != nil {
		return r.notReady(ctx, &cluster, "JWTUnavailable", err.Error(), nil)
	}
	if _, err := restClient.PendingJobs(ctx); err != nil {
		return r.notReady(ctx, &cluster, "RESTUnavailable", err.Error(), nil)
	}
	accountingError := restClient.AccountingReady(ctx, cluster.Name)

	status := cluster.Status.DeepCopy()
	status.ObservedGeneration = cluster.Generation
	status.WorkerPools = poolStatus(cluster.Spec.WorkerPools)
	setCondition(status, orchestrationv1alpha1.ConditionControlPlaneReady, metav1.ConditionTrue, "Ready", "Slurm controllers, REST, and login are ready", cluster.Generation)
	if accountingError == nil {
		setCondition(status, orchestrationv1alpha1.ConditionAccountingReady, metav1.ConditionTrue, "Ready", "Slurm accounting is ready", cluster.Generation)
	} else {
		setCondition(status, orchestrationv1alpha1.ConditionAccountingReady, metav1.ConditionFalse, "AccountingUnavailable", accountingError.Error(), cluster.Generation)
	}
	setPhaseOneConditions(status, cluster.Generation)
	if err := r.updateStatus(ctx, &cluster, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func validateSpec(spec orchestrationv1alpha1.HeterogeneousClusterSpec) error {
	gresMappings := make(map[string]string)
	for _, pool := range spec.WorkerPools {
		profileNames := make(map[string]struct{}, len(pool.Profiles))
		for _, profile := range pool.Profiles {
			if _, exists := profileNames[profile.Name]; exists {
				return fmt.Errorf("worker pool %q has duplicate profile name %q", pool.Name, profile.Name)
			}
			profileNames[profile.Name] = struct{}{}
			if owner, exists := gresMappings[profile.Gres]; exists {
				return fmt.Errorf("GRES mapping %q is used by profiles %q and %q", profile.Gres, owner, pool.Name+"/"+profile.Name)
			}
			gresMappings[profile.Gres] = pool.Name + "/" + profile.Name
		}
	}
	return nil
}

func (r *ClusterReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&orchestrationv1alpha1.HeterogeneousCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}

func (r *ClusterReconciler) validateStateClaim(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) error {
	var claim corev1.PersistentVolumeClaim
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Spec.ControlPlane.Controllers.StateSaveClaim}
	if err := r.Reader.Get(ctx, key, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("RWX state claim %q does not exist", key.Name)
		}
		return fmt.Errorf("read RWX state claim: %w", err)
	}
	if !slices.Contains(claim.Spec.AccessModes, corev1.ReadWriteMany) {
		return fmt.Errorf("state claim %q does not support ReadWriteMany", key.Name)
	}
	return nil
}

func (r *ClusterReconciler) reconcileConfig(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, files render.SlurmFiles) error {
	object := &corev1.ConfigMap{ObjectMeta: objectMeta(cluster, "config")}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		object.Labels = labels(cluster, "config")
		object.Data = map[string]string{"slurm.conf": files.SlurmConf, "gres.conf": files.GRESConf}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileControllerService(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) error {
	object := &corev1.Service{ObjectMeta: objectMeta(cluster, "slurmctld")}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		object.Labels = labels(cluster, "slurmctld")
		object.Spec.ClusterIP = corev1.ClusterIPNone
		object.Spec.PublishNotReadyAddresses = true
		object.Spec.Selector = labels(cluster, "slurmctld")
		object.Spec.Ports = []corev1.ServicePort{{Name: "slurmctld", Port: render.SlurmctldPort, TargetPort: intstr.FromInt32(render.SlurmctldPort)}}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileControllers(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, configHash string) error {
	object := &appsv1.StatefulSet{ObjectMeta: objectMeta(cluster, "slurmctld")}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		componentLabels := labels(cluster, "slurmctld")
		object.Labels = componentLabels
		object.Spec.Replicas = ptr.To(controlPlaneReplicas)
		object.Spec.ServiceName = name(cluster, "slurmctld")
		object.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		object.Spec.Selector = &metav1.LabelSelector{MatchLabels: componentLabels}
		object.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels, Annotations: map[string]string{"orchestration.gputpu.io/config-hash": configHash}},
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: ptr.To[int64](30),
				Affinity:                      requiredAntiAffinity(componentLabels),
				SecurityContext:               podFSGroup(64030),
				InitContainers: []corev1.Container{{
					Name:         "state-permissions",
					Image:        cluster.Spec.ControlPlane.Controllers.Image,
					Command:      []string{"chown", "64030:64030", "/var/lib/slurmctld"},
					VolumeMounts: []corev1.VolumeMount{{Name: "state", MountPath: "/var/lib/slurmctld"}},
				}},
				Containers: []corev1.Container{
					mungeContainer(cluster),
					{
						Name:    "slurmctld",
						Image:   cluster.Spec.ControlPlane.Controllers.Image,
						Command: []string{"slurmctld"},
						Args:    []string{"-D", "-f", "/etc/slurm/slurm.conf"},
						Env:     []corev1.EnvVar{{Name: "SLURM_CONF", Value: "/etc/slurm/slurm.conf"}},
						Ports:   []corev1.ContainerPort{{Name: "slurmctld", ContainerPort: render.SlurmctldPort}},
						VolumeMounts: append(slurmMounts(),
							corev1.VolumeMount{Name: "state", MountPath: "/var/lib/slurmctld"},
							corev1.VolumeMount{Name: "jwt", MountPath: "/run/secrets/slurm/jwt_hs256.key", SubPath: jwtKey, ReadOnly: true},
							corev1.VolumeMount{Name: "spool", MountPath: "/var/lib/slurmd"},
						),
						ReadinessProbe: execProbe("scontrol", "ping"),
					},
				},
				Volumes: append(commonVolumes(cluster),
					corev1.Volume{Name: "state", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cluster.Spec.ControlPlane.Controllers.StateSaveClaim}}},
					corev1.Volume{Name: "spool", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				),
			},
		}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileControllerPDB(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) error {
	object := &policyv1.PodDisruptionBudget{ObjectMeta: objectMeta(cluster, "slurmctld")}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		componentLabels := labels(cluster, "slurmctld")
		object.Labels = componentLabels
		object.Spec.MinAvailable = ptr.To(intstr.FromInt32(1))
		object.Spec.MaxUnavailable = nil
		object.Spec.Selector = &metav1.LabelSelector{MatchLabels: componentLabels}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileDatabaseService(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) error {
	return r.reconcileService(ctx, cluster, "slurmdbd", render.SlurmdbdPort)
}

func (r *ClusterReconciler) reconcileRESTService(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) error {
	return r.reconcileService(ctx, cluster, "slurmrestd", render.SlurmRESTPort)
}

func (r *ClusterReconciler) reconcileService(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, component string, port int32) error {
	object := &corev1.Service{ObjectMeta: objectMeta(cluster, component)}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		object.Labels = labels(cluster, component)
		object.Spec.Selector = labels(cluster, component)
		object.Spec.Ports = []corev1.ServicePort{{Name: component, Port: port, TargetPort: intstr.FromInt32(port)}}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileDatabase(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, configHash string) error {
	return r.reconcileDeployment(ctx, cluster, "slurmdbd", 1, configHash, corev1.PodSpec{
		Hostname:        "slurmdbd",
		SecurityContext: podFSGroup(64030),
		Containers: []corev1.Container{
			mungeContainer(cluster),
			{
				Name:    "slurmdbd",
				Image:   cluster.Spec.ControlPlane.Controllers.Image,
				Command: []string{"/bin/sh", "-ec"},
				Args:    []string{slurmdbdCommand},
				Env: append(databaseEnv(cluster),
					corev1.EnvVar{Name: "SLURM_CONF", Value: "/etc/slurm/slurm.conf"},
				),
				Ports: []corev1.ContainerPort{{Name: "slurmdbd", ContainerPort: render.SlurmdbdPort}},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "config", MountPath: "/config", ReadOnly: true},
					{Name: "munge-run", MountPath: "/run/munge"},
					corev1.VolumeMount{Name: "jwt", MountPath: "/run/secrets/slurm/jwt_hs256.key", SubPath: jwtKey, ReadOnly: true},
					corev1.VolumeMount{Name: "slurmdbd-config", MountPath: "/etc/slurm"},
				},
				ReadinessProbe: tcpProbe(render.SlurmdbdPort),
			},
		},
		Volumes: append(commonVolumes(cluster), corev1.Volume{Name: "slurmdbd-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}),
	})
}

func (r *ClusterReconciler) reconcileREST(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, configHash string) error {
	return r.reconcileDeployment(ctx, cluster, "slurmrestd", controlPlaneReplicas, configHash, corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](64031), RunAsGroup: ptr.To[int64](64031), RunAsNonRoot: ptr.To(true)},
		Containers: []corev1.Container{{
			Name:    "slurmrestd",
			Image:   cluster.Spec.ControlPlane.Controllers.Image,
			Command: []string{"slurmrestd"},
			Args:    []string{"-a", "rest_auth/jwt", "-s", "slurmctld,slurmdbd", "-d", "v0.0.44", fmt.Sprintf("0.0.0.0:%d", render.SlurmRESTPort)},
			Env: []corev1.EnvVar{
				{Name: "SLURM_CONF", Value: "/etc/slurm/slurm.conf"},
				{Name: "SLURM_JWT", Value: "daemon"},
			},
			Ports:          []corev1.ContainerPort{{Name: "http", ContainerPort: render.SlurmRESTPort}},
			VolumeMounts:   []corev1.VolumeMount{{Name: "config", MountPath: "/etc/slurm", ReadOnly: true}},
			ReadinessProbe: tcpProbe(render.SlurmRESTPort),
		}},
		Volumes: []corev1.Volume{configVolume(cluster)},
	})
}

func (r *ClusterReconciler) reconcileLogin(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, configHash string) error {
	confServer := fmt.Sprintf("%s-slurmctld-0.%s-slurmctld.%s.svc:%d", cluster.Name, cluster.Name, cluster.Namespace, render.SlurmctldPort)
	return r.reconcileDeployment(ctx, cluster, "login", cluster.Spec.ControlPlane.Login.Replicas, configHash, corev1.PodSpec{
		SecurityContext: podFSGroup(64030),
		Containers: []corev1.Container{
			mungeContainer(cluster),
			{
				Name:           "login",
				Image:          cluster.Spec.ControlPlane.Controllers.Image,
				Command:        []string{"sackd"},
				Args:           []string{"-D", "--conf-server", confServer},
				Env:            []corev1.EnvVar{{Name: "RUNTIME_DIRECTORY", Value: "/run/slurm"}, {Name: "SLURM_CONF", Value: "/run/slurm/conf/slurm.conf"}},
				VolumeMounts:   []corev1.VolumeMount{{Name: "munge-run", MountPath: "/run/munge"}, {Name: "slurm-run", MountPath: "/run/slurm"}},
				ReadinessProbe: execProbe("squeue", "--noheader"),
			},
		},
		Volumes: append(authVolumes(cluster), corev1.Volume{Name: "slurm-run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}),
	})
}

func (r *ClusterReconciler) reconcileDeployment(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, component string, replicas int32, configHash string, podSpec corev1.PodSpec) error {
	object := &appsv1.Deployment{ObjectMeta: objectMeta(cluster, component)}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		componentLabels := labels(cluster, component)
		object.Labels = componentLabels
		object.Spec.Replicas = ptr.To(replicas)
		object.Spec.Selector = &metav1.LabelSelector{MatchLabels: componentLabels}
		object.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels, Annotations: map[string]string{"orchestration.gputpu.io/config-hash": configHash}},
			Spec:       podSpec,
		}
		return controllerutil.SetControllerReference(cluster, object, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) workloadsReady(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) (bool, error) {
	var controllers appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name(cluster, "slurmctld")}, &controllers); err != nil {
		return false, err
	}
	if controllers.Status.ReadyReplicas != controlPlaneReplicas {
		return false, nil
	}
	for component, replicas := range map[string]int32{"slurmdbd": 1, "slurmrestd": controlPlaneReplicas, "login": cluster.Spec.ControlPlane.Login.Replicas} {
		var deployment appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name(cluster, component)}, &deployment); err != nil {
			return false, err
		}
		if deployment.Status.ReadyReplicas != replicas {
			return false, nil
		}
	}
	return true, nil
}

func (r *ClusterReconciler) jwtKey(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster) ([]byte, error) {
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Spec.Authentication.JWTKeySecretRef}
	if err := r.Reader.Get(ctx, key, &secret); err != nil {
		return nil, fmt.Errorf("read JWT Secret %q: %w", key.Name, err)
	}
	value := secret.Data[jwtKey]
	if len(value) < 32 {
		return nil, fmt.Errorf("JWT Secret %q key %q must contain at least 32 bytes", key.Name, jwtKey)
	}
	return value, nil
}

func (r *ClusterReconciler) restURL(cluster *orchestrationv1alpha1.HeterogeneousCluster) string {
	if r.RESTBaseURL != nil {
		return r.RESTBaseURL(cluster)
	}
	return fmt.Sprintf("http://%s-slurmrestd.%s.svc:%d", cluster.Name, cluster.Namespace, render.SlurmRESTPort)
}

func (r *ClusterReconciler) notReady(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, reason, message string, reconcileError error) (ctrl.Result, error) {
	status := cluster.Status.DeepCopy()
	status.ObservedGeneration = cluster.Generation
	status.WorkerPools = poolStatus(cluster.Spec.WorkerPools)
	setCondition(status, orchestrationv1alpha1.ConditionControlPlaneReady, metav1.ConditionFalse, reason, message, cluster.Generation)
	setCondition(status, orchestrationv1alpha1.ConditionAccountingReady, metav1.ConditionFalse, reason, message, cluster.Generation)
	setPhaseOneConditions(status, cluster.Generation)
	if err := r.updateStatus(ctx, cluster, status); err != nil {
		return ctrl.Result{}, err
	}
	if reconcileError != nil {
		return ctrl.Result{}, reconcileError
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, status *orchestrationv1alpha1.HeterogeneousClusterStatus) error {
	if reflect.DeepEqual(cluster.Status, *status) {
		return nil
	}
	cluster.Status = *status
	return r.Status().Update(ctx, cluster)
}

func setCondition(status *orchestrationv1alpha1.HeterogeneousClusterStatus, conditionType string, value metav1.ConditionStatus, reason, message string, generation int64) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             value,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func setPhaseOneConditions(status *orchestrationv1alpha1.HeterogeneousClusterStatus, generation int64) {
	for _, conditionType := range []string{
		orchestrationv1alpha1.ConditionWorkersReady,
		orchestrationv1alpha1.ConditionCheckpointStoreReachable,
		orchestrationv1alpha1.ConditionDegradedNodes,
	} {
		setCondition(status, conditionType, metav1.ConditionUnknown, "PhaseNotActive", "This condition is managed by a later implementation phase", generation)
	}
}

func poolStatus(pools []orchestrationv1alpha1.WorkerPoolSpec) []orchestrationv1alpha1.WorkerPoolStatus {
	status := make([]orchestrationv1alpha1.WorkerPoolStatus, len(pools))
	for i, pool := range pools {
		status[i].Name = pool.Name
	}
	slices.SortFunc(status, func(a, b orchestrationv1alpha1.WorkerPoolStatus) int { return cmp.Compare(a.Name, b.Name) })
	return status
}

func objectMeta(cluster *orchestrationv1alpha1.HeterogeneousCluster, component string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name(cluster, component), Namespace: cluster.Namespace}
}

func name(cluster *orchestrationv1alpha1.HeterogeneousCluster, component string) string {
	return cluster.Name + "-" + component
}

func labels(cluster *orchestrationv1alpha1.HeterogeneousCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "heterogeneous-cluster",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "slurm-operator",
	}
}

func hash(values ...string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(values))))
}

func commonVolumes(cluster *orchestrationv1alpha1.HeterogeneousCluster) []corev1.Volume {
	return append(authVolumes(cluster),
		configVolume(cluster),
		jwtVolume(cluster),
	)
}

func jwtVolume(cluster *orchestrationv1alpha1.HeterogeneousCluster) corev1.Volume {
	return corev1.Volume{Name: "jwt", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Authentication.JWTKeySecretRef, Items: []corev1.KeyToPath{{Key: jwtKey, Path: jwtKey, Mode: ptr.To[int32](0440)}}}}}
}

func authVolumes(cluster *orchestrationv1alpha1.HeterogeneousCluster) []corev1.Volume {
	return []corev1.Volume{
		{Name: "munge-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Authentication.MungeKeySecretRef, Items: []corev1.KeyToPath{{Key: mungeKey, Path: mungeKey, Mode: ptr.To[int32](0400)}}}}},
		{Name: "munge-run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
}

func configVolume(cluster *orchestrationv1alpha1.HeterogeneousCluster) corev1.Volume {
	return corev1.Volume{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: name(cluster, "config")}}}}
}

func mungeContainer(cluster *orchestrationv1alpha1.HeterogeneousCluster) corev1.Container {
	return corev1.Container{
		Name:           "munge",
		Image:          cluster.Spec.ControlPlane.Controllers.Image,
		Command:        []string{"munged"},
		Args:           []string{"--foreground", "--force", "--key-file=/etc/munge/munge.key", "--socket=/run/munge/munge.socket.2"},
		VolumeMounts:   []corev1.VolumeMount{{Name: "munge-key", MountPath: "/etc/munge/munge.key", SubPath: mungeKey, ReadOnly: true}, {Name: "munge-run", MountPath: "/run/munge"}},
		ReadinessProbe: execProbe("munge", "-n"),
	}
}

func slurmMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{{Name: "config", MountPath: "/etc/slurm", ReadOnly: true}, {Name: "munge-run", MountPath: "/run/munge"}}
}

func requiredAntiAffinity(matchLabels map[string]string) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		TopologyKey:   corev1.LabelHostname,
	}}}}
}

func execProbe(command ...string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: command}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    6,
	}
}

func tcpProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    6,
	}
}

func databaseEnv(cluster *orchestrationv1alpha1.HeterogeneousCluster) []corev1.EnvVar {
	secret := cluster.Spec.ControlPlane.Accounting.DatabaseSecretRef
	keys := []string{"host", "port", "database", "username", "password"}
	result := make([]corev1.EnvVar, len(keys))
	for i, key := range keys {
		result[i] = corev1.EnvVar{
			Name: "DB_" + map[string]string{"host": "HOST", "port": "PORT", "database": "NAME", "username": "USER", "password": "PASSWORD"}[key],
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
				Key:                  key,
			}},
		}
	}
	return result
}

const slurmdbdCommand = `umask 077
cp /config/slurm.conf /etc/slurm/slurm.conf
printf '%s\n' \
  'AuthType=auth/munge' \
  'AuthInfo=/run/munge/munge.socket.2' \
  'AuthAltTypes=auth/jwt' \
  'AuthAltParameters=jwt_key=/run/secrets/slurm/jwt_hs256.key,disable_token_creation' \
  'DbdHost=slurmdbd' \
  'DbdPort=6819' \
  'SlurmUser=slurm' \
  'StorageType=accounting_storage/mysql' \
  "StorageHost=$DB_HOST" \
  "StoragePort=$DB_PORT" \
  "StorageLoc=$DB_NAME" \
  "StorageUser=$DB_USER" \
  "StoragePass=$DB_PASSWORD" \
  'PidFile=/run/slurmdbd.pid' > /etc/slurm/slurmdbd.conf
exec slurmdbd -D`

func podFSGroup(group int64) *corev1.PodSecurityContext {
	policy := corev1.FSGroupChangeOnRootMismatch
	return &corev1.PodSecurityContext{FSGroup: ptr.To(group), FSGroupChangePolicy: &policy}
}
