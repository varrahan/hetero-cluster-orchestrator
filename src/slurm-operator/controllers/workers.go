package controllers

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	resourceplan "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/resources"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

const (
	workerComponent        = "slurmd"
	workerFinalizer        = "orchestration.gputpu.io/slurm-node-cleanup"
	workerPoolLabel        = "orchestration.gputpu.io/worker-pool"
	workerShapeAnnotation  = "orchestration.gputpu.io/worker-shape"
	workerClaimAnnotation  = "orchestration.gputpu.io/resource-claim"
	workerIdleAnnotation   = "orchestration.gputpu.io/idle-since"
	workerDrainAnnotation  = "orchestration.gputpu.io/draining"
	clusterWorkerFinalizer = "orchestration.gputpu.io/worker-cleanup"
	driverName             = "orchestration.gputpu.io"
)

type workerResult struct {
	Status  []orchestrationv1alpha1.WorkerPoolStatus
	Ready   bool
	Reason  string
	Message string
}

func (r *ClusterReconciler) cleanupClusterWorkers(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, restClient *slurm.Client) (bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster, workerComponent))); err != nil {
		return false, err
	}
	if len(pods.Items) == 0 {
		return true, nil
	}
	nodes, err := restClient.Nodes(ctx)
	if err != nil {
		return false, err
	}
	byName := map[string]slurm.Node{}
	for _, node := range nodes {
		byName[node.Name] = node
	}
	for i := range pods.Items {
		if err := validateWorkerPod(cluster, &pods.Items[i]); err != nil {
			return false, err
		}
		if err := r.cleanupWorker(ctx, &pods.Items[i], restClient, byName[pods.Items[i].Name]); err != nil {
			return false, err
		}
	}
	return false, nil
}

type workerCandidate struct {
	pod   *corev1.Pod
	shape resourceplan.Shape
}

func (r *ClusterReconciler) reconcileWorkers(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, restClient *slurm.Client, jobs []slurm.PendingJob) (workerResult, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster, workerComponent))); err != nil {
		return workerResult{}, fmt.Errorf("list worker Pods: %w", err)
	}
	nodes, err := restClient.Nodes(ctx)
	if err != nil {
		return workerResult{Status: statuses(cluster.Spec.WorkerPools, podList.Items), Ready: false, Reason: "RESTUnavailable", Message: err.Error()}, nil
	}
	nodeByName := make(map[string]slurm.Node, len(nodes))
	for _, node := range nodes {
		nodeByName[node.Name] = node
	}
	podNames := make(map[string]bool, len(podList.Items))

	for i := range podList.Items {
		pod := &podList.Items[i]
		if err := validateWorkerPod(cluster, pod); err != nil {
			return workerResult{}, err
		}
		podNames[pod.Name] = true
		if pod.Annotations[workerRecoveryAnnotation] != "" {
			continue
		}
		if !pod.DeletionTimestamp.IsZero() {
			if err := r.cleanupWorker(ctx, pod, restClient, nodeByName[pod.Name]); err != nil {
				return workerResult{}, err
			}
			continue
		}
		if !controllerutil.ContainsFinalizer(pod, workerFinalizer) {
			copy := pod.DeepCopy()
			controllerutil.AddFinalizer(copy, workerFinalizer)
			if err := r.Update(ctx, copy); err != nil {
				return workerResult{}, fmt.Errorf("restore worker Pod finalizer: %w", err)
			}
			pod.Finalizers = copy.Finalizers
		}
		if pod.Annotations[workerDrainAnnotation] == "true" || pod.Status.Phase == corev1.PodFailed {
			if err := r.cleanupWorker(ctx, pod, restClient, nodeByName[pod.Name]); err != nil {
				return workerResult{}, err
			}
			continue
		}
		if node := nodeByName[pod.Name]; node.Name != "" {
			var claim resourceapi.ResourceClaim
			claimName := pod.Annotations[workerClaimAnnotation]
			var err error
			if claimName != "" {
				err = r.Reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: claimName}, &claim)
			}
			if err != nil && !apierrors.IsNotFound(err) {
				return workerResult{}, err
			}
			if claimName == "" || apierrors.IsNotFound(err) || claim.Status.Allocation == nil || !metav1.IsControlledBy(&claim, pod) {
				if err := r.cleanupWorker(ctx, pod, restClient, node); err != nil {
					return workerResult{}, err
				}
				continue
			}
		}
		if err := r.ensureWorkerClaim(ctx, cluster, pod); err != nil {
			return workerResult{}, err
		}
	}
	for _, node := range nodes {
		if podNames[node.Name] || !managedWorkerNode(cluster, node.Name) {
			continue
		}
		if slices.Contains(node.State, "DRAIN") && nodeIdle(node) {
			if err := restClient.DeleteNode(ctx, node.Name); err != nil {
				return workerResult{}, err
			}
		} else if err := restClient.DrainNode(ctx, node.Name, "orphaned elastic worker"); err != nil {
			return workerResult{}, err
		}
	}

	footprints, err := r.openTPUFootprints(ctx)
	if err != nil {
		return workerResult{}, err
	}
	demands, rejected := resourceplan.Demands(jobs, cluster.Spec.WorkerPools, footprints)
	for _, rejection := range rejected {
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "WorkerDemandRejected", rejection.Error())
		}
	}

	pools := make(map[string]orchestrationv1alpha1.WorkerPoolSpec, len(cluster.Spec.WorkerPools))
	counts := make(map[string]int, len(cluster.Spec.WorkerPools))
	for _, pool := range cluster.Spec.WorkerPools {
		pools[pool.Name] = pool
	}
	var candidates []workerCandidate
	for i := range podList.Items {
		pod := &podList.Items[i]
		pool := pod.Labels[workerPoolLabel]
		if _, ok := pools[pool]; !ok || !pod.DeletionTimestamp.IsZero() || pod.Status.Phase == corev1.PodFailed || pod.Annotations[workerRecoveryAnnotation] != "" {
			continue
		}
		counts[pool]++
		shape, err := podShape(pod)
		if err != nil {
			return workerResult{}, fmt.Errorf("worker Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		ready := podReady(pod)
		node, registered := nodeByName[pod.Name]
		idle := ready && registered && nodeIdle(node)
		if !ready || !registered || idle {
			candidates = append(candidates, workerCandidate{pod: pod, shape: shape})
		}
	}

	groups := make([]uint32, 0)
	byGroup := map[uint32][]resourceplan.Demand{}
	for _, demand := range demands {
		if _, exists := byGroup[demand.GroupID]; !exists {
			groups = append(groups, demand.GroupID)
		}
		byGroup[demand.GroupID] = append(byGroup[demand.GroupID], demand)
	}
	assigned := map[string]bool{}
	used := map[string]bool{}
	for _, group := range groups {
		localUsed := map[string]bool{}
		var creates []resourceplan.Demand
		for _, demand := range byGroup[group] {
			for range demand.Count {
				index := bestCandidate(candidates, used, localUsed, demand)
				if index >= 0 {
					localUsed[candidates[index].pod.Name] = true
				} else {
					creates = append(creates, resourceplan.Demand{JobID: demand.JobID, GroupID: demand.GroupID, PoolName: demand.PoolName, Count: 1, Shape: demand.Shape})
				}
			}
		}
		createCounts := map[string]int{}
		fits := true
		for _, demand := range creates {
			createCounts[demand.PoolName]++
			if _, err := resourceClaim(cluster.Namespace, "validation", demand.Shape, pools[demand.PoolName], r.WorkerMemoryHeadroom); err != nil {
				fits = false
				if r.Recorder != nil {
					r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "WorkerDemandRejected", "job %d: %v", demand.JobID, err)
				}
			}
		}
		for pool, count := range createCounts {
			if counts[pool]+count > int(pools[pool].Scaling.MaxWorkers) {
				fits = false
				break
			}
		}
		if !fits {
			if r.Recorder != nil {
				r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "WorkerPoolLimit", "heterogeneous job group %d exceeds worker pool limits", group)
			}
			continue
		}
		for name := range localUsed {
			used[name] = true
			assigned[name] = true
		}
		for _, demand := range creates {
			if r.WorkerImage == "" {
				return workerResult{}, fmt.Errorf("WORKER_IMAGE is required when worker demand exists")
			}
			if _, err := r.createWorker(ctx, cluster, pools[demand.PoolName], demand); err != nil {
				return workerResult{}, err
			}
			counts[demand.PoolName]++
		}
	}

	readyByPool := map[string]int{}
	for i := range podList.Items {
		pod := &podList.Items[i]
		pool, ok := pools[pod.Labels[workerPoolLabel]]
		if !ok || !pod.DeletionTimestamp.IsZero() || pod.Annotations[workerDrainAnnotation] == "true" || pod.Annotations[workerRecoveryAnnotation] != "" {
			continue
		}
		if podReady(pod) {
			readyByPool[pool.Name]++
		}
		node, registered := nodeByName[pod.Name]
		idleCapacity := !assigned[pod.Name] && (!podReady(pod) || registered && nodeIdle(node))
		if !idleCapacity {
			if pod.Annotations[workerIdleAnnotation] != "" {
				if err := r.setWorkerAnnotation(ctx, pod, workerIdleAnnotation, ""); err != nil {
					return workerResult{}, err
				}
			}
			continue
		}
		if !podReady(pod) || !registered {
			if err := r.cleanupWorker(ctx, pod, restClient, node); err != nil {
				return workerResult{}, err
			}
			continue
		}
		if readyByPool[pool.Name] <= int(pool.Scaling.MinReady) {
			continue
		}
		idleSince, err := time.Parse(time.RFC3339, pod.Annotations[workerIdleAnnotation])
		if err != nil {
			if err := r.setWorkerAnnotation(ctx, pod, workerIdleAnnotation, time.Now().UTC().Format(time.RFC3339)); err != nil {
				return workerResult{}, err
			}
			continue
		}
		if time.Since(idleSince) >= pool.Scaling.IdleTimeout.Duration {
			if err := r.cleanupWorker(ctx, pod, restClient, node); err != nil {
				return workerResult{}, err
			}
			readyByPool[pool.Name]--
		}
	}

	result := workerResult{Status: statuses(cluster.Spec.WorkerPools, podList.Items), Ready: len(rejected) == 0, Reason: "Ready", Message: "elastic workers are reconciled"}
	if len(rejected) > 0 {
		result.Reason, result.Message = "DemandRejected", rejected[0].Error()
	}
	return result, nil
}

func bestCandidate(candidates []workerCandidate, used, localUsed map[string]bool, demand resourceplan.Demand) int {
	best := -1
	for i, candidate := range candidates {
		if used[candidate.pod.Name] || localUsed[candidate.pod.Name] || candidate.pod.Labels[workerPoolLabel] != demand.PoolName || !shapeFits(candidate.shape, demand.Shape) {
			continue
		}
		if best == -1 || lessWaste(candidate, candidates[best], demand.Shape) {
			best = i
		}
	}
	return best
}

func shapeFits(candidate, wanted resourceplan.Shape) bool {
	if candidate.CPUs < wanted.CPUs || candidate.MemoryBytes < wanted.MemoryBytes || len(candidate.GRES) != len(wanted.GRES) {
		return false
	}
	for name, count := range wanted.GRES {
		if candidate.GRES[name] != count {
			return false
		}
	}
	return true
}

func lessWaste(a, b workerCandidate, wanted resourceplan.Shape) bool {
	aWaste, bWaste := a.shape.CPUs-wanted.CPUs, b.shape.CPUs-wanted.CPUs
	if order := cmp.Compare(aWaste, bWaste); order != 0 {
		return order < 0
	}
	if order := cmp.Compare(a.shape.MemoryBytes-wanted.MemoryBytes, b.shape.MemoryBytes-wanted.MemoryBytes); order != 0 {
		return order < 0
	}
	return a.pod.Name < b.pod.Name
}

func (r *ClusterReconciler) createWorker(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, pool orchestrationv1alpha1.WorkerPoolSpec, demand resourceplan.Demand) (*corev1.Pod, error) {
	podName := fmt.Sprintf("%s-%s-%s", cluster.Name, pool.Name, strings.ToLower(rand.Text()[:10]))
	claimName := podName + "-resources"
	shapeJSON, _ := json.Marshal(demand.Shape)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: podName, Namespace: cluster.Namespace, Finalizers: []string{workerFinalizer},
		Labels:      labels(cluster, workerComponent),
		Annotations: map[string]string{workerShapeAnnotation: string(shapeJSON), workerClaimAnnotation: claimName},
	}}
	pod.Labels[workerPoolLabel] = pool.Name
	if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
		return nil, err
	}
	resources, err := workerResources(demand.Shape, r.WorkerMemoryHeadroom)
	if err != nil {
		return nil, err
	}
	claimUse := []corev1.ResourceClaim{{Name: "allocation"}}
	claimRef := []corev1.PodResourceClaim{{Name: "allocation", ResourceClaimName: new(claimName)}}
	nodeSelector := map[string]string{"orchestration.gputpu.io/compute": "true"}
	for key, value := range pool.NodeSelector {
		nodeSelector[key] = value
	}
	sharedMemory := resource.NewQuantity(max(demand.Shape.SharedMemoryBytes, 64<<20), resource.BinarySI)
	pod.Spec = corev1.PodSpec{
		Hostname: podName, ServiceAccountName: "slurm-worker", RestartPolicy: corev1.RestartPolicyNever,
		NodeSelector:   nodeSelector,
		Tolerations:    []corev1.Toleration{{Key: "orchestration.gputpu.io/compute", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
		ResourceClaims: claimRef,
		InitContainers: []corev1.Container{{
			Name: "gres-init", Image: r.WorkerImage, Command: []string{"/bin/sh", "-ec"},
			Args:      []string{"munged --force --key-file=/etc/munge/munge.key --socket=/run/munge/munge.socket.2; exec /usr/local/bin/gres-init"},
			Resources: resources, VolumeMounts: workerMounts(), Env: workerEnv(cluster, pool, claimName),
			SecurityContext: &corev1.SecurityContext{Privileged: new(true)},
		}},
		Containers: []corev1.Container{{
			Name: "slurmd", Image: r.WorkerImage, Command: []string{"/bin/sh", "-ec"},
			Args:      []string{"munged --force --key-file=/etc/munge/munge.key --socket=/run/munge/munge.socket.2; . /etc/slurm/worker.env; exec slurmd -D -Z --conf \"$SLURMD_CONF\""},
			Resources: resources, VolumeMounts: workerMounts(), Env: workerEnv(cluster, pool, claimName),
			SecurityContext: &corev1.SecurityContext{Privileged: new(true)},
			ReadinessProbe:  execProbe("/bin/sh", "-ec", ". /etc/slurm/worker.env; scontrol show node \"$HOSTNAME\" >/dev/null"),
		}},
		Volumes: []corev1.Volume{
			{Name: "worker-config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "slurm-spool", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "munge-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Authentication.MungeKeySecretRef, Items: []corev1.KeyToPath{{Key: mungeKey, Path: mungeKey, Mode: new(int32(0400))}}}}},
			{Name: "munge-run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "cgroup", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys/fs/cgroup", Type: new(corev1.HostPathDirectory)}}},
			{Name: "shared-memory", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: sharedMemory}}},
		},
	}
	if cluster.Spec.Checkpointing != nil {
		checkpointMounts := []corev1.VolumeMount{
			{Name: "checkpoint-socket", MountPath: "/run/gputpu-checkpoint"},
			{Name: "checkpoint-shm", MountPath: "/dev/shm/ai-orch"},
			{Name: "quantization-socket", MountPath: "/run/gputpu-quantization"},
		}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, checkpointMounts...)
		checkpointResources := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
			Name: "checkpoint-flusher", Image: r.WorkerImage, Command: []string{"/usr/local/bin/checkpoint-flusher"},
			Env: []corev1.EnvVar{
				{Name: "CHECKPOINT_CLUSTER_UID", Value: string(cluster.UID)},
				{Name: "CHECKPOINT_SHM_BUDGET_BYTES", Value: strconv.FormatInt(sharedMemory.Value(), 10)},
			},
			Resources: corev1.ResourceRequirements{Requests: checkpointResources.DeepCopy(), Limits: checkpointResources.DeepCopy()},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "checkpoint-socket", MountPath: "/run/gputpu-checkpoint"},
				{Name: "checkpoint-shm", MountPath: "/dev/shm/ai-orch"},
				{Name: "checkpoint-store", MountPath: "/run/secrets/checkpoint-store", ReadOnly: true},
			},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			ReadinessProbe:  execProbe("test", "-S", "/run/gputpu-checkpoint/flusher.sock"),
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			corev1.Volume{Name: "checkpoint-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			corev1.Volume{Name: "checkpoint-shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/shm/ai-orch", Type: new(corev1.HostPathDirectoryOrCreate)}}},
			corev1.Volume{Name: "quantization-socket", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/run/gputpu-quantization", Type: new(corev1.HostPathDirectoryOrCreate)}}},
			corev1.Volume{Name: "checkpoint-store", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cluster.Spec.Checkpointing.ObjectStoreSecretRef}}},
		)
	}
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Resources.Claims = claimUse
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "slurmd" {
			pod.Spec.Containers[i].Resources.Claims = claimUse
		}
	}
	if err := r.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("create worker Pod: %w", err)
	}
	if err := r.ensureWorkerClaim(ctx, cluster, pod); err != nil {
		return nil, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "WorkerCreated", "created worker %s for job %d", pod.Name, demand.JobID)
	}
	return pod, nil
}

func workerResources(shape resourceplan.Shape, headroom int64) (corev1.ResourceRequirements, error) {
	if shape.CPUs < 1 || shape.MemoryBytes < 1 {
		return corev1.ResourceRequirements{}, fmt.Errorf("invalid worker shape")
	}
	values := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(shape.CPUs, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(shape.MemoryBytes+headroom, resource.BinarySI),
	}
	return corev1.ResourceRequirements{Requests: values.DeepCopy(), Limits: values.DeepCopy()}, nil
}

func workerEnv(cluster *orchestrationv1alpha1.HeterogeneousCluster, pool orchestrationv1alpha1.WorkerPoolSpec, claim string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		{Name: "RESOURCE_CLAIM", Value: claim}, {Name: "WORKER_POOL", Value: pool.Name},
		{Name: "SLURM_CONF_SERVER", Value: slurmConfigServers(cluster)},
	}
}

func workerMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: "worker-config", MountPath: "/etc/slurm"},
		{Name: "slurm-spool", MountPath: "/var/lib/slurmd"},
		{Name: "shared-memory", MountPath: "/dev/shm"},
		{Name: "munge-key", MountPath: "/etc/munge/munge.key", SubPath: mungeKey, ReadOnly: true},
		{Name: "munge-run", MountPath: "/run/munge"},
		{Name: "cgroup", MountPath: "/sys/fs/cgroup"},
	}
}

func (r *ClusterReconciler) ensureWorkerClaim(ctx context.Context, cluster *orchestrationv1alpha1.HeterogeneousCluster, pod *corev1.Pod) error {
	claimName := pod.Annotations[workerClaimAnnotation]
	if claimName == "" {
		return fmt.Errorf("worker Pod %q has no resource claim annotation", pod.Name)
	}
	var existing resourceapi.ResourceClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: claimName}, &existing); err == nil {
		if !metav1.IsControlledBy(&existing, pod) {
			return fmt.Errorf("worker ResourceClaim %s/%s is not controlled by Pod %s", pod.Namespace, claimName, pod.Name)
		}
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	shape, err := podShape(pod)
	if err != nil {
		return err
	}
	var poolSpec *orchestrationv1alpha1.WorkerPoolSpec
	for i := range cluster.Spec.WorkerPools {
		if cluster.Spec.WorkerPools[i].Name == pod.Labels[workerPoolLabel] {
			poolSpec = &cluster.Spec.WorkerPools[i]
			break
		}
	}
	if poolSpec == nil {
		return fmt.Errorf("worker Pod %q references unknown pool", pod.Name)
	}
	claim, err := resourceClaim(pod.Namespace, claimName, shape, *poolSpec, r.WorkerMemoryHeadroom)
	if err != nil {
		return err
	}
	if err := controllerutil.SetControllerReference(pod, claim, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create worker ResourceClaim: %w", err)
	}
	return nil
}

func resourceClaim(namespace, name string, shape resourceplan.Shape, pool orchestrationv1alpha1.WorkerPoolSpec, headroom int64) (*resourceapi.ResourceClaim, error) {
	unit, err := resource.ParseQuantity(pool.MemoryUnit)
	if err != nil || unit.Value() < 1 {
		return nil, fmt.Errorf("invalid memory unit %q", pool.MemoryUnit)
	}
	memoryCount := (shape.MemoryBytes + headroom + unit.Value() - 1) / unit.Value()
	total := shape.CPUs + memoryCount
	requests := []resourceapi.DeviceRequest{
		exactRequest("cpu", "cpu.orchestration.gputpu.io", shape.CPUs, kindSelector("cpu")),
		exactRequest("memory", "memory.orchestration.gputpu.io", memoryCount, kindAndValueSelector("memory", "unitBytes", strconv.FormatInt(unit.Value(), 10), false)),
	}
	profiles := map[string]orchestrationv1alpha1.WorkerProfile{}
	for _, profile := range pool.Profiles {
		profiles[profile.Gres] = profile
	}
	names := slices.Sorted(maps.Keys(shape.GRES))
	for _, gres := range names {
		profile, ok := profiles[gres]
		if !ok {
			return nil, fmt.Errorf("GRES %q has no profile", gres)
		}
		kind, model, _ := strings.Cut(gres, ":")
		attribute := "model"
		if kind == "tpu" {
			attribute = "profile"
		}
		count := shape.GRES[gres]
		total += count
		requests = append(requests, exactRequest("accelerator-"+profile.Name, profile.DeviceClassName, count, kindAndValueSelector(map[string]string{"gpu": "gpu", "tpu": "opentpu"}[kind], attribute, model, true)))
	}
	if total > resourceapi.AllocationResultsMaxSize {
		return nil, fmt.Errorf("worker shape needs %d DRA devices; maximum is %d, increase memoryUnit", total, resourceapi.AllocationResultsMaxSize)
	}
	match := resourceapi.FullyQualifiedName(driverName + "/numaNode")
	return &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{Requests: requests, Constraints: []resourceapi.DeviceConstraint{{MatchAttribute: &match}}}}}, nil
}

func exactRequest(name, class string, count int64, selector resourceapi.DeviceSelector) resourceapi.DeviceRequest {
	return resourceapi.DeviceRequest{Name: name, Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: class, AllocationMode: resourceapi.DeviceAllocationModeExactCount, Count: count, Selectors: []resourceapi.DeviceSelector{selector}}}
}

func kindSelector(kind string) resourceapi.DeviceSelector {
	return celSelector(fmt.Sprintf(`device.driver == %q && device.attributes[%q].kind == %q`, driverName, driverName, kind))
}

func kindAndValueSelector(kind, attribute, value string, quote bool) resourceapi.DeviceSelector {
	wanted := value
	if quote {
		wanted = strconv.Quote(value)
	}
	return celSelector(fmt.Sprintf(`device.driver == %q && device.attributes[%q].kind == %q && device.attributes[%q][%q] == %s`, driverName, driverName, kind, driverName, attribute, wanted))
}

func celSelector(expression string) resourceapi.DeviceSelector {
	return resourceapi.DeviceSelector{CEL: &resourceapi.CELDeviceSelector{Expression: expression}}
}

func (r *ClusterReconciler) openTPUFootprints(ctx context.Context) (map[string]resourceplan.Footprint, error) {
	var list resourceapi.ResourceSliceList
	if err := r.Reader.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list ResourceSlices: %w", err)
	}
	selected, err := completeDriverSlices(list.Items)
	if err != nil {
		return nil, err
	}
	result := map[string]resourceplan.Footprint{}
	for _, slice := range selected {
		for _, device := range slice.Spec.Devices {
			kind, ok := stringAttr(device, driverName+"/kind")
			if !ok || kind != "opentpu" {
				continue
			}
			profile, ok := stringAttr(device, driverName+"/profile")
			cpus, cpuOK := intAttr(device, driverName+"/cpuCores")
			memory, memOK := capacityBytes(device, driverName+"/memory")
			shared, sharedOK := capacityBytes(device, driverName+"/sharedMemory")
			if !ok || !cpuOK || !memOK || !sharedOK {
				return nil, fmt.Errorf("OpenTPU device %q has incomplete footprint attributes", device.Name)
			}
			key := "tpu:" + profile
			footprint := resourceplan.Footprint{CPUs: cpus, MemoryBytes: memory, SharedMemory: shared}
			if old, exists := result[key]; exists && old != footprint {
				return nil, fmt.Errorf("OpenTPU profile %q has inconsistent footprints", profile)
			}
			result[key] = footprint
		}
	}
	return result, nil
}

func completeDriverSlices(slices []resourceapi.ResourceSlice) ([]resourceapi.ResourceSlice, error) {
	generations := map[string]int64{}
	for _, slice := range slices {
		if slice.Spec.Driver == driverName && slice.Spec.Pool.Generation > generations[slice.Spec.Pool.Name] {
			generations[slice.Spec.Pool.Name] = slice.Spec.Pool.Generation
		}
	}
	byPool := map[string][]resourceapi.ResourceSlice{}
	for _, slice := range slices {
		if slice.Spec.Driver == driverName && slice.Spec.Pool.Generation == generations[slice.Spec.Pool.Name] {
			byPool[slice.Spec.Pool.Name] = append(byPool[slice.Spec.Pool.Name], slice)
		}
	}
	var result []resourceapi.ResourceSlice
	for pool, current := range byPool {
		expected := current[0].Spec.Pool.ResourceSliceCount
		for _, slice := range current {
			if slice.Spec.Pool.ResourceSliceCount != expected {
				return nil, fmt.Errorf("resource pool %q has inconsistent slice counts", pool)
			}
		}
		if expected < 1 || int64(len(current)) != expected {
			return nil, fmt.Errorf("resource pool %q generation %d is incomplete: have %d of %d slices", pool, generations[pool], len(current), expected)
		}
		result = append(result, current...)
	}
	return result, nil
}

func stringAttr(device resourceapi.Device, name string) (string, bool) {
	value, ok := device.Attributes[resourceapi.QualifiedName(name)]
	if !ok || value.StringValue == nil {
		return "", false
	}
	return *value.StringValue, true
}
func intAttr(device resourceapi.Device, name string) (int64, bool) {
	value, ok := device.Attributes[resourceapi.QualifiedName(name)]
	if !ok || value.IntValue == nil {
		return 0, false
	}
	return *value.IntValue, true
}
func capacityBytes(device resourceapi.Device, name string) (int64, bool) {
	value, ok := device.Capacity[resourceapi.QualifiedName(name)]
	return value.Value.Value(), ok
}

func (r *ClusterReconciler) cleanupWorker(ctx context.Context, pod *corev1.Pod, restClient *slurm.Client, node slurm.Node) error {
	if node.Name != "" {
		if pod.Annotations[workerDrainAnnotation] != "true" {
			if err := restClient.DrainNode(ctx, pod.Name, "elastic worker removal"); err != nil {
				return err
			}
			return r.setWorkerAnnotation(ctx, pod, workerDrainAnnotation, "true")
		}
		if !nodeIdle(node) || !slices.Contains(node.State, "DRAIN") {
			return nil
		}
		if err := restClient.DeleteNode(ctx, pod.Name); err != nil {
			return err
		}
	}
	if controllerutil.RemoveFinalizer(pod, workerFinalizer) {
		if err := r.Update(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if pod.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) setWorkerAnnotation(ctx context.Context, pod *corev1.Pod, key, value string) error {
	copy := pod.DeepCopy()
	if copy.Annotations == nil {
		copy.Annotations = map[string]string{}
	}
	if value == "" {
		delete(copy.Annotations, key)
	} else {
		copy.Annotations[key] = value
	}
	if err := r.Update(ctx, copy); err != nil {
		return err
	}
	pod.Annotations = copy.Annotations
	return nil
}

func podShape(pod *corev1.Pod) (resourceplan.Shape, error) {
	var shape resourceplan.Shape
	if err := json.Unmarshal([]byte(pod.Annotations[workerShapeAnnotation]), &shape); err != nil || shape.CPUs < 1 || shape.MemoryBytes < 1 {
		return shape, fmt.Errorf("invalid worker shape annotation")
	}
	if shape.GRES == nil {
		shape.GRES = map[string]int64{}
	}
	return shape, nil
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
	})
}

func nodeIdle(node slurm.Node) bool {
	return slices.Contains(node.State, "IDLE") && node.Reservation == "" && node.AllocCPUs == 0 && node.AllocMemory == 0 && gresIdle(node.GRESUsed)
}

func gresIdle(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "(null)" || raw == "N/A" {
		return true
	}
	for item := range strings.SplitSeq(raw, ",") {
		item, _, _ = strings.Cut(strings.TrimSpace(item), "(")
		separator := strings.LastIndexAny(item, ":=")
		count, err := strconv.ParseInt(item[separator+1:], 10, 64)
		if separator < 0 || err != nil || count != 0 {
			return false
		}
	}
	return true
}

func validateWorkerPod(cluster *orchestrationv1alpha1.HeterogeneousCluster, pod *corev1.Pod) error {
	if !metav1.IsControlledBy(pod, cluster) {
		return fmt.Errorf("worker Pod %s/%s is not controlled by HeterogeneousCluster %s", pod.Namespace, pod.Name, cluster.Name)
	}
	return nil
}

func managedWorkerNode(cluster *orchestrationv1alpha1.HeterogeneousCluster, name string) bool {
	for _, pool := range cluster.Spec.WorkerPools {
		if strings.HasPrefix(name, cluster.Name+"-"+pool.Name+"-") {
			return true
		}
	}
	return false
}

func statuses(pools []orchestrationv1alpha1.WorkerPoolSpec, pods []corev1.Pod) []orchestrationv1alpha1.WorkerPoolStatus {
	result := poolStatus(pools)
	byPool := make(map[string]*orchestrationv1alpha1.WorkerPoolStatus, len(result))
	for i := range result {
		byPool[result[i].Name] = &result[i]
	}
	for i := range pods {
		status := byPool[pods[i].Labels[workerPoolLabel]]
		if status == nil {
			continue
		}
		if !pods[i].DeletionTimestamp.IsZero() || pods[i].Annotations[workerDrainAnnotation] == "true" {
			status.Draining++
		} else if podReady(&pods[i]) {
			status.Ready++
		} else {
			status.Pending++
		}
	}
	return result
}
