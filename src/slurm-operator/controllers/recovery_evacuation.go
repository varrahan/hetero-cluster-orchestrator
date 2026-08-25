package controllers

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

func (r *RecoveryReconciler) reconcileEvacuation(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState) (ctrl.Result, error) {
	changed, err := r.discoverWorkers(ctx, node, state)
	if err != nil {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	if changed {
		object, err = r.saveState(ctx, node, object, state)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	snapshots, err := r.slurmSnapshots(ctx, state)
	if err != nil {
		if r.Recorder != nil {
			r.Recorder.Event(object, corev1.EventTypeWarning, "SlurmRESTUnavailable", "node remains isolated; recovery requires authoritative Slurm state")
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	addedJobs := addAffectedJobs(state, snapshots)
	if addedJobs {
		object, err = r.saveState(ctx, node, object, state)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := drainRecoveryWorkers(ctx, state, snapshots); err != nil {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	if addedJobs && state.Phase != phaseIsolated && state.Phase != phaseDraining {
		if _, err := r.setPhase(ctx, node, object, state, phaseDraining); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}

	switch state.Phase {
	case phaseIsolated:
		if _, err := r.setPhase(ctx, node, object, state, phaseDraining); err != nil {
			return ctrl.Result{}, err
		}
	case phaseDraining:
		complete, err := r.signalJobs(ctx, node, object, state, snapshots)
		if err != nil {
			return ctrl.Result{RequeueAfter: recoveryRequeue}, err
		}
		if complete {
			next := phaseCheckpointing
			if len(state.Jobs) == 0 {
				next = phaseRequeued
			}
			if _, err := r.setPhase(ctx, node, object, state, next); err != nil {
				return ctrl.Result{}, err
			}
		}
	case phaseCheckpointing:
		complete, err := r.checkpointAndRequeue(ctx, node, object, state, snapshots)
		if err != nil {
			return ctrl.Result{RequeueAfter: recoveryRequeue}, err
		}
		if complete {
			if _, err := r.setPhase(ctx, node, object, state, phaseRequeued); err != nil {
				return ctrl.Result{}, err
			}
		}
	case phaseRequeued:
		complete, err := r.cleanupEvacuatedWorkers(ctx, state, snapshots)
		if err != nil {
			return ctrl.Result{RequeueAfter: recoveryRequeue}, err
		}
		if complete {
			now := metav1.NewTime(r.now())
			state.RebootRequestedAt = &now
			if _, err := r.setPhase(ctx, node, object, state, phaseRebootRequested); err != nil {
				return ctrl.Result{}, err
			}
			if r.Recorder != nil {
				r.Recorder.Eventf(object, corev1.EventTypeNormal, "RebootRequested", "watchdog reboot requested for incident %s", state.Incident)
			}
		}
	}
	return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
}

func (r *RecoveryReconciler) discoverWorkers(ctx context.Context, node *corev1.Node, state *recoveryState) (bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.MatchingFields{workerNodeIndex: node.Name}); err != nil {
		return false, fmt.Errorf("list workers on degraded Node: %w", err)
	}
	changed := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels["app.kubernetes.io/component"] != workerComponent || pod.Labels["app.kubernetes.io/managed-by"] != "slurm-operator" {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.Kind != "HeterogeneousCluster" || owner.APIVersion != orchestrationv1alpha1.GroupVersion.String() {
			return false, fmt.Errorf("worker Pod %s/%s has no HeterogeneousCluster controller", pod.Namespace, pod.Name)
		}
		var cluster orchestrationv1alpha1.HeterogeneousCluster
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}, &cluster); err != nil {
			return false, fmt.Errorf("read worker cluster %s/%s: %w", pod.Namespace, owner.Name, err)
		}
		if cluster.UID != owner.UID {
			return false, fmt.Errorf("worker Pod %s/%s refers to a replaced cluster", pod.Namespace, pod.Name)
		}
		if !slices.ContainsFunc(state.Clusters, func(item recoveryCluster) bool {
			return item.Namespace == cluster.Namespace && item.Name == cluster.Name
		}) {
			state.Clusters = append(state.Clusters, recoveryCluster{Namespace: cluster.Namespace, Name: cluster.Name, UID: cluster.UID})
			changed = true
		}
		worker := recoveryWorker{ClusterNamespace: cluster.Namespace, ClusterName: cluster.Name, Namespace: pod.Namespace, Name: pod.Name, Claim: pod.Annotations[workerClaimAnnotation]}
		if !slices.ContainsFunc(state.Workers, func(item recoveryWorker) bool { return item.Namespace == worker.Namespace && item.Name == worker.Name }) {
			state.Workers = append(state.Workers, worker)
			changed = true
		}
		if pod.Annotations[workerRecoveryAnnotation] != state.Incident {
			copy := pod.DeepCopy()
			if copy.Annotations == nil {
				copy.Annotations = map[string]string{}
			}
			copy.Annotations[workerRecoveryAnnotation] = state.Incident
			if err := r.Patch(ctx, copy, client.MergeFrom(pod)); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
		}
	}
	slices.SortFunc(state.Clusters, func(a, b recoveryCluster) int {
		return strings.Compare(clusterKey(a.Namespace, a.Name), clusterKey(b.Namespace, b.Name))
	})
	slices.SortFunc(state.Workers, func(a, b recoveryWorker) int {
		return strings.Compare(clusterKey(a.Namespace, a.Name), clusterKey(b.Namespace, b.Name))
	})
	return changed, nil
}

func (r *RecoveryReconciler) slurmSnapshots(ctx context.Context, state *recoveryState) (map[string]clusterSnapshot, error) {
	result := make(map[string]clusterSnapshot, len(state.Clusters))
	for _, reference := range state.Clusters {
		var cluster orchestrationv1alpha1.HeterogeneousCluster
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: reference.Namespace, Name: reference.Name}, &cluster); err != nil {
			return nil, err
		}
		if cluster.UID != reference.UID {
			return nil, fmt.Errorf("cluster %s/%s was replaced during recovery", reference.Namespace, reference.Name)
		}
		key, err := r.jwtKey(ctx, &cluster)
		if err != nil {
			return nil, err
		}
		restClient, err := slurm.NewClient(r.restURL(&cluster), key, r.HTTPClient)
		if err != nil {
			return nil, err
		}
		nodes, err := restClient.Nodes(ctx)
		if err != nil {
			return nil, err
		}
		jobs, err := restClient.Jobs(ctx)
		if err != nil {
			return nil, err
		}
		byName := make(map[string]slurm.Node, len(nodes))
		for _, node := range nodes {
			byName[node.Name] = node
		}
		result[clusterKey(reference.Namespace, reference.Name)] = clusterSnapshot{cluster: &cluster, client: restClient, nodes: byName, jobs: jobs}
	}
	return result, nil
}

func addAffectedJobs(state *recoveryState, snapshots map[string]clusterSnapshot) bool {
	changed := false
	for key, snapshot := range snapshots {
		workers := workerNames(state, key)
		for _, job := range snapshot.jobs {
			if job.Terminal() || !job.UsesAnyNode(workers) {
				continue
			}
			root := job.RootID()
			if slices.ContainsFunc(state.Jobs, func(item recoveryJob) bool {
				return clusterKey(item.ClusterNamespace, item.ClusterName) == key && item.ID == root
			}) {
				continue
			}
			namespace, name, _ := strings.Cut(key, "/")
			state.Jobs = append(state.Jobs, recoveryJob{ClusterNamespace: namespace, ClusterName: name, ID: root})
			changed = true
		}
	}
	slices.SortFunc(state.Jobs, func(a, b recoveryJob) int {
		if order := strings.Compare(clusterKey(a.ClusterNamespace, a.ClusterName), clusterKey(b.ClusterNamespace, b.ClusterName)); order != 0 {
			return order
		}
		return int(a.ID) - int(b.ID)
	})
	return changed
}

func drainRecoveryWorkers(ctx context.Context, state *recoveryState, snapshots map[string]clusterSnapshot) error {
	for _, worker := range state.Workers {
		snapshot := snapshots[clusterKey(worker.ClusterNamespace, worker.ClusterName)]
		node, exists := snapshot.nodes[worker.Name]
		if !exists || slices.Contains(node.State, "DRAIN") {
			continue
		}
		if err := snapshot.client.DrainNode(ctx, worker.Name, "hardware incident "+state.Incident); err != nil {
			return err
		}
	}
	return nil
}

func (r *RecoveryReconciler) signalJobs(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState, snapshots map[string]clusterSnapshot) (bool, error) {
	for i := range state.Jobs {
		job := &state.Jobs[i]
		snapshot := snapshots[clusterKey(job.ClusterNamespace, job.ClusterName)]
		if jobEvacuated(*job, state, snapshot.jobs) {
			job.Evacuated = true
			if _, err := r.saveState(ctx, node, object, state); err != nil {
				return false, err
			}
			continue
		}
		if job.SignalSent {
			continue
		}
		if job.SignalRequestedAt == nil {
			now := metav1.NewTime(r.now())
			job.SignalRequestedAt = &now
			var err error
			object, err = r.saveState(ctx, node, object, state)
			if err != nil {
				return false, err
			}
		}
		if err := snapshot.client.SignalJob(ctx, job.ID, "USR1"); err != nil {
			return false, err
		}
		job.SignalSent = true
		var err error
		object, err = r.saveState(ctx, node, object, state)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *RecoveryReconciler) checkpointAndRequeue(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState, snapshots map[string]clusterSnapshot) (bool, error) {
	allEvacuated := true
	for i := range state.Jobs {
		job := &state.Jobs[i]
		snapshot := snapshots[clusterKey(job.ClusterNamespace, job.ClusterName)]
		if jobEvacuated(*job, state, snapshot.jobs) {
			if !job.Evacuated {
				job.Evacuated = true
				var err error
				object, err = r.saveState(ctx, node, object, state)
				if err != nil {
					return false, err
				}
			}
			continue
		}
		allEvacuated = false
		if job.SignalRequestedAt == nil || !job.SignalSent {
			return false, nil
		}
		deadline := job.SignalRequestedAt.Time.Add(checkpointGrace)
		if !job.CheckpointSeen && snapshot.cluster.Spec.Checkpointing != nil {
			seen, err := r.checkpointCommittedSince(ctx, snapshot.cluster, uint64(job.ID), job.SignalRequestedAt.Time)
			if err == nil && seen {
				job.CheckpointSeen = true
				var saveErr error
				object, saveErr = r.saveState(ctx, node, object, state)
				if saveErr != nil {
					return false, saveErr
				}
			}
		}
		if !job.CheckpointSeen && r.now().Before(deadline) {
			continue
		}
		if job.RequeueSent {
			continue
		}
		if job.RequeueRequestedAt == nil {
			now := metav1.NewTime(r.now())
			job.RequeueRequestedAt = &now
			var err error
			object, err = r.saveState(ctx, node, object, state)
			if err != nil {
				return false, err
			}
		}
		if err := snapshot.client.RequeueJob(ctx, job.ID); err != nil {
			return false, err
		}
		job.RequeueSent = true
		var err error
		object, err = r.saveState(ctx, node, object, state)
		if err != nil {
			return false, err
		}
	}
	return allEvacuated, nil
}

func jobEvacuated(job recoveryJob, state *recoveryState, jobs []slurm.Job) bool {
	wanted := workerNames(state, clusterKey(job.ClusterNamespace, job.ClusterName))
	active := false
	for _, current := range jobs {
		if current.RootID() != job.ID {
			continue
		}
		if current.Pending() || current.Terminal() {
			continue
		}
		active = true
		if current.UsesAnyNode(wanted) {
			return false
		}
	}
	return job.RequeueSent || !active
}

func (r *RecoveryReconciler) cleanupEvacuatedWorkers(ctx context.Context, state *recoveryState, snapshots map[string]clusterSnapshot) (bool, error) {
	for _, job := range state.Jobs {
		if !jobEvacuated(job, state, snapshots[clusterKey(job.ClusterNamespace, job.ClusterName)].jobs) {
			return false, nil
		}
	}
	for _, worker := range state.Workers {
		snapshot := snapshots[clusterKey(worker.ClusterNamespace, worker.ClusterName)]
		if _, exists := snapshot.nodes[worker.Name]; exists {
			if err := snapshot.client.DeleteNode(ctx, worker.Name); err != nil {
				return false, err
			}
		}
	}
	complete := true
	for _, worker := range state.Workers {
		var pod corev1.Pod
		err := r.Get(ctx, types.NamespacedName{Namespace: worker.Namespace, Name: worker.Name}, &pod)
		if err == nil {
			complete = false
			if controllerutil.RemoveFinalizer(&pod, workerFinalizer) {
				if err := r.Update(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
			}
			if pod.DeletionTimestamp.IsZero() {
				if err := r.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
			}
			continue
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
		if worker.Claim != "" {
			claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: worker.Namespace, Name: worker.Claim}}
			if err := r.Delete(ctx, claim); err == nil {
				complete = false
			} else if !apierrors.IsNotFound(err) {
				return false, err
			}
		}
	}
	return complete, nil
}

func workerNames(state *recoveryState, cluster string) map[string]struct{} {
	result := map[string]struct{}{}
	for _, worker := range state.Workers {
		if clusterKey(worker.ClusterNamespace, worker.ClusterName) == cluster {
			result[worker.Name] = struct{}{}
		}
	}
	return result
}

func clusterKey(namespace, name string) string { return namespace + "/" + name }
