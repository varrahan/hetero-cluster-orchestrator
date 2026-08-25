package controllers

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

var (
	clusterCondition  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_cluster_condition", Help: "Current HeterogeneousCluster condition: 1 true, 0 false, -1 unknown."}, []string{"namespace", "cluster", "condition"})
	workerCount       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_worker_pool_workers", Help: "Current elastic worker count by state."}, []string{"namespace", "cluster", "pool", "state"})
	pendingJobAge     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_pending_job_age_seconds", Help: "Seconds since a pending Slurm job became eligible."}, []string{"namespace", "cluster", "pool", "job"})
	checkpointAge     = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_checkpoint_age_seconds", Help: "Age of the newest committed checkpoint."}, []string{"namespace", "cluster"})
	recoveryPhaseInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_recovery_phase", Help: "Current durable hardware recovery phase."}, []string{"node", "incident", "phase"})
	recoveryDuration  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gputpu_recovery_duration_seconds", Help: "Elapsed time for the current hardware recovery incident."}, []string{"node", "incident"})
)

func init() {
	controllermetrics.Registry.MustRegister(clusterCondition, workerCount, pendingJobAge, checkpointAge, recoveryPhaseInfo, recoveryDuration)
}

func observeCluster(cluster *orchestrationv1alpha1.HeterogeneousCluster) {
	labels := prometheus.Labels{"namespace": cluster.Namespace, "cluster": cluster.Name}
	clusterCondition.DeletePartialMatch(labels)
	for _, name := range []string{orchestrationv1alpha1.ConditionControlPlaneReady, orchestrationv1alpha1.ConditionAccountingReady, orchestrationv1alpha1.ConditionWorkersReady, orchestrationv1alpha1.ConditionCheckpointReady} {
		value := -1.0
		if condition := meta.FindStatusCondition(cluster.Status.Conditions, name); condition != nil {
			if condition.Status == metav1.ConditionTrue {
				value = 1
			} else if condition.Status == metav1.ConditionFalse {
				value = 0
			}
		}
		clusterCondition.WithLabelValues(cluster.Namespace, cluster.Name, name).Set(value)
	}
	workerCount.DeletePartialMatch(labels)
	for _, pool := range cluster.Status.WorkerPools {
		workerCount.WithLabelValues(cluster.Namespace, cluster.Name, pool.Name, "ready").Set(float64(pool.Ready))
		workerCount.WithLabelValues(cluster.Namespace, cluster.Name, pool.Name, "pending").Set(float64(pool.Pending))
		workerCount.WithLabelValues(cluster.Namespace, cluster.Name, pool.Name, "draining").Set(float64(pool.Draining))
	}
	checkpointAge.DeletePartialMatch(labels)
	if cluster.Status.NewestCommittedCheckpoint != nil {
		checkpointAge.WithLabelValues(cluster.Namespace, cluster.Name).Set(max(0, time.Since(cluster.Status.NewestCommittedCheckpoint.Time).Seconds()))
	}
}

func observePendingJobs(cluster *orchestrationv1alpha1.HeterogeneousCluster, jobs []slurm.PendingJob) {
	pendingJobAge.DeletePartialMatch(prometheus.Labels{"namespace": cluster.Namespace, "cluster": cluster.Name})
	pools := make(map[string]string, len(cluster.Spec.WorkerPools))
	for _, pool := range cluster.Spec.WorkerPools {
		pools[pool.Partition] = pool.Name
	}
	for _, job := range jobs {
		if pool := pools[job.Partition]; pool != "" && job.EligibleTime > 0 {
			pendingJobAge.WithLabelValues(cluster.Namespace, cluster.Name, pool, strconv.FormatUint(uint64(job.ID), 10)).Set(max(0, time.Since(time.Unix(job.EligibleTime, 0)).Seconds()))
		}
	}
}

func forgetCluster(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "cluster": name}
	clusterCondition.DeletePartialMatch(labels)
	workerCount.DeletePartialMatch(labels)
	pendingJobAge.DeletePartialMatch(labels)
	checkpointAge.DeletePartialMatch(labels)
}

func observeRecovery(state *recoveryState) {
	labels := prometheus.Labels{"node": state.NodeName, "incident": state.Incident}
	recoveryPhaseInfo.DeletePartialMatch(labels)
	recoveryPhaseInfo.WithLabelValues(state.NodeName, state.Incident, string(state.Phase)).Set(1)
	recoveryDuration.WithLabelValues(state.NodeName, state.Incident).Set(max(0, time.Since(state.StartedAt.Time).Seconds()))
}

func forgetRecovery(state *recoveryState) {
	labels := prometheus.Labels{"node": state.NodeName, "incident": state.Incident}
	recoveryPhaseInfo.DeletePartialMatch(labels)
	recoveryDuration.DeletePartialMatch(labels)
}
