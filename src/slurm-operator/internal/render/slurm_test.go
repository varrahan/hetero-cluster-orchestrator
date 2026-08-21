package render

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

func TestSlurm(t *testing.T) {
	cluster := &orchestrationv1alpha1.HeterogeneousCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "research", Namespace: "slurm-system"},
		Spec: orchestrationv1alpha1.HeterogeneousClusterSpec{WorkerPools: []orchestrationv1alpha1.WorkerPoolSpec{
			{Name: "tpu", Partition: "z-tpu", MemoryUnit: "1Gi", Scaling: orchestrationv1alpha1.ScalingSpec{MaxWorkers: 4}, Profiles: []orchestrationv1alpha1.WorkerProfile{{Gres: "tpu:opentpu_m8"}}},
			{Name: "gpu", Partition: "a-gpu", MemoryUnit: "1Gi", Scaling: orchestrationv1alpha1.ScalingSpec{MaxWorkers: 2}, Profiles: []orchestrationv1alpha1.WorkerProfile{{Gres: "gpu:rtx_4050"}}},
		}},
	}

	files := Slurm(cluster)
	for _, expected := range []string{
		"SlurmctldHost=research-slurmctld-0(research-slurmctld-0.research-slurmctld.slurm-system.svc)\n",
		"SlurmctldTimeout=30\n",
		"SelectType=select/cons_tres\n",
		"TreeWidth=65533\n",
		"MaxNodeCount=6\n",
		"GresTypes=gpu,tpu\n",
		"AccountingStorageTRES=gres/gpu,gres/tpu\n",
		"TaskPlugin=task/cgroup,task/affinity\n",
		"NodeSet=pool_gpu Feature=pool_gpu\n",
		"PartitionName=a-gpu Nodes=pool_gpu State=UP Default=YES\n",
		"PartitionName=z-tpu Nodes=pool_tpu State=UP\n",
	} {
		if !strings.Contains(files.SlurmConf, expected) {
			t.Errorf("slurm.conf missing %q\n%s", expected, files.SlurmConf)
		}
	}
	if strings.Index(files.SlurmConf, "PartitionName=a-gpu") > strings.Index(files.SlurmConf, "PartitionName=z-tpu") {
		t.Fatal("partitions are not sorted")
	}
	if files.GRESConf != "" {
		t.Fatalf("controller gres.conf = %q, want empty", files.GRESConf)
	}
	if !strings.Contains(files.CgroupConf, "ConstrainDevices=yes") {
		t.Fatalf("cgroup.conf = %q", files.CgroupConf)
	}
}
