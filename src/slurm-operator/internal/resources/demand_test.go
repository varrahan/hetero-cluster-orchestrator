package resources

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

func TestDemandsExactShapesAndRejections(t *testing.T) {
	pools := []orchestrationv1alpha1.WorkerPoolSpec{{Name: "accelerated", Partition: "compute", MemoryUnit: "1Gi", Scaling: orchestrationv1alpha1.ScalingSpec{MaxWorkers: 4, IdleTimeout: metav1.Duration{Duration: time.Minute}}, Profiles: []orchestrationv1alpha1.WorkerProfile{{Name: "gpu", Gres: "gpu:rtx_4050", DeviceClassName: "nvidia.orchestration.gputpu.io"}, {Name: "tpu", Gres: "tpu:opentpu_m8", DeviceClassName: "opentpu.orchestration.gputpu.io"}}}}
	jobs := []slurm.PendingJob{
		{ID: 1, Partition: "compute", Reason: "Resources", CPUs: 8, NodeCount: 2, MemoryPerCPU: 512, TRESPerNode: "gres/gpu:rtx_4050=1"},
		{ID: 2, Partition: "compute", Reason: "Resources", CPUs: 1, NodeCount: 1, TRESPerNode: "gres/tpu:opentpu_m8=1"},
		{ID: 3, Partition: "compute", Reason: "Priority", CPUs: 64},
		{ID: 4, Partition: "compute", Reason: "Resources", TRESPerNode: "gres/gpu:unknown=1"},
	}
	demands, rejected := Demands(jobs, pools, map[string]Footprint{"tpu:opentpu_m8": {CPUs: 2, MemoryBytes: 1 << 30, SharedMemory: 512 << 20}})
	if len(demands) != 2 || len(rejected) != 1 {
		t.Fatalf("demands=%#v rejected=%v", demands, rejected)
	}
	if demands[0].Count != 2 || demands[0].Shape.CPUs != 4 || demands[0].Shape.MemoryBytes != 2<<30 || demands[0].Shape.GRES["gpu:rtx_4050"] != 1 {
		t.Fatalf("GPU shape = %#v", demands[0])
	}
	if demands[1].Shape.CPUs != 2 || demands[1].Shape.MemoryBytes != 1536<<20 {
		t.Fatalf("OpenTPU footprint = %#v", demands[1])
	}
}
