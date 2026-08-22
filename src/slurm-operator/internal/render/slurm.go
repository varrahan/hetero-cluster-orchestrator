package render

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

const (
	SlurmctldPort = 6817
	SlurmdPort    = 6818
	SlurmdbdPort  = 6819
	SlurmRESTPort = 6820
)

type SlurmFiles struct {
	SlurmConf  string
	GRESConf   string
	CgroupConf string
}

func Slurm(cluster *orchestrationv1alpha1.HeterogeneousCluster) SlurmFiles {
	name, namespace := cluster.Name, cluster.Namespace
	pools := slices.Clone(cluster.Spec.WorkerPools)
	slices.SortFunc(pools, func(a, b orchestrationv1alpha1.WorkerPoolSpec) int { return cmp.Compare(a.Partition, b.Partition) })

	gresTypes := make(map[string]struct{})
	gresProfiles := make(map[string]struct{})
	accountingTRES := make(map[string]struct{})
	maxNodes := int32(0)
	for _, pool := range pools {
		maxNodes += pool.Scaling.MaxWorkers
		if len(pool.Profiles) > 0 {
			maxNodes++
		}
		for _, profile := range pool.Profiles {
			kind, _, _ := strings.Cut(profile.Gres, ":")
			gresTypes[kind] = struct{}{}
			gresProfiles[profile.Gres] = struct{}{}
			accountingTRES["gres/"+kind] = struct{}{}
			accountingTRES["gres/"+profile.Gres] = struct{}{}
		}
	}

	var b strings.Builder
	line := func(key string, value any) { fmt.Fprintf(&b, "%s=%v\n", key, value) }
	line("ClusterName", name)
	for ordinal := range 2 {
		host := fmt.Sprintf("%s-slurmctld-%d", name, ordinal)
		line("SlurmctldHost", fmt.Sprintf("%s(%s.%s-slurmctld.%s.svc)", host, host, name, namespace))
	}
	line("SlurmctldPort", SlurmctldPort)
	line("SlurmctldTimeout", 30)
	line("SlurmdPort", SlurmdPort)
	line("SlurmUser", "slurm")
	line("StateSaveLocation", "/var/lib/slurmctld")
	line("SlurmdSpoolDir", "/var/lib/slurmd")
	line("AuthType", "auth/munge")
	line("AuthInfo", "/run/munge/munge.socket.2")
	line("AuthAltTypes", "auth/jwt")
	line("AuthAltParameters", "jwt_key=/run/secrets/slurm/jwt_hs256.key,disable_token_creation")
	line("CredType", "cred/munge")
	line("SelectType", "select/cons_tres")
	line("SelectTypeParameters", "CR_Core_Memory")
	line("TaskPlugin", "task/cgroup,task/affinity")
	line("TaskProlog", "/etc/slurm/task-prolog")
	line("ProctrackType", "proctrack/cgroup")
	line("SchedulerType", "sched/backfill")
	line("SlurmctldParameters", "enable_configless,reconfig_on_restart")
	line("AccountingStorageType", "accounting_storage/slurmdbd")
	line("AccountingStorageHost", fmt.Sprintf("%s-slurmdbd.%s.svc", name, namespace))
	line("AccountingStoragePort", SlurmdbdPort)
	line("JobAcctGatherType", "jobacct_gather/none")
	line("ReturnToService", 2)
	line("TreeWidth", 65533)
	line("MaxNodeCount", maxNodes)
	if values := slices.Sorted(maps.Keys(gresTypes)); len(values) > 0 {
		line("GresTypes", strings.Join(values, ","))
		line("AccountingStorageTRES", strings.Join(slices.Sorted(maps.Keys(accountingTRES)), ","))
	}
	for i, pool := range pools {
		nodeSet := "pool_" + strings.ReplaceAll(pool.Name, "-", "_")
		if len(pool.Profiles) > 0 {
			profiles := make([]string, 0, len(pool.Profiles))
			for _, profile := range pool.Profiles {
				profiles = append(profiles, fmt.Sprintf("%s:%d", profile.Gres, resourceapi.AllocationResultsMaxSize))
			}
			slices.Sort(profiles)
			line("NodeName", fmt.Sprintf("catalog_%s CPUs=%d Boards=1 SocketsPerBoard=1 CoresPerSocket=%d ThreadsPerCore=1 RealMemory=%d Gres=%s Feature=%s State=FUTURE", nodeSet, resourceapi.AllocationResultsMaxSize, resourceapi.AllocationResultsMaxSize, (1<<63-1)>>20, strings.Join(profiles, ","), nodeSet))
		}
		line("NodeSet", nodeSet+" Feature="+nodeSet)
		memoryUnit := resource.MustParse(pool.MemoryUnit)
		value := fmt.Sprintf("Nodes=%s State=UP DefMemPerNode=%d", nodeSet, memoryUnit.Value()>>20)
		if i == 0 {
			value += " Default=YES"
		}
		line("PartitionName", pool.Partition+" "+value)
	}

	var gres strings.Builder
	for _, profile := range slices.Sorted(maps.Keys(gresProfiles)) {
		name, profileType, _ := strings.Cut(profile, ":")
		fmt.Fprintf(&gres, "Name=%s Type=%s Count=0 Flags=CountOnly\n", name, profileType)
	}

	return SlurmFiles{
		SlurmConf:  b.String(),
		GRESConf:   gres.String(),
		CgroupConf: "CgroupPlugin=autodetect\nIgnoreSystemd=yes\nConstrainCores=yes\nConstrainRAMSpace=yes\nConstrainDevices=yes\n",
	}
}
