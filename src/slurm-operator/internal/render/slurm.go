package render

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
)

const (
	SlurmctldPort = 6817
	SlurmdPort    = 6818
	SlurmdbdPort  = 6819
	SlurmRESTPort = 6820
)

type SlurmFiles struct {
	SlurmConf string
	GRESConf  string
}

func Slurm(cluster *orchestrationv1alpha1.HeterogeneousCluster) SlurmFiles {
	name, namespace := cluster.Name, cluster.Namespace
	pools := slices.Clone(cluster.Spec.WorkerPools)
	sort.Slice(pools, func(i, j int) bool {
		return pools[i].Partition < pools[j].Partition
	})

	gresTypes := make(map[string]struct{})
	maxNodes := int32(0)
	for _, pool := range pools {
		maxNodes += pool.Scaling.MaxWorkers
		for _, profile := range pool.Profiles {
			kind, _, _ := strings.Cut(profile.Gres, ":")
			gresTypes[kind] = struct{}{}
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
		tres := make([]string, len(values))
		for i, value := range values {
			tres[i] = "gres/" + value
		}
		line("AccountingStorageTRES", strings.Join(tres, ","))
	}
	for i, pool := range pools {
		value := fmt.Sprintf("Nodes=ALL State=UP")
		if i == 0 {
			value += " Default=YES"
		}
		line("PartitionName", pool.Partition+" "+value)
	}

	return SlurmFiles{SlurmConf: b.String(), GRESConf: ""}
}
