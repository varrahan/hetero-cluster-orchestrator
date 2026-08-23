package resources

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

type Footprint struct {
	CPUs         int64
	MemoryBytes  int64
	SharedMemory int64
}

type Shape struct {
	CPUs              int64
	MemoryBytes       int64
	SharedMemoryBytes int64
	GRES              map[string]int64
}

type Demand struct {
	JobID    uint32
	GroupID  uint32
	PoolName string
	Count    int64
	Shape    Shape
}

func Demands(jobs []slurm.PendingJob, pools []orchestrationv1alpha1.WorkerPoolSpec, footprints map[string]Footprint) ([]Demand, []error) {
	byPartition := make(map[string]orchestrationv1alpha1.WorkerPoolSpec, len(pools))
	for _, pool := range pools {
		byPartition[pool.Partition] = pool
	}
	slices.SortFunc(jobs, func(a, b slurm.PendingJob) int {
		if order := cmp.Compare(b.Priority, a.Priority); order != 0 {
			return order
		}
		if order := cmp.Compare(a.EligibleTime, b.EligibleTime); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	var output []Demand
	var rejected []error
	for _, job := range jobs {
		if !job.Requeued && job.Reason != "Resources" && job.Reason != "ReqNodeNotAvail" && job.Reason != "PartitionConfig" {
			continue
		}
		if job.RequiredNodes != "" {
			rejected = append(rejected, fmt.Errorf("job %d: explicit required nodes are unsupported", job.ID))
			continue
		}
		pool, ok := byPartition[job.Partition]
		if !ok {
			rejected = append(rejected, fmt.Errorf("job %d: partition %q has no worker pool", job.ID, job.Partition))
			continue
		}
		if job.Features != "" && job.Features != "pool_"+normalize(pool.Name) {
			rejected = append(rejected, fmt.Errorf("job %d: unsupported feature constraint %q", job.ID, job.Features))
			continue
		}
		shape, err := shapeFor(job, pool, footprints)
		if err != nil {
			rejected = append(rejected, fmt.Errorf("job %d: %w", job.ID, err))
			continue
		}
		count := max(job.NodeCount, 1)
		group := job.HetJobID
		if group == 0 {
			group = job.ID
		}
		output = append(output, Demand{JobID: job.ID, GroupID: group, PoolName: pool.Name, Count: count, Shape: shape})
	}
	return output, rejected
}

func shapeFor(job slurm.PendingJob, pool orchestrationv1alpha1.WorkerPoolSpec, footprints map[string]Footprint) (Shape, error) {
	nodes := max(job.NodeCount, 1)
	cpus := max(divRoundUp(job.CPUs, nodes), job.CPUsPerTask, 1)
	if job.TasksPerNode > 0 && job.CPUsPerTask > 0 {
		cpus = max(cpus, job.TasksPerNode*job.CPUsPerTask)
	}
	memory := job.MemoryPerNode << 20
	if memory == 0 && job.MemoryPerCPU > 0 {
		memory = job.MemoryPerCPU * cpus << 20
	}

	gres := map[string]int64{}
	sharedMemory := int64(0)
	tresSource := job.TRESPerNode
	multiplier := int64(1)
	if tresSource == "" && job.TRESPerTask != "" {
		tresSource, multiplier = job.TRESPerTask, max(job.TasksPerNode, divRoundUp(job.Tasks, nodes), 1)
	}
	if tresSource == "" && job.TRESPerSocket != "" {
		tresSource = job.TRESPerSocket
	}
	if tresSource == "" && job.TRESPerJob != "" {
		tresSource, multiplier = job.TRESPerJob, -nodes
	}
	if tresSource == "" {
		tresSource, multiplier = job.TRESRequested, -nodes
	}
	values, err := parseTRES(tresSource)
	if err != nil {
		return Shape{}, err
	}
	for key, value := range values {
		if multiplier > 0 {
			value *= multiplier
		} else {
			value = divRoundUp(value, -multiplier)
		}
		switch key {
		case "cpu":
			cpus = max(cpus, value)
		case "mem":
			memory = max(memory, value)
		default:
			if strings.HasPrefix(key, "gres/") {
				gres[strings.TrimPrefix(key, "gres/")] += value
			}
		}
	}

	profiles := map[string]orchestrationv1alpha1.WorkerProfile{}
	for _, profile := range pool.Profiles {
		profiles[profile.Gres] = profile
	}
	for name, count := range gres {
		if count < 1 {
			return Shape{}, fmt.Errorf("GRES %q has a non-positive count", name)
		}
		if _, ok := profiles[name]; !ok {
			return Shape{}, fmt.Errorf("GRES %q has no exact profile mapping", name)
		}
		if footprint, ok := footprints[name]; ok {
			cpus = max(cpus, footprint.CPUs*count)
			memory = max(memory, (footprint.MemoryBytes+footprint.SharedMemory)*count)
			sharedMemory += footprint.SharedMemory * count
		}
	}
	if constraints, err := parseTRES(job.CPUsPerTRES); err != nil {
		return Shape{}, fmt.Errorf("CPUsPerTRES: %w", err)
	} else {
		for name, count := range gres {
			cpus = max(cpus, constraints["gres/"+name]*count)
		}
	}
	if constraints, err := parseTRES(job.MemoryPerTRES); err != nil {
		return Shape{}, fmt.Errorf("MemoryPerTRES: %w", err)
	} else {
		for name, count := range gres {
			memory = max(memory, constraints["gres/"+name]*count)
		}
	}
	unit, err := resource.ParseQuantity(pool.MemoryUnit)
	if err != nil || unit.Value() < 1 {
		return Shape{}, fmt.Errorf("invalid pool memory unit %q", pool.MemoryUnit)
	}
	memory = max(memory, unit.Value())
	return Shape{CPUs: cpus, MemoryBytes: memory, SharedMemoryBytes: sharedMemory, GRES: gres}, nil
}

func parseTRES(raw string) (map[string]int64, error) {
	result := map[string]int64{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, valueRaw, ok := strings.Cut(item, "=")
		if !ok && strings.HasPrefix(item, "gres/") {
			if last := strings.LastIndex(item, ":"); last > len("gres/") {
				key, valueRaw, ok = item[:last], item[last+1:], true
			}
		}
		if !ok {
			return nil, fmt.Errorf("invalid TRES %q", item)
		}
		key = strings.TrimSpace(key)
		if strings.HasPrefix(key, "gres:") {
			key = "gres/" + strings.TrimPrefix(key, "gres:")
		}
		if key != "cpu" && key != "mem" && !strings.HasPrefix(key, "gres/") {
			continue
		}
		value, err := parseTRESValue(key, strings.TrimSpace(valueRaw))
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %q", key, valueRaw)
		}
		result[key] += value
	}
	return result, nil
}

func parseTRESValue(key, raw string) (int64, error) {
	if key != "mem" {
		return strconv.ParseInt(raw, 10, 64)
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
		raw += "Mi"
	} else {
		if len(raw) < 2 {
			return 0, err
		}
		suffix := strings.ToUpper(raw[len(raw)-1:])
		if !strings.Contains("KMGT", suffix) {
			return 0, fmt.Errorf("unknown memory suffix")
		}
		raw = raw[:len(raw)-1] + suffix + "i"
	}
	quantity, err := resource.ParseQuantity(raw)
	if err != nil {
		return 0, err
	}
	return quantity.Value(), nil
}

func normalize(value string) string {
	value = strings.ToLower(value)
	return strings.Join(strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') }), "_")
}
func divRoundUp(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return (value + divisor - 1) / divisor
}
