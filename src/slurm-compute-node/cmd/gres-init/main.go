package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/cpuset"
	"k8s.io/utils/ptr"
)

const (
	driverName      = "orchestration.gputpu.io"
	shapeAnnotation = "orchestration.gputpu.io/worker-shape"
)

type shape struct {
	CPUs              int64            `json:"CPUs"`
	MemoryBytes       int64            `json:"MemoryBytes"`
	SharedMemoryBytes int64            `json:"SharedMemoryBytes"`
	GRES              map[string]int64 `json:"GRES"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gres-init:", err)
		os.Exit(1)
	}
}

func run() error {
	for _, name := range []string{"POD_NAME", "POD_NAMESPACE", "POD_IP", "RESOURCE_CLAIM", "WORKER_POOL", "SLURM_CONF_SERVER"} {
		if os.Getenv(name) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx := context.Background()
	namespace, podName := os.Getenv("POD_NAMESPACE"), os.Getenv("POD_NAME")
	pod, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read Pod: %w", err)
	}
	claim, err := client.ResourceV1().ResourceClaims(namespace).Get(ctx, os.Getenv("RESOURCE_CLAIM"), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read ResourceClaim: %w", err)
	}
	if claim.Status.Allocation == nil {
		return errors.New("ResourceClaim is not allocated")
	}
	devices, err := allocatedDevices(ctx, client, pod, claim)
	if err != nil {
		return err
	}
	var wanted shape
	if err := json.Unmarshal([]byte(pod.Annotations[shapeAnnotation]), &wanted); err != nil || wanted.CPUs < 1 || wanted.MemoryBytes < 1 {
		return errors.New("invalid worker shape annotation")
	}
	if err := validate(devices, wanted, "/proc/self/status"); err != nil {
		return err
	}
	return renderAndCheck(devices, wanted)
}

func allocatedDevices(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, claim *resourceapi.ResourceClaim) ([]resourceapi.Device, error) {
	sliceList, err := client.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ResourceSlices: %w", err)
	}
	byPool := map[string][]resourceapi.ResourceSlice{}
	for _, slice := range sliceList.Items {
		if slice.Spec.Driver == driverName && slice.Spec.NodeName != nil && *slice.Spec.NodeName == pod.Spec.NodeName {
			byPool[slice.Spec.Pool.Name] = append(byPool[slice.Spec.Pool.Name], slice)
		}
	}
	result := make([]resourceapi.Device, 0, len(claim.Status.Allocation.Devices.Results))
	for _, allocation := range claim.Status.Allocation.Devices.Results {
		if allocation.Driver != driverName {
			return nil, fmt.Errorf("allocation %q uses driver %q", allocation.Device, allocation.Driver)
		}
		slicesForPool := byPool[allocation.Pool]
		if len(slicesForPool) == 0 {
			return nil, fmt.Errorf("allocation pool %q is not published by node %q", allocation.Pool, pod.Spec.NodeName)
		}
		generation := int64(-1)
		for _, slice := range slicesForPool {
			generation = max(generation, slice.Spec.Pool.Generation)
		}
		current := make([]resourceapi.ResourceSlice, 0, len(slicesForPool))
		for _, slice := range slicesForPool {
			if slice.Spec.Pool.Generation == generation {
				current = append(current, slice)
			}
		}
		if len(current) != int(current[0].Spec.Pool.ResourceSliceCount) {
			return nil, fmt.Errorf("allocation pool %q generation %d is incomplete", allocation.Pool, generation)
		}
		found := false
		for _, slice := range current {
			for _, device := range slice.Spec.Devices {
				if device.Name == allocation.Device {
					result = append(result, device)
					found = true
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("allocated device %q is absent from the current pool generation", allocation.Device)
		}
	}
	return result, nil
}

func validate(devices []resourceapi.Device, wanted shape, statusPath string) error {
	cpus := []int{}
	numa := int64(-1)
	actualGRES := map[string]int64{}
	for _, device := range devices {
		kind, ok := stringAttr(device, driverName+"/kind")
		cell, cellOK := intAttr(device, driverName+"/numaNode")
		if !ok || !cellOK {
			return fmt.Errorf("device %q lacks kind or NUMA identity", device.Name)
		}
		if numa == -1 {
			numa = cell
		} else if numa != cell {
			return errors.New("allocated devices span NUMA nodes")
		}
		switch kind {
		case "cpu":
			cpu, ok := intAttr(device, driverName+"/logicalCPU")
			if !ok {
				return fmt.Errorf("CPU device %q lacks logical CPU", device.Name)
			}
			cpus = append(cpus, int(cpu))
		case "memory":
		case "gpu":
			model, ok := stringAttr(device, driverName+"/model")
			if !ok {
				return fmt.Errorf("GPU %q lacks model", device.Name)
			}
			actualGRES["gpu:"+model]++
			if err := validateGPU(device); err != nil {
				return err
			}
		case "opentpu":
			profile, ok := stringAttr(device, driverName+"/profile")
			if !ok {
				return fmt.Errorf("OpenTPU %q lacks profile", device.Name)
			}
			actualGRES["tpu:"+profile]++
		default:
			return fmt.Errorf("unknown allocated device kind %q", kind)
		}
	}
	if int64(len(cpus)) != wanted.CPUs {
		return fmt.Errorf("allocated %d CPUs, expected %d", len(cpus), wanted.CPUs)
	}
	if !maps.Equal(actualGRES, wanted.GRES) {
		return fmt.Errorf("allocated GRES %v, expected %v", actualGRES, wanted.GRES)
	}
	allowedCPUs, allowedMems, err := allowedSets(statusPath)
	if err != nil {
		return err
	}
	expectedCPUs := cpuset.New(cpus...).String()
	if allowedCPUs != expectedCPUs {
		return fmt.Errorf("NRI CPU set %q does not match allocation %q", allowedCPUs, expectedCPUs)
	}
	if allowedMems != strconv.FormatInt(numa, 10) {
		return fmt.Errorf("NRI memory set %q does not match NUMA node %d", allowedMems, numa)
	}
	return nil
}

func validateGPU(device resourceapi.Device) error {
	uuid, uuidOK := stringAttr(device, driverName+"/uuid")
	model, modelOK := stringAttr(device, driverName+"/model")
	path, pathOK := stringAttr(device, driverName+"/path")
	if !uuidOK || !modelOK || !pathOK {
		return fmt.Errorf("GPU %q has incomplete identity", device.Name)
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("GPU %q path %q: %w", uuid, path, err)
	}
	output, err := exec.Command("nvidia-smi", "--query-gpu=uuid,name", "--format=csv,noheader").Output()
	if err != nil {
		return fmt.Errorf("inspect NVIDIA devices: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		parts := strings.SplitN(line, ",", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == uuid && normalizeGPU(parts[1]) == model {
			return nil
		}
	}
	return fmt.Errorf("GPU UUID/model %s/%s is not present", uuid, model)
}

func renderAndCheck(devices []resourceapi.Device, wanted shape) error {
	var gres []string
	var opentpu []string
	matrixSize := int64(0)
	cores := "0"
	if wanted.CPUs > 1 {
		cores = fmt.Sprintf("0-%d", wanted.CPUs-1)
	}
	for _, device := range devices {
		kind, _ := stringAttr(device, driverName+"/kind")
		switch kind {
		case "gpu":
			model, _ := stringAttr(device, driverName+"/model")
			path, _ := stringAttr(device, driverName+"/path")
			gres = append(gres, fmt.Sprintf("Name=gpu Type=%s File=%s Cores=%s", model, path, cores))
		case "opentpu":
			profile, _ := stringAttr(device, driverName+"/profile")
			matrix, ok := intAttr(device, driverName+"/matrixSize")
			if !ok || (matrixSize != 0 && matrixSize != matrix) {
				return fmt.Errorf("OpenTPU allocation has missing or mixed matrix sizes")
			}
			matrixSize = matrix
			gres = append(gres, fmt.Sprintf("Name=tpu Type=%s Count=1 Flags=CountOnly", profile))
			opentpu = append(opentpu, device.Name)
		}
	}
	slices.Sort(gres)
	slices.Sort(opentpu)
	gresSummary := make([]string, 0, len(wanted.GRES))
	for name, count := range wanted.GRES {
		kind, model, _ := strings.Cut(name, ":")
		gresSummary = append(gresSummary, fmt.Sprintf("%s:%s:%d", kind, model, count))
	}
	slices.Sort(gresSummary)
	realMemory := max((wanted.MemoryBytes-wanted.SharedMemoryBytes)>>20, 1)
	dynamic := fmt.Sprintf("CPUs=%d RealMemory=%d Sockets=1 CoresPerSocket=%d ThreadsPerCore=1 NodeAddr=%s Feature=pool_%s", wanted.CPUs, realMemory, wanted.CPUs, os.Getenv("POD_IP"), strings.ReplaceAll(os.Getenv("WORKER_POOL"), "-", "_"))
	if len(gresSummary) > 0 {
		dynamic += " Gres=" + strings.Join(gresSummary, ",")
	}
	gresConfig := strings.Join(gres, "\n") + "\n"
	if err := os.WriteFile("/etc/slurm/cgroup.conf", []byte("CgroupPlugin=autodetect\nIgnoreSystemd=yes\nConstrainCores=yes\nConstrainRAMSpace=yes\nConstrainDevices=yes\n"), 0644); err != nil {
		return err
	}
	env := "SLURMD_CONF=" + shellQuote(dynamic) + "\nexport SLURM_CONF=/var/lib/slurmd/conf-cache/slurm.conf\nexport OPENTPU_DEVICES=" + shellQuote(strings.Join(opentpu, ",")) + "\n"
	if matrixSize != 0 {
		env += "export OPENTPU_MATRIX_SIZE=" + strconv.FormatInt(matrixSize, 10) + "\n"
	}
	if err := os.WriteFile("/etc/slurm/worker.env", []byte(env), 0644); err != nil {
		return err
	}
	prolog := "#!/bin/sh\necho export OPENTPU_DEVICES=" + strings.Join(opentpu, ",") + "\n"
	if matrixSize != 0 {
		prolog += "echo export OPENTPU_MATRIX_SIZE=" + strconv.FormatInt(matrixSize, 10) + "\n"
	}
	if err := os.WriteFile("/etc/slurm/task-prolog", []byte(prolog), 0755); err != nil {
		return err
	}
	command := exec.Command("slurmd", "-G", "-Z", "-N", os.Getenv("POD_NAME"), "--conf-server", os.Getenv("SLURM_CONF_SERVER"), "--conf", dynamic)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("slurmd -G failed: %w: %s", err, output)
	}
	if err := os.WriteFile("/var/lib/slurmd/conf-cache/gres.conf", []byte(gresConfig), 0644); err != nil {
		return err
	}
	command = exec.Command("slurmd", "-G", "-Z", "-N", os.Getenv("POD_NAME"), "--conf", dynamic)
	command.Env = append(os.Environ(), "SLURM_CONF=/var/lib/slurmd/conf-cache/slurm.conf")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("slurmd local GRES validation failed: %w: %s", err, output)
	}
	return nil
}

func allowedSets(path string) (string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	var cpus, mems string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}
		switch key {
		case "Cpus_allowed_list":
			cpus = strings.TrimSpace(value)
		case "Mems_allowed_list":
			mems = strings.TrimSpace(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", err
	}
	if cpus == "" || mems == "" {
		return "", "", errors.New("process cgroup CPU or memory set is missing")
	}
	return cpus, mems, nil
}

func stringAttr(device resourceapi.Device, name string) (string, bool) {
	value := device.Attributes[resourceapi.QualifiedName(name)].StringValue
	return ptr.Deref(value, ""), value != nil
}
func intAttr(device resourceapi.Device, name string) (int64, bool) {
	value := device.Attributes[resourceapi.QualifiedName(name)].IntValue
	return ptr.Deref(value, 0), value != nil
}
func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
func normalizeGPU(value string) string {
	value = strings.ToLower(value)
	for _, word := range []string{"nvidia", "geforce", "laptop", "gpu"} {
		value = strings.ReplaceAll(value, word, " ")
	}
	fields := strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') })
	return strings.Join(fields, "_")
}
