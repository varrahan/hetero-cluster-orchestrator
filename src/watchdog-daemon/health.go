package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	healthHealthy   = "Healthy"
	healthUnhealthy = "Unhealthy"
	healthUnknown   = "Unknown"
)

var errUnsupported = errors.New("hardware check unsupported")

type inventorySummary struct {
	Version int             `json:"version"`
	BootID  string          `json:"bootID"`
	Cells   []inventoryCell `json:"cells"`
}

type inventoryCell struct {
	NUMA            int                `json:"numaNode"`
	CPUs            int64              `json:"cpus"`
	MemoryUnits     int64              `json:"memoryUnits"`
	MemoryUnitBytes int64              `json:"memoryUnitBytes"`
	GPUs            []inventoryGPU     `json:"gpus,omitempty"`
	OpenTPU         []inventoryOpenTPU `json:"openTPU,omitempty"`
}

type inventoryGPU struct {
	UUID  string `json:"uuid"`
	Model string `json:"model"`
	PCI   string `json:"pci"`
}

type inventoryOpenTPU struct {
	Profile      string `json:"profile"`
	Count        int64  `json:"count"`
	MatrixSize   int64  `json:"matrixSize"`
	CPUCores     int64  `json:"cpuCores"`
	MemoryBytes  int64  `json:"memoryBytes"`
	SharedMemory int64  `json:"sharedMemoryBytes"`
}

type healthResult struct {
	Status        string `json:"status"`
	Reason        string `json:"reason"`
	CheckedAt     string `json:"checkedAt"`
	BootID        string `json:"bootID,omitempty"`
	Inventory     string `json:"inventory,omitempty"`
	InventoryHash string `json:"inventoryHash,omitempty"`
}

type healthChecker struct {
	client      kubernetes.Interface
	nodeName    string
	events      *kernelEventState
	memoryBytes int
	sysRoot     string
	mu          sync.RWMutex
	failures    int
	current     healthResult
}

func newHealthChecker(client kubernetes.Interface, nodeName string, events *kernelEventState) (*healthChecker, error) {
	bytes := 64 << 20
	if raw := os.Getenv("WATCHDOG_MEMORY_CHECK_BYTES"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1<<20 || value > 1<<30 {
			return nil, errors.New("WATCHDOG_MEMORY_CHECK_BYTES must be between 1MiB and 1GiB")
		}
		bytes = int(value)
	}
	return &healthChecker{client: client, nodeName: nodeName, events: events, memoryBytes: bytes, sysRoot: "/sys", current: healthResult{Status: healthUnknown, Reason: "health has not run"}}, nil
}

func (h *healthChecker) run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		h.check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *healthChecker) result() healthResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current
}

func (h *healthChecker) check(ctx context.Context) {
	result, hard, err := h.checkOnce(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	result.CheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err == nil {
		h.failures = 0
		result.Status, result.Reason = healthHealthy, "all hardware checks passed"
	} else if errors.Is(err, errUnsupported) {
		result.Status, result.Reason = healthUnknown, err.Error()
	} else if hard {
		h.failures = 3
		result.Status, result.Reason = healthUnhealthy, err.Error()
	} else {
		h.failures++
		result.Reason = err.Error()
		if h.failures >= 3 {
			result.Status = healthUnhealthy
		} else {
			result.Status = healthUnknown
		}
	}
	h.current = result
}

func (h *healthChecker) checkOnce(ctx context.Context) (healthResult, bool, error) {
	node, err := h.client.CoreV1().Nodes().Get(ctx, h.nodeName, metav1.GetOptions{})
	if err != nil {
		return healthResult{}, false, fmt.Errorf("read own Node: %w", err)
	}
	result := healthResult{BootID: node.Status.NodeInfo.BootID, Inventory: node.Annotations[inventoryAnnotation], InventoryHash: node.Annotations[inventoryHashAnnotation]}
	if event := h.events.failure(); event != "" {
		return result, true, errors.New(event)
	}
	if failures, err := uncorrectableEDAC(h.sysRoot); err != nil {
		return result, false, err
	} else if failures != 0 {
		return result, true, fmt.Errorf("EDAC reports %d uncorrectable errors", failures)
	}
	if result.BootID == "" || node.Annotations[inventoryBootIDAnnotation] != result.BootID || result.Inventory == "" || result.InventoryHash == "" {
		return result, false, errors.New("DRA boot inventory is unavailable")
	}
	var inventory inventorySummary
	if err := json.Unmarshal([]byte(result.Inventory), &inventory); err != nil || inventory.Version != 1 || inventory.BootID != result.BootID || len(inventory.Cells) == 0 {
		return result, false, errors.New("DRA boot inventory is invalid")
	}
	identity, err := json.Marshal(inventory.Cells)
	digest := sha256.Sum256(identity)
	if err != nil || hex.EncodeToString(digest[:]) != result.InventoryHash {
		return result, false, errors.New("DRA boot inventory hash is invalid")
	}
	cores, err := physicalCores(h.sysRoot)
	if err != nil {
		return result, false, err
	}
	for _, cell := range inventory.Cells {
		if int64(cores[cell.NUMA]) < cell.CPUs {
			return result, true, fmt.Errorf("NUMA node %d lost CPU cores", cell.NUMA)
		}
		memory, err := numaMemoryBytes(h.sysRoot, cell.NUMA)
		if err != nil {
			return result, false, err
		}
		if memory < cell.MemoryUnits*cell.MemoryUnitBytes {
			return result, true, fmt.Errorf("NUMA node %d lost memory capacity", cell.NUMA)
		}
		if err := memoryChecksum(cell.NUMA, h.memoryBytes); err != nil {
			return result, false, fmt.Errorf("NUMA node %d memory check: %w", cell.NUMA, err)
		}
	}
	if err := checkNVIDIA(inventory); err != nil {
		return result, true, err
	}
	return result, false, nil
}

func physicalCores(sysRoot string) (map[int]int, error) {
	paths, err := filepath.Glob(filepath.Join(sysRoot, "devices/system/cpu/cpu[0-9]*"))
	if err != nil || len(paths) == 0 {
		return nil, errors.New("CPU topology is unavailable")
	}
	type coreKey struct{ numa, socket, core int }
	cores := map[coreKey]struct{}{}
	for _, path := range paths {
		if data, err := os.ReadFile(filepath.Join(path, "online")); err == nil && strings.TrimSpace(string(data)) == "0" {
			continue
		}
		socket, err := readInt(filepath.Join(path, "topology/physical_package_id"))
		if err != nil {
			return nil, fmt.Errorf("read CPU socket: %w", err)
		}
		core, err := readInt(filepath.Join(path, "topology/core_id"))
		if err != nil {
			return nil, fmt.Errorf("read CPU core: %w", err)
		}
		numa := 0
		if nodes, _ := filepath.Glob(filepath.Join(path, "node[0-9]*")); len(nodes) == 1 {
			numa, _ = strconv.Atoi(strings.TrimPrefix(filepath.Base(nodes[0]), "node"))
		}
		cores[coreKey{numa: numa, socket: socket, core: core}] = struct{}{}
	}
	result := map[int]int{}
	for key := range cores {
		result[key.numa]++
	}
	return result, nil
}

func numaMemoryBytes(sysRoot string, numa int) (int64, error) {
	file, err := os.Open(filepath.Join(sysRoot, "devices/system/node", fmt.Sprintf("node%d", numa), "meminfo"))
	if err != nil {
		return 0, fmt.Errorf("read NUMA memory: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 4 && fields[2] == "MemTotal:" {
			value, err := strconv.ParseInt(fields[3], 10, 64)
			return value << 10, err
		}
	}
	return 0, errors.New("NUMA MemTotal is missing")
}

func memoryChecksum(numa, size int) error {
	data := make([]byte, size)
	bits := strconv.IntSize
	mask := make([]uintptr, numa/bits+1)
	mask[numa/bits] = uintptr(1) << (numa % bits)
	_, _, err := unix.Syscall6(unix.SYS_MBIND, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), unix.MPOL_BIND, uintptr(unsafe.Pointer(&mask[0])), uintptr(len(mask)*bits), unix.MPOL_MF_MOVE)
	if err != 0 {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.ENOSYS) {
			return fmt.Errorf("%w: bind NUMA memory", errUnsupported)
		}
		return fmt.Errorf("bind memory: %w", err)
	}
	for i := range data {
		data[i] = byte((i*131 + numa*17) % 251)
	}
	for i, value := range data {
		if value != byte((i*131+numa*17)%251) {
			return fmt.Errorf("checksum mismatch at byte %d", i)
		}
	}
	runtime.KeepAlive(data)
	return nil
}

func checkNVIDIA(inventory inventorySummary) error {
	expected := map[string]inventoryGPU{}
	for _, cell := range inventory.Cells {
		for _, gpu := range cell.GPUs {
			expected[gpu.UUID] = gpu
		}
	}
	if len(expected) == 0 {
		return nil
	}
	output, err := nvidiaSMI("--query-gpu=uuid,name,pci.bus_id", "--format=csv,noheader").Output()
	if err != nil {
		return fmt.Errorf("NVIDIA health query failed: %w", err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(output))).ReadAll()
	if err != nil {
		return fmt.Errorf("parse NVIDIA health query: %w", err)
	}
	actual := map[string]inventoryGPU{}
	for _, row := range rows {
		if len(row) != 3 {
			return errors.New("NVIDIA health query returned an invalid row")
		}
		gpu := inventoryGPU{UUID: strings.TrimSpace(row[0]), Model: normalizeGPU(row[1]), PCI: strings.ToLower(strings.TrimSpace(row[2]))}
		actual[gpu.UUID] = gpu
	}
	if !maps.EqualFunc(expected, actual, func(a, b inventoryGPU) bool { return a == b }) {
		return errors.New("NVIDIA inventory identity changed")
	}
	return nil
}

func nvidiaSMI(arguments ...string) *exec.Cmd {
	if binary := os.Getenv("NVIDIA_SMI"); binary != "" {
		return exec.Command(binary, arguments...)
	}
	if _, err := os.Stat("/host/usr/bin/nvidia-smi"); err == nil {
		return exec.Command("chroot", append([]string{"/host", "/usr/bin/nvidia-smi"}, arguments...)...)
	}
	return exec.Command("nvidia-smi", arguments...)
}

func uncorrectableEDAC(sysRoot string) (int64, error) {
	paths, err := filepath.Glob(filepath.Join(sysRoot, "devices/system/edac/mc/mc*/ue_count"))
	if err != nil {
		return 0, err
	}
	var total int64
	for _, path := range paths {
		value, err := readInt(path)
		if err != nil {
			return 0, fmt.Errorf("read EDAC counter: %w", err)
		}
		total += int64(value)
	}
	return total, nil
}

func readInt(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func normalizeGPU(value string) string {
	value = strings.ToLower(value)
	for _, word := range []string{"nvidia", "geforce", "laptop", "gpu"} {
		value = strings.ReplaceAll(value, word, " ")
	}
	fields := strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') })
	return strings.Join(fields, "_")
}

type kernelEventState struct {
	mu     sync.RWMutex
	reason string
}

func (k *kernelEventState) watch(ctx context.Context, path string) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Seek(0, io.SeekEnd)
	reader := bufio.NewReader(file)
	for ctx.Err() == nil {
		line, err := reader.ReadString('\n')
		if reason := criticalKernelEvent(line); reason != "" {
			k.mu.Lock()
			k.reason = reason
			k.mu.Unlock()
		}
		if err != nil {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (k *kernelEventState) failure() string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.reason
}

func criticalKernelEvent(line string) string {
	lower := strings.ToLower(line)
	patterns := []struct{ pattern, reason string }{
		{"nvrm: xid", "NVIDIA Xid event detected"},
		{"machine check", "CPU machine-check event detected"},
		{"mce:", "CPU MCE event detected"},
		{"uncorrectable", "uncorrectable hardware event detected"},
	}
	for _, candidate := range patterns {
		if strings.Contains(lower, candidate.pattern) {
			return candidate.reason
		}
	}
	return ""
}
