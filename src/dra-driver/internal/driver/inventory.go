package driver

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/hardware"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

const (
	annotationPrefix          = "orchestration.gputpu.io/"
	inventoryAnnotation       = annotationPrefix + "inventory-v1"
	inventoryHashAnnotation   = annotationPrefix + "inventory-hash"
	inventoryBootIDAnnotation = annotationPrefix + "inventory-boot-id"
	kindCPU                   = "cpu"
	kindMemory                = "memory"
	kindGPU                   = "gpu"
	kindOpenTPU               = "opentpu"
	attributeDomain           = "orchestration.gputpu.io/"
)

type localDevice struct {
	Name         string
	Kind         string
	NUMA         int
	CPU          int
	Socket       int
	Core         int
	UnitBytes    int64
	UUID         string
	Model        string
	VRAMMiB      int64
	PCI          string
	Path         string
	Profile      string
	MatrixSize   int64
	CPUCores     int64
	MemoryBytes  int64
	SharedMemory int64
}

type inventory struct {
	nodeName string
	devices  map[string]localDevice
}

type openTPUConfig struct {
	Version  int              `json:"version"`
	Profiles []openTPUProfile `json:"profiles"`
}

type openTPUProfile struct {
	Name         string `json:"name"`
	MatrixSize   int64  `json:"matrixSize"`
	NUMA         int    `json:"numaNode"`
	Slots        int    `json:"slots"`
	CPUCores     int64  `json:"cpuCores"`
	Memory       string `json:"memory"`
	SharedMemory string `json:"sharedMemory"`
}

func discoverInventory(sysRoot, nodeName string, annotations map[string]string) (*inventory, error) {
	memoryUnit, err := quantityBytes(annotations[annotationPrefix+"memory-unit"], "1Gi")
	if err != nil {
		return nil, fmt.Errorf("memory unit: %w", err)
	}
	reservedMemory, err := quantityBytes(annotations[annotationPrefix+"reserved-memory-per-numa"], "1Gi")
	if err != nil {
		return nil, fmt.Errorf("reserved memory: %w", err)
	}
	reservedCores, err := positiveInt(annotations[annotationPrefix+"reserved-cores-per-numa"], 1)
	if err != nil {
		return nil, fmt.Errorf("reserved cores: %w", err)
	}

	inv := &inventory{nodeName: nodeName, devices: map[string]localDevice{}}
	if err := inv.discoverCPU(sysRoot, reservedCores); err != nil {
		return nil, err
	}
	if err := inv.discoverMemory(sysRoot, memoryUnit, reservedMemory); err != nil {
		return nil, err
	}
	if err := inv.discoverOpenTPU(annotations[annotationPrefix+"opentpu-profiles"]); err != nil {
		return nil, err
	}
	if err := inv.discoverNVIDIA(sysRoot); err != nil {
		slog.Warn("NVIDIA inventory unavailable", "error", err)
	}
	return inv, nil
}

func (inv *inventory) summary(bootID string) (hardware.Inventory, string, string, error) {
	if bootID == "" {
		return hardware.Inventory{}, "", "", errors.New("Kubernetes bootID is empty")
	}
	byNUMA := map[int]*hardware.Cell{}
	profiles := map[int]map[string]*hardware.OpenTPU{}
	for _, device := range inv.devices {
		cell := byNUMA[device.NUMA]
		if cell == nil {
			cell = &hardware.Cell{NUMA: device.NUMA}
			byNUMA[device.NUMA] = cell
		}
		switch device.Kind {
		case kindCPU:
			cell.CPUs++
		case kindMemory:
			if cell.MemoryUnitBytes != 0 && cell.MemoryUnitBytes != device.UnitBytes {
				return hardware.Inventory{}, "", "", fmt.Errorf("NUMA node %d has mixed memory units", device.NUMA)
			}
			cell.MemoryUnitBytes = device.UnitBytes
			cell.MemoryUnits++
		case kindGPU:
			cell.GPUs = append(cell.GPUs, hardware.GPU{UUID: device.UUID, Model: device.Model, PCI: device.PCI})
		case kindOpenTPU:
			if profiles[device.NUMA] == nil {
				profiles[device.NUMA] = map[string]*hardware.OpenTPU{}
			}
			profile := profiles[device.NUMA][device.Profile]
			if profile == nil {
				profile = &hardware.OpenTPU{Profile: device.Profile, MatrixSize: device.MatrixSize, CPUCores: device.CPUCores, MemoryBytes: device.MemoryBytes, SharedMemory: device.SharedMemory}
				profiles[device.NUMA][device.Profile] = profile
			}
			if profile.MatrixSize != device.MatrixSize || profile.CPUCores != device.CPUCores || profile.MemoryBytes != device.MemoryBytes || profile.SharedMemory != device.SharedMemory {
				return hardware.Inventory{}, "", "", fmt.Errorf("OpenTPU profile %q has inconsistent footprints", device.Profile)
			}
			profile.Count++
		}
	}
	result := hardware.Inventory{Version: 1, BootID: bootID}
	for _, numa := range slices.Sorted(maps.Keys(byNUMA)) {
		cell := byNUMA[numa]
		slices.SortFunc(cell.GPUs, func(a, b hardware.GPU) int { return strings.Compare(a.UUID, b.UUID) })
		for _, name := range slices.Sorted(maps.Keys(profiles[numa])) {
			cell.OpenTPU = append(cell.OpenTPU, *profiles[numa][name])
		}
		result.Cells = append(result.Cells, *cell)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return hardware.Inventory{}, "", "", err
	}
	identity, err := json.Marshal(result.Cells)
	if err != nil {
		return hardware.Inventory{}, "", "", err
	}
	digest := sha256.Sum256(identity)
	return result, string(encoded), hex.EncodeToString(digest[:]), nil
}

func (inv *inventory) discoverCPU(sysRoot string, reserve int) error {
	paths, err := filepath.Glob(filepath.Join(sysRoot, "devices/system/cpu/cpu[0-9]*"))
	if err != nil || len(paths) == 0 {
		return fmt.Errorf("discover CPUs: no CPU topology found")
	}
	type key struct{ numa, socket, core int }
	cores := map[key]int{}
	for _, path := range paths {
		cpu, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "cpu"))
		if err != nil || !onlineCPU(path) {
			continue
		}
		socket, err1 := readInt(filepath.Join(path, "topology/physical_package_id"))
		core, err2 := readInt(filepath.Join(path, "topology/core_id"))
		if err1 != nil || err2 != nil {
			return fmt.Errorf("read topology for CPU %d", cpu)
		}
		numa := cpuNUMA(path)
		k := key{numa, socket, core}
		if old, ok := cores[k]; !ok || cpu < old {
			cores[k] = cpu
		}
	}
	byNUMA := map[int][]localDevice{}
	for k, cpu := range cores {
		byNUMA[k.numa] = append(byNUMA[k.numa], localDevice{Kind: kindCPU, NUMA: k.numa, CPU: cpu, Socket: k.socket, Core: k.core})
	}
	for _, devices := range byNUMA {
		slices.SortFunc(devices, func(a, b localDevice) int { return a.CPU - b.CPU })
		if len(devices) <= reserve {
			continue
		}
		for _, device := range devices[reserve:] {
			device.Name = fmt.Sprintf("cpu-s%d-c%d", device.Socket, device.Core)
			inv.devices[device.Name] = device
		}
	}
	return nil
}

func (inv *inventory) discoverMemory(sysRoot string, unit, reserve int64) error {
	paths, _ := filepath.Glob(filepath.Join(sysRoot, "devices/system/node/node[0-9]*"))
	if len(paths) == 0 {
		return fmt.Errorf("discover NUMA memory: no node topology found")
	}
	for _, path := range paths {
		numa, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(path), "node"))
		total, err := nodeMemoryBytes(filepath.Join(path, "meminfo"))
		if err != nil {
			return fmt.Errorf("read NUMA node %d memory: %w", numa, err)
		}
		count := (total - reserve) / unit
		for i := int64(0); i < count; i++ {
			name := fmt.Sprintf("memory-n%d-%05d", numa, i)
			inv.devices[name] = localDevice{Name: name, Kind: kindMemory, NUMA: numa, UnitBytes: unit}
		}
	}
	return nil
}

func (inv *inventory) discoverOpenTPU(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var config openTPUConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil || config.Version != 1 {
		return fmt.Errorf("parse OpenTPU profiles: version 1 JSON is required")
	}
	for _, profile := range config.Profiles {
		memory, err := quantityBytes(profile.Memory, "")
		if err != nil {
			return fmt.Errorf("OpenTPU profile %q memory: %w", profile.Name, err)
		}
		shared, err := quantityBytes(profile.SharedMemory, "")
		if err != nil {
			return fmt.Errorf("OpenTPU profile %q shared memory: %w", profile.Name, err)
		}
		name := normalize(profile.Name)
		if name == "" || profile.Name != name || profile.Slots < 1 || profile.CPUCores < 1 || (profile.MatrixSize != 8 && profile.MatrixSize != 16) {
			return fmt.Errorf("OpenTPU profile %q has invalid name, slot count, footprint, or matrix size", profile.Name)
		}
		publishedProfile := "opentpu_" + strings.TrimPrefix(name, "opentpu_")
		for slot := range profile.Slots {
			deviceName := fmt.Sprintf("opentpu-%s-n%d-%03d", name, profile.NUMA, slot)
			if _, exists := inv.devices[deviceName]; exists {
				return fmt.Errorf("duplicate OpenTPU device %q", deviceName)
			}
			inv.devices[deviceName] = localDevice{Name: deviceName, Kind: kindOpenTPU, NUMA: profile.NUMA, Profile: publishedProfile, MatrixSize: profile.MatrixSize, CPUCores: profile.CPUCores, MemoryBytes: memory, SharedMemory: shared}
		}
	}
	return nil
}

func (inv *inventory) discoverNVIDIA(sysRoot string) error {
	command := nvidiaSMI("--query-gpu=index,uuid,name,memory.total,pci.bus_id", "--format=csv,noheader,nounits")
	output, err := command.Output()
	if err != nil {
		var executable *exec.Error
		if errors.As(err, &executable) {
			return nil
		}
		return err
	}
	numaCount := len(inv.numaNodes())
	rows, err := csv.NewReader(strings.NewReader(string(output))).ReadAll()
	if err != nil {
		return fmt.Errorf("parse nvidia-smi output: %w", err)
	}
	for _, parts := range rows {
		if len(parts) != 5 {
			return fmt.Errorf("unexpected nvidia-smi row %q", parts)
		}
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		index, err1 := strconv.Atoi(parts[0])
		vram, err2 := strconv.ParseInt(parts[3], 10, 64)
		if err1 != nil || err2 != nil || parts[1] == "" {
			return fmt.Errorf("invalid nvidia-smi row %q", parts)
		}
		pci := strings.ToLower(parts[4])
		numa, err := readInt(filepath.Join(sysRoot, "bus/pci/devices", pci, "numa_node"))
		if err != nil || numa < 0 {
			if numaCount != 1 {
				slog.Warn("GPU omitted because NUMA locality is unknown", "uuid", parts[1], "pci", pci)
				continue
			}
			numa = inv.numaNodes()[0]
		}
		name := "gpu-" + strings.ToLower(parts[1])
		path := fmt.Sprintf("/dev/nvidia%d", index)
		if _, err := os.Stat("/dev/dxg"); err == nil {
			path = "/dev/dxg"
		}
		inv.devices[name] = localDevice{Name: name, Kind: kindGPU, NUMA: numa, UUID: parts[1], Model: hardware.NormalizeGPU(parts[2]), VRAMMiB: vram, PCI: pci, Path: path}
	}
	return nil
}

func (inv *inventory) resources() resourceslice.DriverResources {
	names := slices.Sorted(maps.Keys(inv.devices))
	devices := make([]resourceapi.Device, 0, len(names))
	for _, name := range names {
		devices = append(devices, inv.devices[name].resourceDevice())
	}
	slicesOut := make([]resourceslice.Slice, 0, (len(devices)+resourceapi.ResourceSliceMaxDevices-1)/resourceapi.ResourceSliceMaxDevices)
	for len(devices) > 0 {
		n := min(len(devices), resourceapi.ResourceSliceMaxDevices)
		slicesOut = append(slicesOut, resourceslice.Slice{Devices: slices.Clone(devices[:n])})
		devices = devices[n:]
	}
	if len(slicesOut) == 0 {
		slicesOut = []resourceslice.Slice{{}}
	}
	return resourceslice.DriverResources{Pools: map[string]resourceslice.Pool{inv.nodeName: {Slices: slicesOut}}}
}

func (d localDevice) resourceDevice() resourceapi.Device {
	attrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		attributeDomain + "kind":     stringAttribute(d.Kind),
		attributeDomain + "numaNode": intAttribute(int64(d.NUMA)),
	}
	capacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{}
	switch d.Kind {
	case kindCPU:
		attrs[attributeDomain+"logicalCPU"] = intAttribute(int64(d.CPU))
		attrs[attributeDomain+"socket"] = intAttribute(int64(d.Socket))
		attrs[attributeDomain+"core"] = intAttribute(int64(d.Core))
	case kindMemory:
		attrs[attributeDomain+"unitBytes"] = intAttribute(d.UnitBytes)
		capacity[attributeDomain+"memory"] = capacityValue(d.UnitBytes)
	case kindGPU:
		attrs[attributeDomain+"uuid"] = stringAttribute(d.UUID)
		attrs[attributeDomain+"model"] = stringAttribute(d.Model)
		attrs[attributeDomain+"pci"] = stringAttribute(d.PCI)
		attrs[attributeDomain+"path"] = stringAttribute(d.Path)
		capacity[attributeDomain+"vram"] = resourceapi.DeviceCapacity{Value: resource.MustParse(fmt.Sprintf("%dMi", d.VRAMMiB))}
	case kindOpenTPU:
		attrs[attributeDomain+"profile"] = stringAttribute(d.Profile)
		attrs[attributeDomain+"matrixSize"] = intAttribute(d.MatrixSize)
		attrs[attributeDomain+"cpuCores"] = intAttribute(d.CPUCores)
		capacity[attributeDomain+"memory"] = capacityValue(d.MemoryBytes)
		capacity[attributeDomain+"sharedMemory"] = capacityValue(d.SharedMemory)
	}
	return resourceapi.Device{Name: d.Name, Attributes: attrs, Capacity: capacity}
}

func (inv *inventory) hasKind(kind string) bool {
	for _, device := range inv.devices {
		if device.Kind == kind {
			return true
		}
	}
	return false
}

func (inv *inventory) removeKind(kind string) {
	for name, device := range inv.devices {
		if device.Kind == kind {
			delete(inv.devices, name)
		}
	}
}

func (inv *inventory) numaNodes() []int {
	set := map[int]struct{}{}
	for _, device := range inv.devices {
		if device.Kind == kindCPU || device.Kind == kindMemory {
			set[device.NUMA] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func quantityBytes(raw, fallback string) (int64, error) {
	if raw == "" {
		raw = fallback
	}
	quantity, err := resource.ParseQuantity(raw)
	if err != nil || quantity.Value() <= 0 {
		return 0, fmt.Errorf("must be a positive Kubernetes quantity")
	}
	return quantity.Value(), nil
}

func positiveInt(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	return value, nil
}

func onlineCPU(path string) bool {
	data, err := os.ReadFile(filepath.Join(path, "online"))
	return err != nil || strings.TrimSpace(string(data)) != "0"
}

func cpuNUMA(path string) int {
	nodes, _ := filepath.Glob(filepath.Join(path, "node[0-9]*"))
	if len(nodes) == 1 {
		value, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(nodes[0]), "node"))
		return value
	}
	return 0
}

func readInt(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func nodeMemoryBytes(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 4 && fields[2] == "MemTotal:" {
			kib, err := strconv.ParseInt(fields[3], 10, 64)
			return kib * 1024, err
		}
	}
	return 0, fmt.Errorf("MemTotal not found")
}

func normalize(value string) string {
	value = strings.ToLower(value)
	fields := strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') })
	return strings.Join(fields, "_")
}

func intAttribute(value int64) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{IntValue: &value}
}
func stringAttribute(value string) resourceapi.DeviceAttribute {
	return resourceapi.DeviceAttribute{StringValue: &value}
}
func capacityValue(bytes int64) resourceapi.DeviceCapacity {
	return resourceapi.DeviceCapacity{Value: *resource.NewQuantity(bytes, resource.BinarySI)}
}

func nvidiaSMI(arguments ...string) *exec.Cmd {
	path := os.Getenv("NVIDIA_SMI")
	if path == "" {
		if _, err := os.Stat("/host/usr/bin/nvidia-smi"); err == nil {
			return exec.Command("chroot", append([]string{"/host", "/usr/bin/nvidia-smi"}, arguments...)...)
		}
		path = "nvidia-smi"
	}
	return exec.Command(path, arguments...)
}
