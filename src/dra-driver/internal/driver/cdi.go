package driver

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

type cdiSpec struct {
	Version string      `json:"cdiVersion"`
	Kind    string      `json:"kind"`
	Devices []cdiDevice `json:"devices"`
}

type cdiDevice struct {
	Name  string   `json:"name"`
	Edits cdiEdits `json:"containerEdits"`
}

type cdiEdits struct {
	Env         []string        `json:"env,omitempty"`
	DeviceNodes []cdiDeviceNode `json:"deviceNodes,omitempty"`
	Mounts      []cdiMount      `json:"mounts,omitempty"`
}

type cdiDeviceNode struct {
	Path string `json:"path"`
}

type cdiMount struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath"`
	Options       []string `json:"options,omitempty"`
}

func writeOpenTPUCDI(path string, inv *inventory) error {
	spec := cdiSpec{Version: "0.8.0", Kind: driverName + "/opentpu"}
	for _, device := range inv.devices {
		if device.Kind != kindOpenTPU {
			continue
		}
		spec.Devices = append(spec.Devices, cdiDevice{Name: device.Name, Edits: cdiEdits{Env: []string{
			"OPENTPU_DEVICE=" + device.Name,
			"OPENTPU_PROFILE=" + device.Profile,
			fmt.Sprintf("OPENTPU_MATRIX_SIZE=%d", device.MatrixSize),
			fmt.Sprintf("OPENTPU_SHARED_MEMORY_BYTES=%d", device.SharedMemory),
		}}})
	}
	slices.SortFunc(spec.Devices, func(a, b cdiDevice) int { return strings.Compare(a.Name, b.Name) })
	return atomicJSON(path, spec, 0644)
}

func generateNVIDIACDI(path string, inv *inventory) error {
	if _, err := os.Stat("/dev/dxg"); err == nil {
		return writeWSLNVIDIACDI(path, inv)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	arguments := []string{"cdi", "generate", "--device-name-strategy=uuid", "--vendor=" + driverName, "--class=nvidia", "--output=" + path}
	if _, err := os.Stat("/host"); err == nil {
		arguments = append(arguments, "--driver-root=/host")
	}
	command := exec.Command("nvidia-ctk", arguments...)
	if _, err := os.Stat("/host/usr/lib/wsl/lib"); err == nil {
		paths := []string{"/host/usr/lib/wsl/lib", "/host/usr/lib/x86_64-linux-gnu"}
		if inherited := os.Getenv("LD_LIBRARY_PATH"); inherited != "" {
			paths = append(paths, inherited)
		}
		command.Env = append(os.Environ(), "LD_LIBRARY_PATH="+strings.Join(paths, ":"))
	}
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("nvidia-ctk cdi generate: %w: %s", err, output)
	}
	return nil
}

func writeWSLNVIDIACDI(path string, inv *inventory) error {
	required := []string{
		"/usr/bin/nvidia-smi",
		"/usr/lib/x86_64-linux-gnu/libcuda.so.1",
		"/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1",
		"/usr/lib/x86_64-linux-gnu/libnvidia-gpucomp.so",
		"/usr/lib/x86_64-linux-gnu/libdxcore.so",
	}
	edits := cdiEdits{DeviceNodes: []cdiDeviceNode{{Path: "/dev/dxg"}}}
	for _, source := range required {
		if _, err := os.Stat("/host" + source); err != nil {
			return fmt.Errorf("WSL NVIDIA path %s: %w", source, err)
		}
		edits.Mounts = append(edits.Mounts, cdiMount{HostPath: source, ContainerPath: source, Options: []string{"ro", "nosuid", "nodev", "bind"}})
	}
	driverFiles, err := filepath.Glob("/host/usr/lib/wsl/drivers/*/*")
	if err != nil {
		return fmt.Errorf("discover WSL NVIDIA driver files: %w", err)
	}
	if len(driverFiles) == 0 {
		return fmt.Errorf("discover WSL NVIDIA driver files: none found")
	}
	for _, file := range driverFiles {
		info, err := os.Stat(file)
		if err != nil {
			return err
		}
		if info.IsDir() {
			continue
		}
		source := strings.TrimPrefix(file, "/host")
		edits.Mounts = append(edits.Mounts, cdiMount{HostPath: source, ContainerPath: source, Options: []string{"ro", "nosuid", "nodev", "bind"}})
	}
	spec := cdiSpec{Version: "0.8.0", Kind: driverName + "/nvidia"}
	for _, device := range inv.devices {
		if device.Kind == kindGPU {
			spec.Devices = append(spec.Devices, cdiDevice{Name: device.UUID, Edits: edits})
		}
	}
	slices.SortFunc(spec.Devices, func(a, b cdiDevice) int { return strings.Compare(a.Name, b.Name) })
	return atomicJSON(path, spec, 0644)
}

func atomicJSON(path string, value any, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	defer temporary.Close()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}
