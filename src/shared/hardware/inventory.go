package hardware

import "strings"

type Inventory struct {
	Version int    `json:"version"`
	BootID  string `json:"bootID"`
	Cells   []Cell `json:"cells"`
}

type Cell struct {
	NUMA            int       `json:"numaNode"`
	CPUs            int64     `json:"cpus"`
	MemoryUnits     int64     `json:"memoryUnits"`
	MemoryUnitBytes int64     `json:"memoryUnitBytes"`
	GPUs            []GPU     `json:"gpus,omitempty"`
	OpenTPU         []OpenTPU `json:"openTPU,omitempty"`
}

type GPU struct {
	UUID  string `json:"uuid"`
	Model string `json:"model"`
	PCI   string `json:"pci"`
}

type OpenTPU struct {
	Profile      string `json:"profile"`
	Count        int64  `json:"count"`
	MatrixSize   int64  `json:"matrixSize"`
	CPUCores     int64  `json:"cpuCores"`
	MemoryBytes  int64  `json:"memoryBytes"`
	SharedMemory int64  `json:"sharedMemoryBytes"`
}

func NormalizeGPU(value string) string {
	value = strings.ToLower(value)
	for _, word := range []string{"nvidia", "geforce", "laptop", "gpu"} {
		value = strings.ReplaceAll(value, word, " ")
	}
	fields := strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') })
	return strings.Join(fields, "_")
}
