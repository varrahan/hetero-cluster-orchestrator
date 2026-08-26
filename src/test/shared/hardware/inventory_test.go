package hardware

import (
	"encoding/json"
	"testing"
)

func TestInventoryJSONAndNormalizeGPU(t *testing.T) {
	inventory := Inventory{Version: 1, BootID: "boot-a", Cells: []Cell{{
		NUMA: 1, CPUs: 4, MemoryUnits: 8, MemoryUnitBytes: 1 << 30,
		GPUs:    []GPU{{UUID: "GPU-a", Model: "rtx_4050", PCI: "0000:01:00.0"}},
		OpenTPU: []OpenTPU{{Profile: "opentpu_m8", Count: 2, MatrixSize: 8, CPUCores: 2, MemoryBytes: 1 << 30, SharedMemory: 512 << 20}},
	}}}
	encoded, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":1,"bootID":"boot-a","cells":[{"numaNode":1,"cpus":4,"memoryUnits":8,"memoryUnitBytes":1073741824,"gpus":[{"uuid":"GPU-a","model":"rtx_4050","pci":"0000:01:00.0"}],"openTPU":[{"profile":"opentpu_m8","count":2,"matrixSize":8,"cpuCores":2,"memoryBytes":1073741824,"sharedMemoryBytes":536870912}]}]}`
	if string(encoded) != want {
		t.Fatalf("inventory JSON = %s", encoded)
	}
	if got := NormalizeGPU("NVIDIA GeForce RTX 4050 Laptop GPU"); got != "rtx_4050" {
		t.Fatalf("NormalizeGPU = %q", got)
	}
}
