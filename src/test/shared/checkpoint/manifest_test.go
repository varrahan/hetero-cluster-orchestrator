package checkpoint

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validManifest() Manifest {
	hash := strings.Repeat("0", 64)
	return Manifest{CheckpointVersion: 2, RunID: "run", GlobalStep: 1, Epoch: 0, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Metadata: Metadata{TotalParameters: 4, CanonicalDType: "float32", ModelID: "model", ModelSchemaHash: hash, DatasetID: "data", ContainerImageDigest: "sha256:" + hash, Framework: "test", FrameworkVersion: "1", AdapterVersions: map[string]string{"cpu": "1"}},
		World:    World{Size: 1, Ranks: []Rank{{Rank: 0, Hardware: "cpu", Adapter: "cpu"}}},
		Tensors:  map[string]Tensor{"weight": {Role: "model_parameter", GlobalShape: []uint64{4}, CanonicalDType: "float32", ByteOrder: "little", Layout: "C", Chunks: []Chunk{{ChunkID: "weight_0", StoragePath: "shards/weight.bin", Slice: [][2]uint64{{0, 4}}, Shape: []uint64{4}, ByteLength: 16, SHA256: hash, WriterRank: 0}}}},
		State:    State{OptimizerMetadata: Artifact{StoragePath: "optimizer_state.json", ByteLength: 2, SHA256: hash}, Scheduler: map[string]json.RawMessage{}, RNG: map[string]Artifact{"0": {StoragePath: "rng/rank_00000.bin", ByteLength: 1, SHA256: hash}}, DataCursor: map[string]json.RawMessage{}},
	}
}

func TestManifestValidation(t *testing.T) {
	manifest := validManifest()
	data, _ := json.Marshal(manifest)
	if _, err := DecodeManifest(data); err != nil {
		t.Fatal(err)
	}
	manifest.Tensors["weight"] = Tensor{Role: "model_parameter", GlobalShape: []uint64{4}, CanonicalDType: "float32", ByteOrder: "little", Layout: "C", Chunks: []Chunk{
		{ChunkID: "a", StoragePath: "a.bin", Slice: [][2]uint64{{0, 3}}, Shape: []uint64{3}, ByteLength: 12, SHA256: strings.Repeat("0", 64)},
		{ChunkID: "b", StoragePath: "b.bin", Slice: [][2]uint64{{2, 4}}, Shape: []uint64{2}, ByteLength: 8, SHA256: strings.Repeat("0", 64)},
	}}
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestCompatibilityAndOptimizerReferences(t *testing.T) {
	manifest := validManifest()
	want := Compatibility{ModelID: "model", ModelSchemaHash: manifest.Metadata.ModelSchemaHash, DatasetID: "data", ContainerImageDigest: manifest.Metadata.ContainerImageDigest, Framework: "test", FrameworkVersion: "1", AdapterVersions: map[string]string{"cpu": "1"}}
	if err := manifest.Compatible(want); err != nil {
		t.Fatal(err)
	}
	want.FrameworkVersion = "2"
	if err := manifest.Compatible(want); err == nil {
		t.Fatal("incompatible framework version accepted")
	}
	state := []byte(`{"format_version":1,"parameter_groups":[{"parameters":["weight"],"options":{}}],"parameters":{"weight":{"scalars":{"step":1},"tensors":{"momentum":"missing"}}}}`)
	if _, err := DecodeOptimizerState(state, &manifest); err == nil {
		t.Fatal("unknown optimizer tensor accepted")
	}
}
