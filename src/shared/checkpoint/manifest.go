package checkpoint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	Version          = 2
	MaxManifestBytes = 16 << 20
	MaxChunks        = 4096
	MaxDimensions    = 16
)

var (
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	tensorPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}$`)
	adapterPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	hashPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Manifest struct {
	CheckpointVersion int               `json:"checkpoint_version"`
	RunID             string            `json:"run_id"`
	GlobalStep        uint64            `json:"global_step"`
	Epoch             float64           `json:"epoch"`
	CreatedAt         string            `json:"created_at"`
	Metadata          Metadata          `json:"metadata"`
	World             World             `json:"world"`
	Tensors           map[string]Tensor `json:"tensors"`
	State             State             `json:"state"`
}

type Metadata struct {
	TotalParameters      uint64            `json:"total_parameters"`
	CanonicalDType       string            `json:"canonical_dtype"`
	ModelID              string            `json:"model_id"`
	ModelSchemaHash      string            `json:"model_schema_hash"`
	DatasetID            string            `json:"dataset_id"`
	ContainerImageDigest string            `json:"container_image_digest"`
	Framework            string            `json:"framework"`
	FrameworkVersion     string            `json:"framework_version"`
	AdapterVersions      map[string]string `json:"adapter_versions"`
}

type World struct {
	Size  int    `json:"size"`
	Ranks []Rank `json:"ranks"`
}

type Rank struct {
	Rank     int    `json:"rank"`
	Hardware string `json:"hardware"`
	Adapter  string `json:"adapter"`
}

type Tensor struct {
	Role           string        `json:"role"`
	GlobalShape    []uint64      `json:"global_shape"`
	CanonicalDType string        `json:"canonical_dtype"`
	ByteOrder      string        `json:"byte_order"`
	Layout         string        `json:"layout"`
	Quantization   *Quantization `json:"quantization,omitempty"`
	Chunks         []Chunk       `json:"chunks"`
}

type Quantization struct {
	Scheme    string   `json:"scheme"`
	Scale     float64  `json:"scale"`
	ZeroPoint int      `json:"zero_point"`
	Rounding  string   `json:"rounding"`
	Clamp     [2]int16 `json:"clamp"`
}

type Chunk struct {
	ChunkID     string      `json:"chunk_id"`
	StoragePath string      `json:"storage_path"`
	Slice       [][2]uint64 `json:"slice"`
	Shape       []uint64    `json:"shape"`
	ByteOffset  uint64      `json:"byte_offset"`
	ByteLength  uint64      `json:"byte_length"`
	SHA256      string      `json:"sha256"`
	WriterRank  int         `json:"writer_rank"`
}

type State struct {
	OptimizerMetadata Artifact                   `json:"optimizer_metadata"`
	Scheduler         map[string]json.RawMessage `json:"scheduler"`
	RNG               map[string]Artifact        `json:"rng"`
	DataCursor        map[string]json.RawMessage `json:"data_cursor"`
}

type Artifact struct {
	StoragePath string `json:"storage_path"`
	ByteLength  uint64 `json:"byte_length"`
	SHA256      string `json:"sha256"`
}

type Object struct {
	ID         string
	Path       string
	Offset     uint64
	Length     uint64
	SHA256     string
	WriterRank int
}

type Receipt struct {
	StoragePath string `json:"storage_path"`
	ByteLength  uint64 `json:"byte_length"`
	SHA256      string `json:"sha256"`
	ETag        string `json:"etag,omitempty"`
}

type CommitMarker struct {
	CheckpointVersion int    `json:"checkpoint_version"`
	GlobalStep        uint64 `json:"global_step"`
	ManifestSHA256    string `json:"manifest_sha256"`
	CommittedAt       string `json:"committed_at"`
	SlurmJobID        uint64 `json:"slurm_job_id,omitempty"`
}

type Compatibility struct {
	ModelID              string            `json:"model_id"`
	ModelSchemaHash      string            `json:"model_schema_hash"`
	DatasetID            string            `json:"dataset_id"`
	ContainerImageDigest string            `json:"container_image_digest"`
	Framework            string            `json:"framework"`
	FrameworkVersion     string            `json:"framework_version"`
	AdapterVersions      map[string]string `json:"adapter_versions"`
}

func DecodeManifest(data []byte) (*Manifest, error) {
	if len(data) == 0 || len(data) > MaxManifestBytes {
		return nil, fmt.Errorf("manifest length must be between 1 and %d bytes", MaxManifestBytes)
	}
	var manifest Manifest
	if err := DecodeStrictJSON(bytes.NewReader(data), &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// DecodeStrictJSON decodes exactly one JSON value and rejects unknown fields.
func DecodeStrictJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (m *Manifest) Validate() error {
	if m.CheckpointVersion != Version {
		return fmt.Errorf("checkpoint_version must be %d", Version)
	}
	if !idPattern.MatchString(m.RunID) {
		return errors.New("invalid run_id")
	}
	if !finiteNonNegative(m.Epoch) {
		return errors.New("epoch must be finite and non-negative")
	}
	if _, err := time.Parse(time.RFC3339, m.CreatedAt); err != nil {
		return fmt.Errorf("invalid created_at: %w", err)
	}
	if err := m.Metadata.validate(); err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	if err := m.World.validate(); err != nil {
		return fmt.Errorf("world: %w", err)
	}
	for _, rank := range m.World.Ranks {
		if m.Metadata.AdapterVersions[rank.Hardware] == "" {
			return fmt.Errorf("rank %d hardware %q has no advertised adapter version", rank.Rank, rank.Hardware)
		}
	}
	if len(m.Tensors) == 0 {
		return errors.New("tensors must not be empty")
	}

	paths := map[string]string{}
	chunkIDs := map[string]struct{}{}
	totalChunks := 0
	var totalParameters uint64
	for name, tensor := range m.Tensors {
		if !tensorPattern.MatchString(name) {
			return fmt.Errorf("invalid tensor name %q", name)
		}
		if err := tensor.validate(name, m.World, paths, chunkIDs); err != nil {
			return err
		}
		totalChunks += len(tensor.Chunks)
		if totalChunks > MaxChunks {
			return fmt.Errorf("manifest contains more than %d chunks", MaxChunks)
		}
		if tensor.Role == "model_parameter" {
			elements, err := product(tensor.GlobalShape)
			if err != nil || math.MaxUint64-totalParameters < elements {
				return fmt.Errorf("tensor %q parameter count overflows", name)
			}
			totalParameters += elements
			if tensor.CanonicalDType != m.Metadata.CanonicalDType {
				return fmt.Errorf("tensor %q dtype differs from metadata canonical_dtype", name)
			}
		}
	}
	if totalParameters != m.Metadata.TotalParameters {
		return fmt.Errorf("total_parameters is %d, tensors contain %d", m.Metadata.TotalParameters, totalParameters)
	}
	if err := m.State.validate(m.World, paths); err != nil {
		return fmt.Errorf("state: %w", err)
	}
	return nil
}

func (m Metadata) validate() error {
	if _, ok := dtypeSize(m.CanonicalDType); !ok {
		return errors.New("invalid canonical_dtype")
	}
	for name, value := range map[string]string{
		"model_id": m.ModelID, "dataset_id": m.DatasetID, "framework": m.Framework,
		"framework_version": m.FrameworkVersion,
	} {
		if value == "" || len(value) > 512 {
			return fmt.Errorf("%s is empty or too long", name)
		}
	}
	if !hashPattern.MatchString(m.ModelSchemaHash) {
		return errors.New("invalid model_schema_hash")
	}
	if !strings.HasPrefix(m.ContainerImageDigest, "sha256:") || !hashPattern.MatchString(strings.TrimPrefix(m.ContainerImageDigest, "sha256:")) {
		return errors.New("invalid container_image_digest")
	}
	if len(m.AdapterVersions) == 0 || len(m.AdapterVersions) > 64 {
		return errors.New("adapter_versions must contain 1 to 64 entries")
	}
	for name, version := range m.AdapterVersions {
		if !adapterPattern.MatchString(name) || version == "" || len(version) > 64 {
			return fmt.Errorf("invalid adapter version %q", name)
		}
	}
	return nil
}

func (w World) validate() error {
	if w.Size < 1 || w.Size > 65536 || len(w.Ranks) != w.Size {
		return errors.New("size must match 1 to 65536 ranks")
	}
	seen := make([]bool, w.Size)
	for _, rank := range w.Ranks {
		if rank.Rank < 0 || rank.Rank >= w.Size || seen[rank.Rank] {
			return fmt.Errorf("invalid or duplicate rank %d", rank.Rank)
		}
		seen[rank.Rank] = true
		if !slices.Contains([]string{"cpu", "nvidia-gpu", "opentpu-sim"}, rank.Hardware) || rank.Adapter == "" || len(rank.Adapter) > 64 {
			return fmt.Errorf("rank %d has invalid hardware or adapter", rank.Rank)
		}
	}
	return nil
}

func (t Tensor) validate(name string, world World, paths map[string]string, chunkIDs map[string]struct{}) error {
	if !slices.Contains([]string{"model_parameter", "model_buffer", "optimizer_tensor", "application_state"}, t.Role) {
		return fmt.Errorf("tensor %q has invalid role", name)
	}
	if len(t.GlobalShape) == 0 || len(t.GlobalShape) > MaxDimensions {
		return fmt.Errorf("tensor %q has invalid dimensionality", name)
	}
	globalElements, err := product(t.GlobalShape)
	if err != nil {
		return fmt.Errorf("tensor %q: %w", name, err)
	}
	size, ok := dtypeSize(t.CanonicalDType)
	if !ok || t.ByteOrder != "little" || t.Layout != "C" || len(t.Chunks) == 0 {
		return fmt.Errorf("tensor %q has invalid dtype, byte order, layout, or chunks", name)
	}
	if q := t.Quantization; q != nil {
		if q.Scheme != "affine_int8" || !finitePositive(q.Scale) || q.ZeroPoint < -128 || q.ZeroPoint > 127 || q.Rounding != "ties_to_even" || q.Clamp != [2]int16{-128, 127} {
			return fmt.Errorf("tensor %q has invalid quantization", name)
		}
		if t.CanonicalDType != "float32" && t.CanonicalDType != "bfloat16" {
			return fmt.Errorf("tensor %q quantization requires float32 or bfloat16", name)
		}
	}

	var covered uint64
	for i, chunk := range t.Chunks {
		if !idPattern.MatchString(chunk.ChunkID) {
			return fmt.Errorf("tensor %q has invalid chunk_id", name)
		}
		if _, exists := chunkIDs[chunk.ChunkID]; exists {
			return fmt.Errorf("duplicate chunk_id %q", chunk.ChunkID)
		}
		chunkIDs[chunk.ChunkID] = struct{}{}
		if err := uniquePath(chunk.StoragePath, "chunk "+chunk.ChunkID, paths); err != nil {
			return err
		}
		if len(chunk.Slice) != len(t.GlobalShape) || len(chunk.Shape) != len(t.GlobalShape) {
			return fmt.Errorf("chunk %q dimensionality does not match tensor", chunk.ChunkID)
		}
		for dimension, bounds := range chunk.Slice {
			if bounds[0] >= bounds[1] || bounds[1] > t.GlobalShape[dimension] || chunk.Shape[dimension] != bounds[1]-bounds[0] {
				return fmt.Errorf("chunk %q has invalid slice or shape", chunk.ChunkID)
			}
		}
		elements, err := product(chunk.Shape)
		if err != nil || elements > math.MaxUint64/size || elements*size != chunk.ByteLength || chunk.ByteLength == 0 || chunk.ByteOffset > math.MaxUint64-chunk.ByteLength {
			return fmt.Errorf("chunk %q has invalid byte range", chunk.ChunkID)
		}
		if !hashPattern.MatchString(chunk.SHA256) || chunk.WriterRank < 0 || chunk.WriterRank >= world.Size {
			return fmt.Errorf("chunk %q has invalid hash or writer rank", chunk.ChunkID)
		}
		if math.MaxUint64-covered < elements {
			return fmt.Errorf("tensor %q coverage overflows", name)
		}
		covered += elements
		// ponytail: O(n^2) is bounded by MaxChunks; add a spatial index only if real manifests hit that ceiling.
		for j := 0; j < i; j++ {
			if slicesOverlap(chunk.Slice, t.Chunks[j].Slice) {
				return fmt.Errorf("tensor %q chunks %q and %q overlap", name, chunk.ChunkID, t.Chunks[j].ChunkID)
			}
		}
	}
	if covered != globalElements {
		return fmt.Errorf("tensor %q chunks cover %d of %d elements", name, covered, globalElements)
	}
	return nil
}

func (s State) validate(world World, paths map[string]string) error {
	if err := s.OptimizerMetadata.validate("optimizer_metadata", paths); err != nil {
		return err
	}
	if s.Scheduler == nil || s.DataCursor == nil || len(s.RNG) != world.Size {
		return errors.New("scheduler, data_cursor, and one RNG artifact per rank are required")
	}
	for rank, artifact := range s.RNG {
		value, err := strconv.Atoi(rank)
		if err != nil || value < 0 || value >= world.Size {
			return fmt.Errorf("invalid RNG rank %q", rank)
		}
		if err := artifact.validate("rng rank "+rank, paths); err != nil {
			return err
		}
	}
	return nil
}

func (a Artifact) validate(owner string, paths map[string]string) error {
	if err := uniquePath(a.StoragePath, owner, paths); err != nil {
		return err
	}
	if a.ByteLength == 0 || !hashPattern.MatchString(a.SHA256) {
		return fmt.Errorf("%s has invalid length or hash", owner)
	}
	return nil
}

func uniquePath(value, owner string, paths map[string]string) error {
	if !ValidStoragePath(value) {
		return fmt.Errorf("%s has invalid storage path", owner)
	}
	if previous, exists := paths[value]; exists {
		return fmt.Errorf("storage path %q is shared by %s and %s", value, previous, owner)
	}
	paths[value] = owner
	return nil
}

func ValidStoragePath(value string) bool {
	return value != "" && len(value) <= 1024 && !strings.ContainsAny(value, "\\\x00") && !strings.HasPrefix(value, "/") && path.Clean(value) == value && value != "." && !strings.HasPrefix(value, "../")
}

func (m *Manifest) Compatible(want Compatibility) error {
	checks := []struct{ name, got, want string }{
		{"model_id", m.Metadata.ModelID, want.ModelID},
		{"model_schema_hash", m.Metadata.ModelSchemaHash, want.ModelSchemaHash},
		{"dataset_id", m.Metadata.DatasetID, want.DatasetID},
		{"container_image_digest", m.Metadata.ContainerImageDigest, want.ContainerImageDigest},
		{"framework", m.Metadata.Framework, want.Framework},
		{"framework_version", m.Metadata.FrameworkVersion, want.FrameworkVersion},
	}
	for _, check := range checks {
		if check.want == "" || check.got != check.want {
			return fmt.Errorf("incompatible %s", check.name)
		}
	}
	if len(want.AdapterVersions) == 0 {
		return errors.New("at least one adapter version is required")
	}
	for adapter, version := range want.AdapterVersions {
		if m.Metadata.AdapterVersions[adapter] != version {
			return fmt.Errorf("incompatible adapter %q", adapter)
		}
	}
	return nil
}

func (m *Manifest) Object(id string) (Object, error) {
	for _, object := range m.Objects() {
		if object.ID == id {
			return object, nil
		}
	}
	return Object{}, fmt.Errorf("object %q is not authorized by the manifest", id)
}

func (m *Manifest) Objects() []Object {
	result := make([]Object, 0)
	for _, tensor := range m.Tensors {
		for _, chunk := range tensor.Chunks {
			result = append(result, Object{ID: chunk.ChunkID, Path: chunk.StoragePath, Offset: chunk.ByteOffset, Length: chunk.ByteLength, SHA256: chunk.SHA256, WriterRank: chunk.WriterRank})
		}
	}
	result = append(result, Object{ID: "optimizer_metadata", Path: m.State.OptimizerMetadata.StoragePath, Length: m.State.OptimizerMetadata.ByteLength, SHA256: m.State.OptimizerMetadata.SHA256, WriterRank: 0})
	for rank, artifact := range m.State.RNG {
		value, _ := strconv.Atoi(rank)
		result = append(result, Object{ID: "rng_" + rank, Path: artifact.StoragePath, Length: artifact.ByteLength, SHA256: artifact.SHA256, WriterRank: value})
	}
	return result
}

func product(shape []uint64) (uint64, error) {
	result := uint64(1)
	for _, dimension := range shape {
		if dimension == 0 || result > math.MaxUint64/dimension {
			return 0, errors.New("shape is empty or overflows")
		}
		result *= dimension
	}
	return result, nil
}

func slicesOverlap(a, b [][2]uint64) bool {
	for i := range a {
		if a[i][1] <= b[i][0] || b[i][1] <= a[i][0] {
			return false
		}
	}
	return true
}

func dtypeSize(dtype string) (uint64, bool) {
	sizes := map[string]uint64{"float16": 2, "bfloat16": 2, "float32": 4, "float64": 8, "int8": 1, "uint8": 1, "int16": 2, "int32": 4, "int64": 8, "bool": 1}
	value, ok := sizes[dtype]
	return value, ok
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
func finiteNonNegative(value float64) bool {
	return value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
