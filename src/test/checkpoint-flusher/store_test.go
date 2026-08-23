package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	checkpointapi "github.com/varrahan/hetero-cluster-orchestrater/src/shared/checkpoint"
)

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (fake *fakeS3) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if request.URL.Query().Get("list-type") == "2" {
		fake.list(response, request)
		return
	}
	key := strings.TrimPrefix(request.URL.Path, "/bucket/")
	data, exists := fake.objects[key]
	switch request.Method {
	case http.MethodPut:
		if exists && request.Header.Get("If-None-Match") == "*" {
			s3Error(response, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			s3Error(response, http.StatusBadRequest, "InvalidRequest")
			return
		}
		fake.objects[key] = body
		response.Header().Set("ETag", fmt.Sprintf("\"%x\"", sha256.Sum256(body)))
		response.WriteHeader(http.StatusOK)
	case http.MethodHead:
		if !exists {
			s3Error(response, http.StatusNotFound, "NoSuchKey")
			return
		}
		response.Header().Set("Content-Length", strconv.Itoa(len(data)))
		response.Header().Set("ETag", fmt.Sprintf("\"%x\"", sha256.Sum256(data)))
		response.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		response.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if !exists {
			s3Error(response, http.StatusNotFound, "NoSuchKey")
			return
		}
		response.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		start, end := 0, len(data)-1
		if raw := request.Header.Get("Range"); raw != "" {
			_, _ = fmt.Sscanf(raw, "bytes=%d-%d", &start, &end)
			response.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			response.WriteHeader(http.StatusPartialContent)
		}
		response.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		_, _ = response.Write(data[start : end+1])
	default:
		s3Error(response, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (fake *fakeS3) list(response http.ResponseWriter, request *http.Request) {
	prefix := request.URL.Query().Get("prefix")
	keys := make([]string, 0)
	for key := range fake.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int    `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	result := struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Name        string    `xml:"Name"`
		Prefix      string    `xml:"Prefix"`
		KeyCount    int       `xml:"KeyCount"`
		MaxKeys     int       `xml:"MaxKeys"`
		IsTruncated bool      `xml:"IsTruncated"`
		Contents    []content `xml:"Contents"`
	}{Name: "bucket", Prefix: prefix, KeyCount: len(keys), MaxKeys: 1000}
	for _, key := range keys {
		data := fake.objects[key]
		result.Contents = append(result.Contents, content{Key: key, LastModified: time.Now().UTC().Format(time.RFC3339), ETag: fmt.Sprintf("\"%x\"", sha256.Sum256(data)), Size: len(data), StorageClass: "STANDARD"})
	}
	response.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(response).Encode(result)
}

func s3Error(response http.ResponseWriter, status int, code string) {
	response.WriteHeader(status)
	_, _ = fmt.Fprintf(response, "<Error><Code>%s</Code><Message>%s</Message></Error>", code, code)
}

func TestCommitAndImmutableConflict(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	server := httptest.NewTLSServer(fake)
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4("access", "secret", ""), Secure: true, Transport: server.Client().Transport, Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := objectStore{client: client, bucket: "bucket"}
	ctx := context.Background()
	tensor := bytes.Repeat([]byte{1}, 16)
	optimizer := []byte(`{"format_version":1,"parameter_groups":[{"parameters":["weight"],"options":{}}],"parameters":{}}`)
	rng := []byte{7}
	for _, object := range []struct {
		id, path string
		data     []byte
	}{{"weight_0", "shards/weight.bin", tensor}, {"optimizer_metadata", "optimizer_state.json", optimizer}, {"rng_0", "rng/rank_00000.bin", rng}} {
		if _, err := store.upload(ctx, "run", 1, object.path, object.id, 0, bytes.NewReader(object.data), uint64(len(object.data))); err != nil {
			t.Fatal(err)
		}
	}
	hash := func(data []byte) string { value := sha256.Sum256(data); return hex.EncodeToString(value[:]) }
	manifest := checkpointapi.Manifest{CheckpointVersion: 2, RunID: "run", GlobalStep: 1, Epoch: 0, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Metadata: checkpointapi.Metadata{TotalParameters: 4, CanonicalDType: "float32", ModelID: "model", ModelSchemaHash: strings.Repeat("0", 64), DatasetID: "data", ContainerImageDigest: "sha256:" + strings.Repeat("0", 64), Framework: "test", FrameworkVersion: "1", AdapterVersions: map[string]string{"cpu": "1"}},
		World:    checkpointapi.World{Size: 1, Ranks: []checkpointapi.Rank{{Rank: 0, Hardware: "cpu", Adapter: "cpu"}}},
		Tensors:  map[string]checkpointapi.Tensor{"weight": {Role: "model_parameter", GlobalShape: []uint64{4}, CanonicalDType: "float32", ByteOrder: "little", Layout: "C", Chunks: []checkpointapi.Chunk{{ChunkID: "weight_0", StoragePath: "shards/weight.bin", Slice: [][2]uint64{{0, 4}}, Shape: []uint64{4}, ByteLength: 16, SHA256: hash(tensor), WriterRank: 0}}}},
		State:    checkpointapi.State{OptimizerMetadata: checkpointapi.Artifact{StoragePath: "optimizer_state.json", ByteLength: uint64(len(optimizer)), SHA256: hash(optimizer)}, Scheduler: map[string]json.RawMessage{}, RNG: map[string]checkpointapi.Artifact{"0": {StoragePath: "rng/rank_00000.bin", ByteLength: 1, SHA256: hash(rng)}}, DataCursor: map[string]json.RawMessage{}},
	}
	manifestBytes, _ := json.Marshal(manifest)
	if _, err := store.commit(ctx, manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.commit(ctx, manifestBytes, &manifest); err != nil {
		t.Fatal("idempotent commit:", err)
	}
	want := checkpointapi.Compatibility{ModelID: "model", ModelSchemaHash: manifest.Metadata.ModelSchemaHash, DatasetID: "data", ContainerImageDigest: manifest.Metadata.ContainerImageDigest, Framework: "test", FrameworkVersion: "1", AdapterVersions: map[string]string{"cpu": "1"}}
	latest, err := store.latest(ctx, "run", want, nil)
	if err != nil || latest.Manifest.GlobalStep != 1 {
		t.Fatalf("latest = %#v, %v", latest, err)
	}
	if _, err := store.upload(ctx, "run", 1, "shards/weight.bin", "weight_0", 0, bytes.NewReader(bytes.Repeat([]byte{2}, 16)), 16); err == nil {
		t.Fatal("immutable conflict was accepted")
	}
}
