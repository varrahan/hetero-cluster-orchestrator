package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	checkpointapi "github.com/varrahan/hetero-cluster-orchestrater/src/shared/checkpoint"
)

const operationTimeout = 120 * time.Second

type objectStore struct {
	client     *minio.Client
	bucket     string
	clusterUID string
}

type latestResult struct {
	Marker   checkpointapi.CommitMarker `json:"marker"`
	Manifest *checkpointapi.Manifest    `json:"manifest"`
}

func (s objectStore) upload(ctx context.Context, run string, step uint64, storagePath, objectID string, rank int, source io.Reader, size uint64) (checkpointapi.Receipt, error) {
	key := objectKey(run, step, storagePath)
	if existing, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err == nil {
		return s.compareExisting(ctx, key, existing, storagePath, source, size)
	} else if !notFound(err) {
		return checkpointapi.Receipt{}, fmt.Errorf("stat immutable object: %w", err)
	}
	hash := sha256.New()
	reader := io.TeeReader(source, hash)
	options := minio.PutObjectOptions{ContentType: "application/octet-stream", UserMetadata: map[string]string{
		"job-id": "local", "writer-rank": strconv.Itoa(rank), "object-id": objectID,
	}}
	options.SetMatchETagExcept("*")
	info, err := s.client.PutObject(ctx, s.bucket, key, reader, int64(size), options)
	if err != nil {
		_, _ = io.Copy(io.Discard, source)
		if preconditionFailed(err) {
			existing, statErr := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
			if statErr == nil {
				return s.compareExistingHash(ctx, key, existing, storagePath, size, hex.EncodeToString(hash.Sum(nil)))
			}
		}
		return checkpointapi.Receipt{}, fmt.Errorf("upload immutable object: %w", err)
	}
	return checkpointapi.Receipt{StoragePath: storagePath, ByteLength: size, SHA256: hex.EncodeToString(hash.Sum(nil)), ETag: strings.Trim(info.ETag, `"`)}, nil
}

func (s objectStore) compareExisting(ctx context.Context, key string, info minio.ObjectInfo, storagePath string, source io.Reader, size uint64) (checkpointapi.Receipt, error) {
	hash := sha256.New()
	written, err := io.Copy(hash, source)
	if err != nil || uint64(written) != size {
		return checkpointapi.Receipt{}, fmt.Errorf("read retry body: copied %d of %d bytes: %w", written, size, err)
	}
	return s.compareExistingHash(ctx, key, info, storagePath, size, hex.EncodeToString(hash.Sum(nil)))
}

func (s objectStore) compareExistingHash(ctx context.Context, key string, info minio.ObjectInfo, storagePath string, size uint64, wantedHash string) (checkpointapi.Receipt, error) {
	if info.Size != int64(size) {
		return checkpointapi.Receipt{}, fmt.Errorf("immutable object %q already exists with a different length", storagePath)
	}
	actual, err := s.hashRange(ctx, key, 0, size)
	if err != nil {
		return checkpointapi.Receipt{}, err
	}
	if actual != wantedHash {
		return checkpointapi.Receipt{}, fmt.Errorf("immutable object %q already exists with different bytes", storagePath)
	}
	return checkpointapi.Receipt{StoragePath: storagePath, ByteLength: size, SHA256: wantedHash, ETag: strings.Trim(info.ETag, `"`)}, nil
}

func (s objectStore) commit(ctx context.Context, manifestBytes []byte, manifest *checkpointapi.Manifest, jobID uint64) (checkpointapi.CommitMarker, error) {
	optimizer, err := s.readArtifact(ctx, manifest.RunID, manifest.GlobalStep, manifest.State.OptimizerMetadata)
	if err != nil {
		return checkpointapi.CommitMarker{}, fmt.Errorf("optimizer metadata: %w", err)
	}
	if _, err := checkpointapi.DecodeOptimizerState(optimizer, manifest); err != nil {
		return checkpointapi.CommitMarker{}, err
	}
	for _, object := range manifest.Objects() {
		if err := s.verifyObject(ctx, manifest.RunID, manifest.GlobalStep, object); err != nil {
			return checkpointapi.CommitMarker{}, err
		}
	}
	manifestHash := sha256.Sum256(manifestBytes)
	manifestDigest := hex.EncodeToString(manifestHash[:])
	manifestKey := objectKey(manifest.RunID, manifest.GlobalStep, "manifest.json")
	if err := s.putImmutable(ctx, manifestKey, manifestBytes, "application/json"); err != nil {
		return checkpointapi.CommitMarker{}, fmt.Errorf("store manifest: %w", err)
	}
	marker := checkpointapi.CommitMarker{CheckpointVersion: checkpointapi.Version, GlobalStep: manifest.GlobalStep, ManifestSHA256: manifestDigest, CommittedAt: time.Now().UTC().Format(time.RFC3339Nano), SlurmJobID: jobID}
	markerBytes, _ := json.Marshal(marker)
	markerKey := fmt.Sprintf("checkpoints/%s/step_%08d.complete", manifest.RunID, manifest.GlobalStep)
	if err := s.putImmutableMarker(ctx, markerKey, markerBytes, marker); err != nil {
		return checkpointapi.CommitMarker{}, err
	}
	return marker, nil
}

func (s objectStore) latest(ctx context.Context, run string, want checkpointapi.Compatibility, before *uint64) (latestResult, error) {
	prefix := "checkpoints/" + run + "/"
	steps := make([]uint64, 0)
	for item := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if item.Err != nil {
			return latestResult{}, item.Err
		}
		name, ok := strings.CutPrefix(item.Key, prefix+"step_")
		if !ok {
			continue
		}
		name, ok = strings.CutSuffix(name, ".complete")
		if !ok {
			continue
		}
		step, err := strconv.ParseUint(name, 10, 64)
		if err != nil || before != nil && step >= *before {
			continue
		}
		steps = append(steps, step)
		if len(steps) > 10000 {
			return latestResult{}, errors.New("checkpoint commit listing exceeds 10000 entries")
		}
	}
	slices.Sort(steps)
	slices.Reverse(steps)
	for _, step := range steps {
		result, err := s.loadCommitted(ctx, run, step)
		if err != nil || result.Manifest.Compatible(want) != nil {
			continue
		}
		valid := true
		for _, object := range result.Manifest.Objects() {
			if s.verifyObject(ctx, run, step, object) != nil {
				valid = false
				break
			}
		}
		if valid {
			return result, nil
		}
	}
	return latestResult{}, errors.New("no compatible committed checkpoint")
}

func (s objectStore) loadCommitted(ctx context.Context, run string, step uint64) (latestResult, error) {
	markerKey := fmt.Sprintf("checkpoints/%s/step_%08d.complete", run, step)
	markerBytes, err := s.readBounded(ctx, markerKey, 64<<10)
	if err != nil {
		return latestResult{}, err
	}
	var marker checkpointapi.CommitMarker
	if err := checkpointapi.DecodeStrictJSON(bytes.NewReader(markerBytes), &marker); err != nil || marker.CheckpointVersion != checkpointapi.Version || marker.GlobalStep != step || len(marker.ManifestSHA256) != 64 {
		return latestResult{}, errors.New("invalid commit marker")
	}
	if _, err := time.Parse(time.RFC3339Nano, marker.CommittedAt); err != nil {
		return latestResult{}, errors.New("invalid commit timestamp")
	}
	manifestBytes, err := s.readBounded(ctx, objectKey(run, step, "manifest.json"), checkpointapi.MaxManifestBytes)
	if err != nil {
		return latestResult{}, err
	}
	hash := sha256.Sum256(manifestBytes)
	if hex.EncodeToString(hash[:]) != marker.ManifestSHA256 {
		return latestResult{}, errors.New("manifest hash does not match commit marker")
	}
	manifest, err := checkpointapi.DecodeManifest(manifestBytes)
	if err != nil || manifest.RunID != run || manifest.GlobalStep != step {
		return latestResult{}, errors.New("manifest route identity mismatch")
	}
	return latestResult{Marker: marker, Manifest: manifest}, nil
}

func (s objectStore) restore(ctx context.Context, run string, step uint64, object checkpointapi.Object, target io.Writer) (checkpointapi.Receipt, error) {
	key := objectKey(run, step, object.Path)
	options := minio.GetObjectOptions{}
	if err := options.SetRange(int64(object.Offset), int64(object.Offset+object.Length-1)); err != nil {
		return checkpointapi.Receipt{}, err
	}
	remote, err := s.client.GetObject(ctx, s.bucket, key, options)
	if err != nil {
		return checkpointapi.Receipt{}, err
	}
	defer remote.Close()
	hash := sha256.New()
	written, err := io.CopyN(io.MultiWriter(target, hash), remote, int64(object.Length))
	if err != nil || written != int64(object.Length) {
		return checkpointapi.Receipt{}, fmt.Errorf("restore %q: copied %d bytes: %w", object.ID, written, err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if digest != object.SHA256 {
		return checkpointapi.Receipt{}, fmt.Errorf("restore %q hash mismatch", object.ID)
	}
	return checkpointapi.Receipt{StoragePath: object.Path, ByteLength: object.Length, SHA256: digest}, nil
}

func (s objectStore) verifyObject(ctx context.Context, run string, step uint64, object checkpointapi.Object) error {
	key := objectKey(run, step, object.Path)
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return fmt.Errorf("stat %q: %w", object.Path, err)
	}
	if object.Offset > uint64(info.Size) || object.Length > uint64(info.Size)-object.Offset {
		return fmt.Errorf("object %q byte range exceeds stored length", object.Path)
	}
	digest, err := s.hashRange(ctx, key, object.Offset, object.Length)
	if err != nil {
		return err
	}
	if digest != object.SHA256 {
		return fmt.Errorf("object %q hash mismatch", object.Path)
	}
	return nil
}

func (s objectStore) hashRange(ctx context.Context, key string, offset, length uint64) (string, error) {
	if length == 0 {
		return "", errors.New("cannot hash an empty object range")
	}
	options := minio.GetObjectOptions{}
	if err := options.SetRange(int64(offset), int64(offset+length-1)); err != nil {
		return "", err
	}
	object, err := s.client.GetObject(ctx, s.bucket, key, options)
	if err != nil {
		return "", err
	}
	defer object.Close()
	hash := sha256.New()
	written, err := io.CopyN(hash, object, int64(length))
	if err != nil || uint64(written) != length {
		return "", fmt.Errorf("hash object %q: copied %d of %d bytes: %w", key, written, length, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s objectStore) readArtifact(ctx context.Context, run string, step uint64, artifact checkpointapi.Artifact) ([]byte, error) {
	if artifact.ByteLength > checkpointapi.MaxOptimizerMetadataBytes {
		return nil, errors.New("artifact is too large")
	}
	key := objectKey(run, step, artifact.StoragePath)
	data, err := s.readBounded(ctx, key, int(artifact.ByteLength))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != artifact.ByteLength {
		return nil, fmt.Errorf("object %q length mismatch", key)
	}
	return data, nil
}

func (s objectStore) readBounded(ctx context.Context, key string, limit int) ([]byte, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("object %q exceeds %d bytes", key, limit)
	}
	return data, nil
}

func (s objectStore) putImmutable(ctx context.Context, key string, data []byte, contentType string) error {
	if _, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err == nil {
		existing, readErr := s.readBounded(ctx, key, len(data))
		if readErr == nil && bytes.Equal(existing, data) {
			return nil
		}
		return fmt.Errorf("immutable object %q conflicts", key)
	} else if !notFound(err) {
		return err
	}
	options := minio.PutObjectOptions{ContentType: contentType}
	if s.clusterUID != "" {
		options.UserMetadata = map[string]string{"cluster-uid": s.clusterUID}
	}
	options.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), options)
	if preconditionFailed(err) {
		existing, readErr := s.readBounded(ctx, key, len(data))
		if readErr == nil && bytes.Equal(existing, data) {
			return nil
		}
	}
	return err
}

func (s objectStore) putImmutableMarker(ctx context.Context, key string, data []byte, marker checkpointapi.CommitMarker) error {
	err := s.putImmutable(ctx, key, data, "application/json")
	if err == nil {
		return nil
	}
	existing, readErr := s.readBounded(ctx, key, 64<<10)
	if readErr != nil {
		return fmt.Errorf("create commit marker: %w", err)
	}
	var old checkpointapi.CommitMarker
	if checkpointapi.DecodeStrictJSON(bytes.NewReader(existing), &old) == nil && old.CheckpointVersion == marker.CheckpointVersion && old.GlobalStep == marker.GlobalStep && old.ManifestSHA256 == marker.ManifestSHA256 {
		return nil
	}
	return fmt.Errorf("commit marker conflicts: %w", err)
}

func objectKey(run string, step uint64, storagePath string) string {
	return fmt.Sprintf("checkpoints/%s/step_%08d/%s", run, step, storagePath)
}

func notFound(err error) bool {
	response := minio.ToErrorResponse(err)
	return response.Code == "NoSuchKey" || response.Code == "NoSuchObject" || response.StatusCode == 404
}

func preconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	response := minio.ToErrorResponse(err)
	return response.Code == "PreconditionFailed" || response.StatusCode == 412
}
