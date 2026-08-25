package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	checkpointapi "github.com/varrahan/hetero-cluster-orchestrater/src/shared/checkpoint"
	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/ring"
)

var transactionPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var streamPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type transaction struct {
	jobID   uint64
	rank    int
	dir     string
	streams map[string]string
}

type server struct {
	store        objectStore
	sharedRoot   string
	clusterUID   string
	budget       uint64
	mu           sync.Mutex
	transactions map[string]*transaction
	active       map[string]string
	runOwners    map[string]uint64
}

type createTransactionRequest struct {
	Streams []streamRequest `json:"streams"`
}
type streamRequest struct {
	Name       string `json:"name"`
	ByteLength uint64 `json:"byte_length"`
}
type createTransactionResponse struct {
	Transaction string            `json:"transaction"`
	Streams     map[string]string `json:"streams"`
	Slots       int               `json:"slots"`
	SlotBytes   uint64            `json:"slot_bytes"`
}

func (s *server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "transactions" {
		s.handleTransaction(response, request, parts)
		return
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "checkpoints" {
		s.handleCheckpoint(response, request, parts)
		return
	}
	writeError(response, http.StatusNotFound, "not_found", "unknown endpoint")
}

func (s *server) handleTransaction(response http.ResponseWriter, request *http.Request, parts []string) {
	if len(parts) != 3 || !transactionPattern.MatchString(parts[2]) {
		writeError(response, http.StatusBadRequest, "invalid_transaction", "invalid transaction identifier")
		return
	}
	job, rank, err := requestIdentity(request)
	if err != nil {
		writeError(response, http.StatusUnauthorized, "invalid_identity", err.Error())
		return
	}
	switch request.Method {
	case http.MethodPost:
		var body createTransactionRequest
		if err := checkpointapi.DecodeStrictJSON(io.LimitReader(request.Body, (64<<10)+1), &body); err != nil {
			writeError(response, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		result, err := s.createTransaction(parts[2], job, rank, body)
		if err != nil {
			writeError(response, http.StatusConflict, "transaction_rejected", err.Error())
			return
		}
		writeJSON(response, http.StatusCreated, result)
	case http.MethodDelete:
		if err := s.deleteTransaction(parts[2], job, rank); err != nil {
			writeError(response, http.StatusNotFound, "transaction_not_found", err.Error())
			return
		}
		writeJSON(response, http.StatusOK, map[string]bool{"deleted": true})
	default:
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *server) handleCheckpoint(response http.ResponseWriter, request *http.Request, parts []string) {
	job, rank, err := requestIdentity(request)
	if err != nil {
		writeError(response, http.StatusUnauthorized, "invalid_identity", err.Error())
		return
	}
	run := parts[2]
	if !streamPattern.MatchString(run) {
		writeError(response, http.StatusBadRequest, "invalid_run", "invalid run identifier")
		return
	}
	if err := s.claimRun(run, job); err != nil {
		writeError(response, http.StatusForbidden, "run_owner_mismatch", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), operationTimeout)
	defer cancel()
	if len(parts) == 4 && parts[3] == "latest" && request.Method == http.MethodGet {
		var compatibility checkpointapi.Compatibility
		if err := json.Unmarshal([]byte(request.Header.Get("X-Checkpoint-Compatibility")), &compatibility); err != nil {
			writeError(response, http.StatusBadRequest, "invalid_compatibility", err.Error())
			return
		}
		var before *uint64
		if value := request.URL.Query().Get("before_step"); value != "" {
			parsed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				writeError(response, http.StatusBadRequest, "invalid_before_step", err.Error())
				return
			}
			before = &parsed
		}
		result, err := s.store.latest(ctx, run, compatibility, before)
		if err != nil {
			writeError(response, http.StatusNotFound, "checkpoint_not_found", err.Error())
			return
		}
		writeJSON(response, http.StatusOK, result)
		return
	}
	if len(parts) != 5 && len(parts) != 6 {
		writeError(response, http.StatusNotFound, "not_found", "unknown checkpoint endpoint")
		return
	}
	step, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_step", "step must be a non-negative integer")
		return
	}
	if len(parts) == 6 && parts[4] == "chunks" && streamPattern.MatchString(parts[5]) {
		s.handleChunk(ctx, response, request, run, step, parts[5], job, rank)
		return
	}
	if len(parts) == 5 && parts[4] == "commit" && request.Method == http.MethodPost {
		if rank != 0 {
			writeError(response, http.StatusForbidden, "rank_zero_required", "only rank zero may commit")
			return
		}
		data, err := io.ReadAll(io.LimitReader(request.Body, checkpointapi.MaxManifestBytes+1))
		if err != nil || len(data) > checkpointapi.MaxManifestBytes {
			writeError(response, http.StatusBadRequest, "invalid_manifest", "manifest exceeds the bounded request size")
			return
		}
		manifest, err := checkpointapi.DecodeManifest(data)
		cpuCoordinator := false
		if err == nil {
			for _, worldRank := range manifest.World.Ranks {
				if worldRank.Rank == 0 && worldRank.Hardware == "cpu" {
					cpuCoordinator = true
				}
			}
		}
		if err != nil || manifest.RunID != run || manifest.GlobalStep != step || !cpuCoordinator {
			writeError(response, http.StatusBadRequest, "invalid_manifest", errorText(err, "manifest route identity or CPU coordinator mismatch"))
			return
		}
		marker, err := s.store.commit(ctx, data, manifest, job)
		if err != nil {
			writeError(response, http.StatusConflict, "commit_rejected", err.Error())
			return
		}
		writeJSON(response, http.StatusOK, marker)
		return
	}
	writeError(response, http.StatusNotFound, "not_found", "unknown checkpoint endpoint")
}

func (s *server) handleChunk(ctx context.Context, response http.ResponseWriter, request *http.Request, run string, step uint64, id string, job uint64, rank int) {
	transactionID := request.Header.Get("X-Checkpoint-Transaction")
	streamPath, err := s.transactionStream(transactionID, request.Header.Get("X-Checkpoint-Stream"), job, rank)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_transaction", err.Error())
		return
	}
	switch request.Method {
	case http.MethodPut:
		storagePath := request.Header.Get("X-Checkpoint-Storage-Path")
		if !checkpointapi.ValidStoragePath(storagePath) {
			writeError(response, http.StatusBadRequest, "invalid_storage_path", "invalid storage path")
			return
		}
		source, err := ring.Open(ctx, streamPath, false)
		if err != nil {
			writeError(response, http.StatusBadRequest, "invalid_ring", err.Error())
			return
		}
		defer source.Close()
		receipt, err := s.store.upload(ctx, run, step, storagePath, id, rank, source, source.Total())
		if err != nil {
			writeError(response, http.StatusConflict, "upload_failed", err.Error())
			return
		}
		writeJSON(response, http.StatusOK, receipt)
	case http.MethodGet:
		committed, err := s.store.loadCommitted(ctx, run, step)
		if err != nil {
			writeError(response, http.StatusNotFound, "checkpoint_not_found", err.Error())
			return
		}
		object, err := committed.Manifest.Object(id)
		if err != nil {
			writeError(response, http.StatusForbidden, "object_not_authorized", err.Error())
			return
		}
		if object.Length == 0 {
			writeError(response, http.StatusBadRequest, "empty_object", "zero-length objects do not use rings")
			return
		}
		target, err := ring.Open(ctx, streamPath, true)
		if err != nil || target.Total() != object.Length {
			writeError(response, http.StatusBadRequest, "invalid_ring", errorText(err, "ring length does not match object"))
			return
		}
		defer target.Close()
		receipt, err := s.store.restore(ctx, run, step, object, target)
		if err != nil {
			writeError(response, http.StatusUnprocessableEntity, "restore_failed", err.Error())
			return
		}
		writeJSON(response, http.StatusOK, receipt)
	default:
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *server) createTransaction(id string, job uint64, rank int, request createTransactionRequest) (createTransactionResponse, error) {
	if len(request.Streams) < 1 || len(request.Streams) > 2 {
		return createTransactionResponse{}, errors.New("a transaction requires one or two streams")
	}
	key := fmt.Sprintf("%d/%d", job, rank)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transactions[id]; exists {
		return createTransactionResponse{}, errors.New("transaction already exists")
	}
	if current := s.active[key]; current != "" {
		return createTransactionResponse{}, fmt.Errorf("transaction %s is already active for this rank", current)
	}
	perStream := s.budget / uint64(len(request.Streams))
	if perStream <= ring.HeaderSize+ring.DefaultSlots*ring.MinSlotSize {
		return createTransactionResponse{}, errors.New("shared-memory budget is too small")
	}
	slotBytes := (perStream - ring.HeaderSize) / ring.DefaultSlots
	if slotBytes > ring.MaxSlotSize {
		slotBytes = ring.MaxSlotSize
	}
	slotBytes -= slotBytes % 4096
	directory := filepath.Join(s.sharedRoot, s.clusterUID, strconv.FormatUint(job, 10), strconv.Itoa(rank), id)
	if err := os.MkdirAll(directory, 0770); err != nil {
		return createTransactionResponse{}, err
	}
	streams := map[string]string{}
	for _, stream := range request.Streams {
		if !streamPattern.MatchString(stream.Name) || stream.ByteLength == 0 || stream.ByteLength > ring.MaxStreamLength {
			_ = os.RemoveAll(directory)
			return createTransactionResponse{}, errors.New("invalid stream name or length")
		}
		if _, duplicate := streams[stream.Name]; duplicate {
			_ = os.RemoveAll(directory)
			return createTransactionResponse{}, errors.New("duplicate stream name")
		}
		path := filepath.Join(directory, stream.Name+".ring")
		if err := ring.Initialize(path, ring.DefaultSlots, slotBytes, stream.ByteLength); err != nil {
			_ = os.RemoveAll(directory)
			return createTransactionResponse{}, err
		}
		streams[stream.Name] = path
	}
	txn := &transaction{jobID: job, rank: rank, dir: directory, streams: streams}
	s.transactions[id] = txn
	s.active[key] = id
	return createTransactionResponse{Transaction: id, Streams: streams, Slots: ring.DefaultSlots, SlotBytes: slotBytes}, nil
}

func (s *server) deleteTransaction(id string, job uint64, rank int) error {
	s.mu.Lock()
	txn := s.transactions[id]
	if txn == nil || txn.jobID != job || txn.rank != rank {
		s.mu.Unlock()
		return errors.New("transaction is not owned by this job and rank")
	}
	delete(s.transactions, id)
	delete(s.active, fmt.Sprintf("%d/%d", job, rank))
	s.mu.Unlock()
	return os.RemoveAll(txn.dir)
}

func (s *server) transactionStream(id, name string, job uint64, rank int) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := s.transactions[id]
	if txn == nil || txn.jobID != job || txn.rank != rank {
		return "", errors.New("transaction is not owned by this job and rank")
	}
	path := txn.streams[name]
	if path == "" {
		return "", errors.New("unknown transaction stream")
	}
	return path, nil
}

func (s *server) claimRun(run string, job uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner := s.runOwners[run]; owner != 0 && owner != job {
		return fmt.Errorf("run is owned by job %d", owner)
	}
	s.runOwners[run] = job
	return nil
}

func requestIdentity(request *http.Request) (uint64, int, error) {
	job, err := strconv.ParseUint(request.Header.Get("X-Slurm-Job-Id"), 10, 64)
	if err != nil || job == 0 {
		return 0, 0, errors.New("X-Slurm-Job-Id must be a positive integer")
	}
	rank, err := strconv.Atoi(request.Header.Get("X-Slurm-Proc-Id"))
	if err != nil || rank < 0 || rank > 65535 {
		return 0, 0, errors.New("X-Slurm-Proc-Id must be between 0 and 65535")
	}
	return job, rank, nil
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]string{"error": code, "message": message})
}
func errorText(err error, fallback string) string {
	if err != nil {
		return err.Error()
	}
	return fallback
}
