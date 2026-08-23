package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	checkpointapi "github.com/varrahan/hetero-cluster-orchestrater/src/shared/checkpoint"
	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/ring"
)

type engine struct{ sharedRoot string }

func main() {
	socket := "/run/gputpu-quantization/engine.sock"
	if err := os.MkdirAll(filepath.Dir(socket), 0770); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0660); err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Handler: engine{sharedRoot: "/dev/shm/ai-orch"}, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (engine engine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodPost || request.URL.Path != "/v1/conversions" {
		writeEngineError(response, http.StatusNotFound, "unknown endpoint")
		return
	}
	job, err := strconv.ParseUint(request.Header.Get("X-Slurm-Job-Id"), 10, 64)
	if err != nil || job == 0 {
		writeEngineError(response, http.StatusUnauthorized, "invalid Slurm job ID")
		return
	}
	rank, err := strconv.Atoi(request.Header.Get("X-Slurm-Proc-Id"))
	if err != nil || rank < 0 {
		writeEngineError(response, http.StatusUnauthorized, "invalid Slurm rank")
		return
	}
	var conversion conversion
	if err := checkpointapi.DecodeStrictJSON(io.LimitReader(request.Body, 64<<10), &conversion); err != nil {
		writeEngineError(response, http.StatusBadRequest, err.Error())
		return
	}
	inputLength, outputLength, err := conversion.validate()
	if err != nil {
		writeEngineError(response, http.StatusBadRequest, err.Error())
		return
	}
	inputPath, err := engine.validRingPath(conversion.InputPath, job, rank)
	if err != nil {
		writeEngineError(response, http.StatusForbidden, err.Error())
		return
	}
	outputPath, err := engine.validRingPath(conversion.OutputPath, job, rank)
	if err != nil {
		writeEngineError(response, http.StatusForbidden, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 120*time.Second)
	defer cancel()
	input, err := ring.Open(ctx, inputPath, false)
	if err != nil {
		writeEngineError(response, http.StatusBadRequest, err.Error())
		return
	}
	defer input.Close()
	output, err := ring.Open(ctx, outputPath, true)
	if err != nil {
		writeEngineError(response, http.StatusBadRequest, err.Error())
		return
	}
	defer output.Close()
	if input.Total() != inputLength || output.Total() != outputLength {
		writeEngineError(response, http.StatusBadRequest, "ring lengths do not match conversion")
		return
	}
	result, err := convert(conversion, input, output)
	if err != nil {
		writeEngineError(response, http.StatusUnprocessableEntity, err.Error())
		return
	}
	response.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(response).Encode(result)
}

func (engine engine) validRingPath(value string, job uint64, rank int) (string, error) {
	root, err := filepath.EvalSymlinks(engine.sharedRoot)
	if err != nil {
		return "", err
	}
	path, err := filepath.EvalSymlinks(value)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(root, path)
	want := string(filepath.Separator) + strconv.FormatUint(job, 10) + string(filepath.Separator) + strconv.Itoa(rank) + string(filepath.Separator)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !strings.Contains(string(filepath.Separator)+relative+string(filepath.Separator), want) {
		return "", errors.New("ring path is outside the job/rank namespace")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("ring path is not a regular file")
	}
	return path, nil
}

func writeEngineError(response http.ResponseWriter, status int, message string) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]string{"error": message})
}
