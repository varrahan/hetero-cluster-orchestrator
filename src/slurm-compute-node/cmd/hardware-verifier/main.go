package main

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
)

type openTPUProfile struct {
	Profile    string `json:"profile"`
	MatrixSize int64  `json:"matrixSize"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hardware-verifier:", err)
		os.Exit(1)
	}
}

func run() error {
	if !cpuCheck() {
		return errors.New("deterministic CPU arithmetic failed")
	}
	memoryBytes, err := strconv.ParseInt(cmp.Or(os.Getenv("MEMORY_CHECK_BYTES"), "67108864"), 10, 64)
	if err != nil || memoryBytes < 1<<20 || memoryBytes > 1<<30 {
		return errors.New("MEMORY_CHECK_BYTES must be between 1MiB and 1GiB")
	}
	if err := memoryCheck(int(memoryBytes)); err != nil {
		return err
	}
	if err := gpuCheck(os.Getenv("EXPECTED_GPU_UUIDS")); err != nil {
		return err
	}
	var profiles []openTPUProfile
	if raw := os.Getenv("OPENTPU_VERIFY_PROFILES"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &profiles); err != nil {
			return fmt.Errorf("decode OpenTPU verifier profiles: %w", err)
		}
	}
	for _, profile := range profiles {
		if profile.Profile == "" || (profile.MatrixSize != 8 && profile.MatrixSize != 16) {
			return fmt.Errorf("invalid OpenTPU profile %q", profile.Profile)
		}
		command := exec.Command("python3", "/usr/local/bin/opentpu-verify.py")
		command.Env = append(os.Environ(), "OPENTPU_DEVICES=verification", "OPENTPU_MATRIX_SIZE="+strconv.FormatInt(profile.MatrixSize, 10))
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("OpenTPU profile %s verification failed: %w: %s", profile.Profile, err, output)
		}
	}
	return nil
}

func cpuCheck() bool {
	const size = 32
	var checksum uint64
	for row := 0; row < size; row++ {
		for column := 0; column < size; column++ {
			value := int64(0)
			for k := 0; k < size; k++ {
				value += int64((row+k)%17-8) * int64((k*3+column)%19-9)
			}
			checksum = checksum*1099511628211 + uint64(value)
		}
	}
	return checksum == 11265742251464690536
}

func memoryCheck(size int) error {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*131 + 17) % 251)
	}
	for i, value := range data {
		if value != byte((i*131+17)%251) {
			return fmt.Errorf("memory checksum mismatch at byte %d", i)
		}
	}
	return nil
}

func gpuCheck(raw string) error {
	expected := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' })
	if len(expected) == 0 {
		return nil
	}
	slices.Sort(expected)
	output, err := exec.Command("nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader").Output()
	if err != nil {
		return fmt.Errorf("query NVIDIA UUIDs: %w", err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(output))).ReadAll()
	if err != nil {
		return fmt.Errorf("parse NVIDIA UUIDs: %w", err)
	}
	actual := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 1 {
			actual = append(actual, strings.TrimSpace(row[0]))
		}
	}
	slices.Sort(actual)
	if !slices.Equal(actual, expected) {
		return fmt.Errorf("visible NVIDIA UUIDs %v do not match expected %v", actual, expected)
	}
	for _, uuid := range expected {
		command := exec.Command("python3", "/usr/local/bin/cuda-smoke.py")
		command.Env = append(os.Environ(), "CUDA_VISIBLE_DEVICES="+uuid)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("CUDA verification failed for %s: %w: %s", uuid, err, output)
		}
	}
	return nil
}
