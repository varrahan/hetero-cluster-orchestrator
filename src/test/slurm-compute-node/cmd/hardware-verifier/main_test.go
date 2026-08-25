package main

import "testing"

func TestCPUAndMemoryChecks(t *testing.T) {
	if !cpuCheck() {
		t.Fatal("CPU check failed")
	}
	if err := memoryCheck(1 << 20); err != nil {
		t.Fatal(err)
	}
}

func TestVerifierWithoutAccelerators(t *testing.T) {
	t.Setenv("MEMORY_CHECK_BYTES", "1048576")
	t.Setenv("EXPECTED_GPU_UUIDS", "")
	t.Setenv("OPENTPU_VERIFY_PROFILES", "[]")
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
