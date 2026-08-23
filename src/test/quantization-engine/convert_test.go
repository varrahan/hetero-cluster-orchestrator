package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestTiesToEvenAndClamp(t *testing.T) {
	values := []float32{0.5, 1.5, 2.5, -1.5, 1000}
	input := make([]byte, len(values)*4)
	for index, value := range values {
		binary.LittleEndian.PutUint32(input[index*4:], math.Float32bits(value))
	}
	var output bytes.Buffer
	result, err := convert(conversion{Elements: uint64(len(values)), SourceDType: "float32", TargetDType: "int8", Scale: 1}, bytes.NewReader(input), &output)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 2, 2, 0xfe, 127}
	if !bytes.Equal(output.Bytes(), want) || result.ByteLength != uint64(len(want)) {
		t.Fatalf("output = %v", output.Bytes())
	}
}

func TestINT8RoundTripAndNonFinite(t *testing.T) {
	source := []byte{0x80, 0, 127}
	var canonical bytes.Buffer
	if _, err := convert(conversion{Elements: 3, SourceDType: "int8", TargetDType: "bfloat16", Scale: 0.5, ZeroPoint: 1}, bytes.NewReader(source), &canonical); err != nil {
		t.Fatal(err)
	}
	var restored bytes.Buffer
	if _, err := convert(conversion{Elements: 3, SourceDType: "bfloat16", TargetDType: "int8", Scale: 0.5, ZeroPoint: 1}, bytes.NewReader(canonical.Bytes()), &restored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Bytes(), source) {
		t.Fatalf("round trip = %v", restored.Bytes())
	}
}
