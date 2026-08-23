package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
)

type conversion struct {
	InputPath   string  `json:"input_path"`
	OutputPath  string  `json:"output_path"`
	Elements    uint64  `json:"elements"`
	SourceDType string  `json:"source_dtype"`
	TargetDType string  `json:"target_dtype"`
	Scale       float64 `json:"scale"`
	ZeroPoint   int     `json:"zero_point"`
}

type conversionResult struct {
	Elements   uint64 `json:"elements"`
	ByteLength uint64 `json:"byte_length"`
	SHA256     string `json:"sha256"`
}

func (request conversion) validate() (uint64, uint64, error) {
	if request.Elements == 0 || request.Elements > 1<<40 || request.Scale <= 0 || math.IsNaN(request.Scale) || math.IsInf(request.Scale, 0) || request.ZeroPoint < -128 || request.ZeroPoint > 127 {
		return 0, 0, errors.New("invalid element count or affine quantization")
	}
	sourceSize, sourceOK := numericSize(request.SourceDType)
	targetSize, targetOK := numericSize(request.TargetDType)
	validPair := request.SourceDType == "int8" && (request.TargetDType == "float32" || request.TargetDType == "bfloat16") ||
		request.TargetDType == "int8" && (request.SourceDType == "float32" || request.SourceDType == "bfloat16")
	if !sourceOK || !targetOK || !validPair || request.Elements > math.MaxUint64/sourceSize || request.Elements > math.MaxUint64/targetSize {
		return 0, 0, errors.New("conversion must be int8 to or from float32/bfloat16")
	}
	return request.Elements * sourceSize, request.Elements * targetSize, nil
}

func convert(request conversion, source io.Reader, target io.Writer) (conversionResult, error) {
	_, outputLength, err := request.validate()
	if err != nil {
		return conversionResult{}, err
	}
	hash := sha256.New()
	output := io.MultiWriter(target, hash)
	const elementsPerBlock = 64 << 10
	remaining := request.Elements
	for remaining > 0 {
		count := min(remaining, elementsPerBlock)
		sourceSize, _ := numericSize(request.SourceDType)
		input := make([]byte, count*sourceSize)
		if _, err := io.ReadFull(source, input); err != nil {
			return conversionResult{}, fmt.Errorf("read conversion input: %w", err)
		}
		converted, err := convertBlock(request, input, count)
		if err != nil {
			return conversionResult{}, err
		}
		if _, err := output.Write(converted); err != nil {
			return conversionResult{}, fmt.Errorf("write conversion output: %w", err)
		}
		remaining -= count
	}
	return conversionResult{Elements: request.Elements, ByteLength: outputLength, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func convertBlock(request conversion, input []byte, elements uint64) ([]byte, error) {
	if request.SourceDType == "int8" {
		targetSize, _ := numericSize(request.TargetDType)
		output := make([]byte, elements*targetSize)
		for index := uint64(0); index < elements; index++ {
			value := float32((float64(int8(input[index])) - float64(request.ZeroPoint)) * request.Scale)
			if request.TargetDType == "float32" {
				binary.LittleEndian.PutUint32(output[index*4:index*4+4], math.Float32bits(value))
			} else {
				binary.LittleEndian.PutUint16(output[index*2:index*2+2], float32ToBF16(value))
			}
		}
		return output, nil
	}
	output := make([]byte, elements)
	for index := uint64(0); index < elements; index++ {
		var value float32
		if request.SourceDType == "float32" {
			value = math.Float32frombits(binary.LittleEndian.Uint32(input[index*4 : index*4+4]))
		} else {
			value = bf16ToFloat32(binary.LittleEndian.Uint16(input[index*2 : index*2+2]))
		}
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, fmt.Errorf("element %d is not finite", index)
		}
		quantized := math.RoundToEven(float64(value)/request.Scale) + float64(request.ZeroPoint)
		quantized = min(max(quantized, -128), 127)
		output[index] = byte(int8(quantized))
	}
	return output, nil
}

func float32ToBF16(value float32) uint16 {
	bits := math.Float32bits(value)
	bits += 0x7fff + (bits>>16)&1
	return uint16(bits >> 16)
}

func bf16ToFloat32(value uint16) float32 { return math.Float32frombits(uint32(value) << 16) }

func numericSize(dtype string) (uint64, bool) {
	switch dtype {
	case "int8":
		return 1, true
	case "bfloat16":
		return 2, true
	case "float32":
		return 4, true
	}
	return 0, false
}
