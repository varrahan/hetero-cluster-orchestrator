package checkpoint

import (
	"bytes"
	"fmt"
)

const MaxOptimizerMetadataBytes = 4 << 20

type OptimizerState struct {
	FormatVersion   int                           `json:"format_version"`
	ParameterGroups []OptimizerParameterGroup     `json:"parameter_groups"`
	Parameters      map[string]OptimizerParameter `json:"parameters"`
}

type OptimizerParameterGroup struct {
	Parameters []string       `json:"parameters"`
	Options    map[string]any `json:"options"`
}

type OptimizerParameter struct {
	Scalars map[string]any    `json:"scalars"`
	Tensors map[string]string `json:"tensors"`
}

func DecodeOptimizerState(data []byte, manifest *Manifest) (*OptimizerState, error) {
	if len(data) == 0 || len(data) > MaxOptimizerMetadataBytes {
		return nil, fmt.Errorf("optimizer metadata length must be between 1 and %d bytes", MaxOptimizerMetadataBytes)
	}
	var state OptimizerState
	if err := DecodeStrictJSON(bytes.NewReader(data), &state); err != nil {
		return nil, fmt.Errorf("decode optimizer metadata: %w", err)
	}
	if state.FormatVersion != 1 {
		return nil, fmt.Errorf("optimizer format_version must be 1")
	}
	model := map[string]struct{}{}
	optimizer := map[string]struct{}{}
	for name, tensor := range manifest.Tensors {
		switch tensor.Role {
		case "model_parameter":
			model[name] = struct{}{}
		case "optimizer_tensor":
			optimizer[name] = struct{}{}
		}
	}
	for _, group := range state.ParameterGroups {
		if len(group.Parameters) == 0 || group.Options == nil {
			return nil, fmt.Errorf("optimizer parameter groups require parameters and options")
		}
		for _, name := range group.Parameters {
			if _, ok := model[name]; !ok {
				return nil, fmt.Errorf("optimizer group references unknown model parameter %q", name)
			}
		}
	}
	for name, parameter := range state.Parameters {
		if _, ok := model[name]; !ok {
			return nil, fmt.Errorf("optimizer state references unknown model parameter %q", name)
		}
		for slot, tensor := range parameter.Tensors {
			if slot == "" {
				return nil, fmt.Errorf("optimizer parameter %q has an empty slot name", name)
			}
			if _, ok := optimizer[tensor]; !ok {
				return nil, fmt.Errorf("optimizer slot %q references unknown tensor %q", slot, tensor)
			}
		}
	}
	return &state, nil
}
