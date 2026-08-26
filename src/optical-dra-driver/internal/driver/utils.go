package driver

import (
	"encoding/json"
	"os"
	"path/filepath"

	resourceapi "k8s.io/api/resource/v1"
)

func atomicJSON(path string, value any, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	defer temporary.Close()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

func intAttribute(value int64) (attribute resourceapi.DeviceAttribute) {
	return resourceapi.DeviceAttribute{IntValue: &value}
}

func stringAttribute(value string) (attribute resourceapi.DeviceAttribute) {
	return resourceapi.DeviceAttribute{StringValue: &value}
}
