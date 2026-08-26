package driver

import (
	resourceapi "k8s.io/api/resource/v1"
)

func intAttribute(value int64) (attribute resourceapi.DeviceAttribute) {
	return resourceapi.DeviceAttribute{IntValue: &value}
}

func stringAttribute(value string) (attribute resourceapi.DeviceAttribute) {
	return resourceapi.DeviceAttribute{StringValue: &value}
}
