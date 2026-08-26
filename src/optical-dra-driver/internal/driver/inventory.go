package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

const (
	annotationPrefix           = "orchestration.gputpu.io/"
	opticalInventoryAnnotation = annotationPrefix + "optical-inventory-v1"
	opticalInventoryHash       = annotationPrefix + "optical-inventory-hash"
	opticalInventoryBootID     = annotationPrefix + "optical-inventory-boot-id"
	opticalTopologyAnnotation  = annotationPrefix + "optical-topology-v1"

	kindSwitch       = "switch"
	kindCPOPhotonic  = "cpo-photonic"
	kindPhysicalAsic = "physical-asic"

	attributeDomain = "orchestration.optical.gputpu.io/"
)

type localDevice struct {
	Name           string
	Kind           string
	NUMA           int
	Model          string
	Vendor         string
	PartNumber     string
	FormFactor     string
	Protocol       string
	ComponentRole  string
	Management     string
	SourceID       string
	LinkID         string
	Topology       string
	Location       string
	Ports          int64
	BandwidthGbps  int64
	WavelengthNM   int64
	FullDuplex     bool
	Lanes          int64
	ReachMeters    int64
	OutputPowerDBm int64
}

type inventory struct {
	nodeName string
	devices  map[string]localDevice
}

type opticalTopology struct {
	Version int             `json:"version"`
	Devices []opticalDevice `json:"devices"`
}

type opticalDevice struct {
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	NUMA           int    `json:"numaNode"`
	Model          string `json:"model"`
	Vendor         string `json:"vendor"`
	PartNumber     string `json:"partNumber"`
	FormFactor     string `json:"formFactor"`
	Protocol       string `json:"protocol"`
	ComponentRole  string `json:"componentRole"`
	Management     string `json:"managementInterface"`
	SourceID       string `json:"sourceId"`
	LinkID         string `json:"linkId"`
	Topology       string `json:"topology"`
	Location       string `json:"location"`
	Ports          int64  `json:"ports"`
	BandwidthGbps  int64  `json:"bandwidthGbps"`
	WavelengthNM   int64  `json:"wavelengthNm"`
	FullDuplex     bool   `json:"fullDuplex"`
	Lanes          int64  `json:"lanes"`
	ReachMeters    int64  `json:"reachMeters"`
	OutputPowerDBm int64  `json:"outputPowerDbm"`
}

type opticalSummary struct {
	Version int             `json:"version"`
	Driver  string          `json:"driver"`
	BootID  string          `json:"bootID"`
	Devices []summaryDevice `json:"devices"`
}

type summaryDevice struct {
	Name           string `json:"name"`
	Kind           string `json:"kind"`
	NUMA           int    `json:"numaNode"`
	Model          string `json:"model"`
	Vendor         string `json:"vendor,omitempty"`
	PartNumber     string `json:"partNumber,omitempty"`
	FormFactor     string `json:"formFactor,omitempty"`
	Protocol       string `json:"protocol,omitempty"`
	ComponentRole  string `json:"componentRole,omitempty"`
	Management     string `json:"managementInterface,omitempty"`
	SourceID       string `json:"sourceId,omitempty"`
	LinkID         string `json:"linkId,omitempty"`
	Topology       string `json:"topology"`
	Location       string `json:"location"`
	Ports          int64  `json:"ports"`
	BandwidthGbps  int64  `json:"bandwidthGbps"`
	WavelengthNM   int64  `json:"wavelengthNm,omitempty"`
	FullDuplex     bool   `json:"fullDuplex,omitempty"`
	Lanes          int64  `json:"lanes,omitempty"`
	ReachMeters    int64  `json:"reachMeters,omitempty"`
	OutputPowerDBm int64  `json:"outputPowerDbm,omitempty"`
}

func discoverInventory(raw string, nodeName string) (*inventory, error) {
	inv := &inventory{nodeName: nodeName, devices: map[string]localDevice{}}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return inv, nil
	}
	var topology opticalTopology
	if err := json.Unmarshal([]byte(raw), &topology); err != nil {
		return nil, fmt.Errorf("parse optical topology JSON: %w", err)
	}
	if topology.Version == 0 {
		topology.Version = 1
	}
	if topology.Version != 1 {
		return nil, errors.New("optical topology annotation requires version 1")
	}
	for _, device := range topology.Devices {
		kind, err := parseKind(device.Kind)
		if err != nil {
			return nil, fmt.Errorf("optical device %q has invalid kind: %w", device.Name, err)
		}
		name := strings.TrimSpace(device.Name)
		if name == "" {
			return nil, errors.New("optical device name is required")
		}
		if _, exists := inv.devices[name]; exists {
			return nil, fmt.Errorf("duplicate optical device name %q", name)
		}
		if device.NUMA < 0 {
			return nil, fmt.Errorf("optical device %q has negative NUMA node", name)
		}
		topology := strings.TrimSpace(device.Topology)
		location := strings.TrimSpace(device.Location)
		if topology == "" {
			return nil, fmt.Errorf("optical device %q requires a topology", name)
		}
		if device.Ports < 0 || device.BandwidthGbps < 0 {
			return nil, fmt.Errorf("optical device %q has negative capacity", name)
		}
		if device.Lanes < 0 || device.ReachMeters < 0 || device.OutputPowerDBm < 0 {
			return nil, fmt.Errorf("optical device %q has negative product metadata", name)
		}
		vendor, err := normalizeOptional("vendor", device.Vendor)
		if err != nil {
			return nil, fmt.Errorf("optical device %q: %w", name, err)
		}
		formFactor, err := normalizeOptional("formFactor", device.FormFactor)
		if err != nil {
			return nil, fmt.Errorf("optical device %q: %w", name, err)
		}
		protocol, err := normalizeOptional("protocol", device.Protocol)
		if err != nil {
			return nil, fmt.Errorf("optical device %q: %w", name, err)
		}
		componentRole, err := normalizeOptional("componentRole", device.ComponentRole)
		if err != nil {
			return nil, fmt.Errorf("optical device %q: %w", name, err)
		}
		management, err := normalizeOptional("managementInterface", device.Management)
		if err != nil {
			return nil, fmt.Errorf("optical device %q: %w", name, err)
		}
		switch kind {
		case kindSwitch:
			if location == "" {
				return nil, fmt.Errorf("optical switch %q requires a location", name)
			}
		case kindCPOPhotonic:
			if device.WavelengthNM <= 0 {
				return nil, fmt.Errorf("CPO photonic device %q requires a positive wavelengthNm", name)
			}
		case kindPhysicalAsic:
			if !device.FullDuplex {
				return nil, fmt.Errorf("physical ASIC %q must be full duplex", name)
			}
		}
		inv.devices[name] = localDevice{
			Name:           name,
			Kind:           kind,
			NUMA:           device.NUMA,
			Model:          strings.TrimSpace(device.Model),
			Vendor:         vendor,
			PartNumber:     strings.TrimSpace(device.PartNumber),
			FormFactor:     formFactor,
			Protocol:       protocol,
			ComponentRole:  componentRole,
			Management:     management,
			SourceID:       strings.TrimSpace(device.SourceID),
			LinkID:         strings.TrimSpace(device.LinkID),
			Topology:       topology,
			Location:       location,
			Ports:          device.Ports,
			BandwidthGbps:  device.BandwidthGbps,
			WavelengthNM:   device.WavelengthNM,
			FullDuplex:     device.FullDuplex,
			Lanes:          device.Lanes,
			ReachMeters:    device.ReachMeters,
			OutputPowerDBm: device.OutputPowerDBm,
		}
	}
	return inv, nil
}

func parseKind(raw string) (string, error) {
	switch normalize(raw) {
	case "switch", "circuit_switch", "optical_switch", "optical_circuit_switch", "opticalcircuitswitch":
		return kindSwitch, nil
	case "cpo", "cpophotonic", "cpo_photonic", "cpo-photonic", "cpophotonicasic", "cpophotonic-silicon":
		return kindCPOPhotonic, nil
	case "asic", "physical_asic", "physical-asic", "physicalasic", "asic_transceiver", "physical_asic_adapter":
		return kindPhysicalAsic, nil
	}
	return "", fmt.Errorf("unsupported optical kind %q", raw)
}

func (inv *inventory) summary(bootID string) (string, string, string, error) {
	if bootID == "" {
		return "", "", "", errors.New("Kubernetes bootID is empty")
	}
	summary := opticalSummary{
		Version: 1,
		Driver:  driverName,
		BootID:  bootID,
	}
	for _, name := range slices.Sorted(maps.Keys(inv.devices)) {
		device := inv.devices[name]
		summary.Devices = append(summary.Devices, summaryDevice{
			Name:           device.Name,
			Kind:           device.Kind,
			NUMA:           device.NUMA,
			Model:          device.Model,
			Vendor:         device.Vendor,
			PartNumber:     device.PartNumber,
			FormFactor:     device.FormFactor,
			Protocol:       device.Protocol,
			ComponentRole:  device.ComponentRole,
			Management:     device.Management,
			SourceID:       device.SourceID,
			LinkID:         device.LinkID,
			Topology:       device.Topology,
			Location:       device.Location,
			Ports:          device.Ports,
			BandwidthGbps:  device.BandwidthGbps,
			WavelengthNM:   device.WavelengthNM,
			FullDuplex:     device.FullDuplex,
			Lanes:          device.Lanes,
			ReachMeters:    device.ReachMeters,
			OutputPowerDBm: device.OutputPowerDBm,
		})
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return "", "", "", err
	}
	identity, err := json.Marshal(summary.Devices)
	if err != nil {
		return "", "", "", err
	}
	digest := sha256.Sum256(encoded)
	return string(encoded), string(identity), hex.EncodeToString(digest[:]), nil
}

func (inv *inventory) resources() resourceslice.DriverResources {
	names := slices.Sorted(maps.Keys(inv.devices))
	devices := make([]resourceapi.Device, 0, len(names))
	for _, name := range names {
		devices = append(devices, inv.devices[name].resourceDevice())
	}
	slicesOut := make([]resourceslice.Slice, 0, (len(devices)+resourceapi.ResourceSliceMaxDevices-1)/resourceapi.ResourceSliceMaxDevices)
	for len(devices) > 0 {
		n := min(len(devices), resourceapi.ResourceSliceMaxDevices)
		slicesOut = append(slicesOut, resourceslice.Slice{Devices: slices.Clone(devices[:n])})
		devices = devices[n:]
	}
	if len(slicesOut) == 0 {
		slicesOut = []resourceslice.Slice{{}}
	}
	return resourceslice.DriverResources{Pools: map[string]resourceslice.Pool{inv.nodeName: {Slices: slicesOut}}}
}

func (device localDevice) resourceDevice() resourceapi.Device {
	attrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		attributeDomain + "kind":     stringAttribute(device.Kind),
		attributeDomain + "numaNode": intAttribute(int64(device.NUMA)),
	}
	if device.Model != "" {
		attrs[attributeDomain+"model"] = stringAttribute(device.Model)
	}
	if device.Vendor != "" {
		attrs[attributeDomain+"vendor"] = stringAttribute(device.Vendor)
	}
	if device.PartNumber != "" {
		attrs[attributeDomain+"partNumber"] = stringAttribute(device.PartNumber)
	}
	if device.FormFactor != "" {
		attrs[attributeDomain+"formFactor"] = stringAttribute(device.FormFactor)
	}
	if device.Protocol != "" {
		attrs[attributeDomain+"protocol"] = stringAttribute(device.Protocol)
	}
	if device.ComponentRole != "" {
		attrs[attributeDomain+"componentRole"] = stringAttribute(device.ComponentRole)
	}
	if device.Management != "" {
		attrs[attributeDomain+"managementInterface"] = stringAttribute(device.Management)
	}
	if device.SourceID != "" {
		attrs[attributeDomain+"sourceId"] = stringAttribute(device.SourceID)
	}
	if device.LinkID != "" {
		attrs[attributeDomain+"linkId"] = stringAttribute(device.LinkID)
	}
	if device.Topology != "" {
		attrs[attributeDomain+"topology"] = stringAttribute(device.Topology)
	}
	if device.Location != "" {
		attrs[attributeDomain+"location"] = stringAttribute(device.Location)
	}
	if device.Ports > 0 {
		attrs[attributeDomain+"ports"] = intAttribute(device.Ports)
	}
	if device.BandwidthGbps > 0 {
		attrs[attributeDomain+"bandwidthGbps"] = intAttribute(device.BandwidthGbps)
	}
	if device.WavelengthNM > 0 {
		attrs[attributeDomain+"wavelengthNm"] = intAttribute(device.WavelengthNM)
	}
	if device.Kind == kindPhysicalAsic {
		fullDuplex := device.FullDuplex
		attrs[attributeDomain+"fullDuplex"] = resourceapi.DeviceAttribute{BoolValue: &fullDuplex}
	}
	if device.Lanes > 0 {
		attrs[attributeDomain+"lanes"] = intAttribute(device.Lanes)
	}
	if device.ReachMeters > 0 {
		attrs[attributeDomain+"reachMeters"] = intAttribute(device.ReachMeters)
	}
	if device.OutputPowerDBm > 0 {
		attrs[attributeDomain+"outputPowerDbm"] = intAttribute(device.OutputPowerDBm)
	}
	return resourceapi.Device{
		Name:       device.Name,
		Attributes: attrs,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func normalize(value string) string {
	value = strings.ToLower(value)
	fields := strings.FieldsFunc(value, func(r rune) bool { return (r < 'a' || r > 'z') && (r < '0' || r > '9') })
	return strings.Join(fields, "_")
}

func normalizeOptional(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	value = normalize(value)
	if value == "" {
		return "", fmt.Errorf("%s must contain a letter or digit", field)
	}
	return value, nil
}
