package driver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

type preparedClaim struct {
	Version   int       `json:"version"`
	UID       types.UID `json:"uid"`
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Hash      string    `json:"allocationHash"`
	Inventory string    `json:"inventoryHash"`
	Devices   []string  `json:"devices"`
	CPUs      []int     `json:"cpus"`
	NUMA      int       `json:"numaNode"`
}

type nodePlugin struct {
	mu       sync.RWMutex
	dir      string
	inv      *inventory
	prepared map[types.UID]preparedClaim
}

func newNodePlugin(dir string, inv *inventory) (*nodePlugin, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create driver state directory: %w", err)
	}
	plugin := &nodePlugin{dir: dir, inv: inv, prepared: map[types.UID]preparedClaim{}}
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	held := map[string]types.UID{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read prepared state: %w", err)
		}
		var record preparedClaim
		if err := json.Unmarshal(data, &record); err != nil || record.Version != 2 || record.UID == "" {
			return nil, fmt.Errorf("invalid prepared state %q", path)
		}
		for _, name := range record.Devices {
			if _, exists := inv.devices[name]; !exists {
				return nil, fmt.Errorf("prepared device %q is absent from current inventory", name)
			}
			if owner, exists := held[name]; exists && owner != record.UID {
				return nil, fmt.Errorf("prepared device %q is held by claims %q and %q", name, owner, record.UID)
			}
			held[name] = record.UID
		}
		if inventoryHash(inv, record.Devices) != record.Inventory {
			return nil, fmt.Errorf("prepared claim %q inventory changed", record.UID)
		}
		plugin.prepared[record.UID] = record
	}
	return plugin, nil
}

func (p *nodePlugin) recover(ctx context.Context, client kubernetes.Interface) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for uid, record := range p.prepared {
		claim, err := client.ResourceV1().ResourceClaims(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || err == nil && claim.UID != uid {
			if err := os.Remove(p.path(uid)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			delete(p.prepared, uid)
			continue
		}
		if err != nil {
			return fmt.Errorf("recover claim %s/%s: %w", record.Namespace, record.Name, err)
		}
		if claim.Status.Allocation == nil {
			return fmt.Errorf("prepared claim %s/%s is no longer allocated", record.Namespace, record.Name)
		}
		if allocationHash(claim.Status.Allocation.Devices.Results) != record.Hash {
			return fmt.Errorf("prepared claim %s/%s allocation changed", record.Namespace, record.Name)
		}
	}
	return nil
}

func (p *nodePlugin) PrepareResourceClaims(_ context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[types.UID]kubeletplugin.PrepareResult, len(claims))
	for _, claim := range claims {
		prepared, devices, err := p.prepare(claim)
		if err == nil {
			err = atomicJSON(p.path(claim.UID), prepared, 0640)
		}
		if err == nil {
			p.prepared[claim.UID] = prepared
		}
		result[claim.UID] = kubeletplugin.PrepareResult{Err: err, Devices: devices}
	}
	return result, nil
}

func (p *nodePlugin) prepare(claim *resourceapi.ResourceClaim) (preparedClaim, []kubeletplugin.Device, error) {
	if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
		return preparedClaim{}, nil, errors.New("claim has no allocated devices")
	}
	results := claim.Status.Allocation.Devices.Results
	hash := allocationHash(results)
	if old, exists := p.prepared[claim.UID]; exists {
		if old.Hash != hash || old.Inventory != inventoryHash(p.inv, old.Devices) || old.Namespace != claim.Namespace || old.Name != claim.Name {
			return preparedClaim{}, nil, errors.New("claim allocation changed after preparation")
		}
		return old, p.cdiDevices(results), nil
	}

	record := preparedClaim{Version: 2, UID: claim.UID, Namespace: claim.Namespace, Name: claim.Name, Hash: hash, NUMA: -1}
	seen := map[string]struct{}{}
	for _, allocation := range results {
		if allocation.Driver != driverName || allocation.Pool != p.inv.nodeName {
			return preparedClaim{}, nil, fmt.Errorf("allocation %s/%s belongs to another driver or node", allocation.Pool, allocation.Device)
		}
		device, exists := p.inv.devices[allocation.Device]
		if !exists {
			return preparedClaim{}, nil, fmt.Errorf("allocated device %q is absent", allocation.Device)
		}
		if _, exists := seen[device.Name]; exists {
			return preparedClaim{}, nil, fmt.Errorf("device %q is allocated twice", device.Name)
		}
		for uid, other := range p.prepared {
			if uid != claim.UID && slices.Contains(other.Devices, device.Name) {
				return preparedClaim{}, nil, fmt.Errorf("device %q is already prepared for another claim", device.Name)
			}
		}
		seen[device.Name] = struct{}{}
		if record.NUMA == -1 {
			record.NUMA = device.NUMA
		} else if record.NUMA != device.NUMA {
			return preparedClaim{}, nil, errors.New("allocated devices do not share one NUMA node")
		}
		record.Devices = append(record.Devices, device.Name)
		if device.Kind == kindCPU {
			record.CPUs = append(record.CPUs, device.CPU)
		}
	}
	if len(record.CPUs) == 0 {
		return preparedClaim{}, nil, errors.New("claim has no CPU allocation")
	}
	slices.Sort(record.CPUs)
	slices.Sort(record.Devices)
	record.Inventory = inventoryHash(p.inv, record.Devices)
	return record, p.cdiDevices(results), nil
}

func inventoryHash(inv *inventory, names []string) string {
	devices := make([]localDevice, 0, len(names))
	for _, name := range names {
		devices = append(devices, inv.devices[name])
	}
	return jsonHash(devices)
}

func allocationHash(results []resourceapi.DeviceRequestAllocationResult) string {
	results = slices.Clone(results)
	slices.SortFunc(results, func(a, b resourceapi.DeviceRequestAllocationResult) int {
		return strings.Compare(
			a.Request+"\x00"+a.Pool+"\x00"+a.Device,
			b.Request+"\x00"+b.Pool+"\x00"+b.Device,
		)
	})
	return jsonHash(results)
}

func jsonHash(value any) string {
	encoded, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func (p *nodePlugin) cdiDevices(results []resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device {
	output := make([]kubeletplugin.Device, 0, len(results))
	for _, allocation := range results {
		device, exists := p.inv.devices[allocation.Device]
		if !exists || (device.Kind != kindGPU && device.Kind != kindOpenTPU) {
			continue
		}
		id := driverName + "/opentpu=" + device.Name
		if device.Kind == kindGPU {
			id = driverName + "/nvidia=" + device.UUID
		}
		output = append(output, kubeletplugin.Device{Requests: []string{allocation.Request}, PoolName: allocation.Pool, DeviceName: allocation.Device, CDIDeviceIDs: []string{id}})
	}
	return output
}

func (p *nodePlugin) UnprepareResourceClaims(_ context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[types.UID]error, len(claims))
	for _, claim := range claims {
		if err := os.Remove(p.path(claim.UID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			result[claim.UID] = err
			continue
		}
		delete(p.prepared, claim.UID)
		result[claim.UID] = nil
	}
	return result, nil
}

func (p *nodePlugin) HandleError(ctx context.Context, err error, message string) {
	utilruntime.HandleErrorWithContext(ctx, err, message)
}

func (p *nodePlugin) path(uid types.UID) string {
	return filepath.Join(p.dir, string(uid)+".json")
}

func (p *nodePlugin) preparedFor(namespace, name string) (preparedClaim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, record := range p.prepared {
		if record.Namespace == namespace && record.Name == name {
			return record, true
		}
	}
	return preparedClaim{}, false
}

func (p preparedClaim) cpuSet() string {
	return cpuset.New(p.CPUs...).String()
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}
