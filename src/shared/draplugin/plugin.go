package draplugin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

type PreparedClaim struct {
	Version   int       `json:"version"`
	UID       types.UID `json:"uid"`
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Hash      string    `json:"allocationHash"`
	Inventory string    `json:"inventoryHash"`
	Devices   []string  `json:"devices"`
	CPUs      []int     `json:"cpus,omitempty"`
	NUMA      int       `json:"numaNode,omitempty"`
}

type Config struct {
	Version       int
	DriverName    string
	PoolName      string
	HasDevice     func(string) bool
	InventoryHash func([]string) string
	Prepare       func([]string, *PreparedClaim) error
	ClaimDevices  func([]resourceapi.DeviceRequestAllocationResult) []kubeletplugin.Device
}

type Plugin struct {
	mu       sync.RWMutex
	dir      string
	config   Config
	prepared map[types.UID]PreparedClaim
}

func New(dir string, config Config) (*Plugin, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create driver state directory: %w", err)
	}
	plugin := &Plugin{dir: dir, config: config, prepared: map[types.UID]PreparedClaim{}}
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
		var record PreparedClaim
		if err := json.Unmarshal(data, &record); err != nil || record.Version != config.Version || record.UID == "" {
			return nil, fmt.Errorf("invalid prepared state %q", path)
		}
		for _, name := range record.Devices {
			if !config.HasDevice(name) {
				return nil, fmt.Errorf("prepared device %q is absent from current inventory", name)
			}
			if owner, exists := held[name]; exists && owner != record.UID {
				return nil, fmt.Errorf("prepared device %q is held by claims %q and %q", name, owner, record.UID)
			}
			held[name] = record.UID
		}
		if plugin.inventoryHash(record.Devices) != record.Inventory {
			return nil, fmt.Errorf("prepared claim %q inventory changed", record.UID)
		}
		plugin.prepared[record.UID] = record
	}
	return plugin, nil
}

func (p *Plugin) Recover(ctx context.Context, client kubernetes.Interface) error {
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

func (p *Plugin) PrepareResourceClaims(_ context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[types.UID]kubeletplugin.PrepareResult, len(claims))
	for _, claim := range claims {
		prepared, devices, err := p.prepare(claim)
		if err == nil {
			err = AtomicJSON(p.path(claim.UID), prepared, 0640)
		}
		if err == nil {
			p.prepared[claim.UID] = prepared
		}
		result[claim.UID] = kubeletplugin.PrepareResult{Err: err, Devices: devices}
	}
	return result, nil
}

func (p *Plugin) prepare(claim *resourceapi.ResourceClaim) (PreparedClaim, []kubeletplugin.Device, error) {
	if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
		return PreparedClaim{}, nil, errors.New("claim has no allocated devices")
	}
	results := claim.Status.Allocation.Devices.Results
	hash := allocationHash(results)
	if old, exists := p.prepared[claim.UID]; exists {
		if old.Hash != hash || old.Inventory != p.inventoryHash(old.Devices) || old.Namespace != claim.Namespace || old.Name != claim.Name {
			return PreparedClaim{}, nil, errors.New("claim allocation changed after preparation")
		}
		return old, p.config.ClaimDevices(results), nil
	}

	record := PreparedClaim{Version: p.config.Version, UID: claim.UID, Namespace: claim.Namespace, Name: claim.Name, Hash: hash}
	seen := map[string]struct{}{}
	for _, allocation := range results {
		if allocation.Driver != p.config.DriverName || allocation.Pool != p.config.PoolName {
			return PreparedClaim{}, nil, fmt.Errorf("allocation %s/%s belongs to another driver or node", allocation.Pool, allocation.Device)
		}
		if !p.config.HasDevice(allocation.Device) {
			return PreparedClaim{}, nil, fmt.Errorf("allocated device %q is absent", allocation.Device)
		}
		if _, exists := seen[allocation.Device]; exists {
			return PreparedClaim{}, nil, fmt.Errorf("device %q is allocated twice", allocation.Device)
		}
		for uid, other := range p.prepared {
			if uid != claim.UID && slices.Contains(other.Devices, allocation.Device) {
				return PreparedClaim{}, nil, fmt.Errorf("device %q is already prepared for another claim", allocation.Device)
			}
		}
		seen[allocation.Device] = struct{}{}
		record.Devices = append(record.Devices, allocation.Device)
	}
	slices.Sort(record.Devices)
	if p.config.Prepare != nil {
		if err := p.config.Prepare(record.Devices, &record); err != nil {
			return PreparedClaim{}, nil, err
		}
	}
	record.Inventory = p.inventoryHash(record.Devices)
	return record, p.config.ClaimDevices(results), nil
}

func (p *Plugin) UnprepareResourceClaims(_ context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
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

func (p *Plugin) HandleError(ctx context.Context, err error, message string) {
	utilruntime.HandleErrorWithContext(ctx, err, message)
}

func (p *Plugin) PreparedFor(namespace, name string) (PreparedClaim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, record := range p.prepared {
		if record.Namespace == namespace && record.Name == name {
			return record, true
		}
	}
	return PreparedClaim{}, false
}

func (p *Plugin) inventoryHash(names []string) string {
	names = slices.Clone(names)
	slices.Sort(names)
	return p.config.InventoryHash(names)
}

func (p *Plugin) path(uid types.UID) string {
	return filepath.Join(p.dir, string(uid)+".json")
}

func allocationHash(results []resourceapi.DeviceRequestAllocationResult) string {
	results = slices.Clone(results)
	slices.SortFunc(results, func(a, b resourceapi.DeviceRequestAllocationResult) int {
		return strings.Compare(a.Request+"\x00"+a.Pool+"\x00"+a.Device, b.Request+"\x00"+b.Pool+"\x00"+b.Device)
	})
	return JSONHash(results)
}

func JSONHash(value any) string {
	encoded, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func AtomicJSON(path string, value any, mode os.FileMode) error {
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

type Environment struct {
	Context  context.Context
	Cancel   context.CancelFunc
	Client   kubernetes.Interface
	NodeName string
	Node     *corev1.Node
}

func InCluster() (*Environment, error) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return nil, errors.New("NODE_NAME is required")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("read node %q: %w", nodeName, err)
	}
	return &Environment{Context: ctx, Cancel: cancel, Client: client, NodeName: nodeName, Node: node}, nil
}

func Start(ctx context.Context, plugin kubeletplugin.DRAPlugin, client kubernetes.Interface, nodeName, driverName string, resources resourceslice.DriverResources) (*kubeletplugin.Helper, error) {
	helper, err := kubeletplugin.Start(ctx, plugin,
		kubeletplugin.DriverName(driverName),
		kubeletplugin.KubeClient(client),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.NodeV1(true),
		kubeletplugin.NodeV1beta1(false),
		kubeletplugin.GRPCVerbosity(2),
	)
	if err != nil {
		return nil, fmt.Errorf("start kubelet plugin: %w", err)
	}
	if err := helper.PublishResources(ctx, resources); err != nil {
		helper.Stop()
		return nil, fmt.Errorf("publish resources: %w", err)
	}
	return helper, nil
}

type AnnotationKeys struct {
	Inventory string
	Hash      string
	BootID    string
}

func PublishInventory[T any](ctx context.Context, client kubernetes.Interface, nodeName string, keys AnnotationKeys, summary func(string) (T, string, string, error)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read node for inventory publication: %w", err)
		}
		_, encoded, hash, err := summary(node.Status.NodeInfo.BootID)
		if err != nil {
			return err
		}
		if node.Annotations[keys.BootID] == node.Status.NodeInfo.BootID && node.Annotations[keys.Hash] != "" && node.Annotations[keys.Hash] != hash {
			return fmt.Errorf("DRA inventory changed within boot %q", node.Status.NodeInfo.BootID)
		}
		if node.Annotations[keys.BootID] == node.Status.NodeInfo.BootID && node.Annotations[keys.Hash] == hash && node.Annotations[keys.Inventory] == encoded {
			return nil
		}
		copy := node.DeepCopy()
		if copy.Annotations == nil {
			copy.Annotations = map[string]string{}
		}
		copy.Annotations[keys.Inventory] = encoded
		copy.Annotations[keys.Hash] = hash
		copy.Annotations[keys.BootID] = node.Status.NodeInfo.BootID
		if _, err := client.CoreV1().Nodes().Update(ctx, copy, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("publish DRA inventory: %w", err)
		}
		return nil
	})
}
