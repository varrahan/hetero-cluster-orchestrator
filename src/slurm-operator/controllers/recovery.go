package controllers

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	orchestrationv1alpha1 "github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/api/v1alpha1"
	"github.com/varrahan/hetero-cluster-orchestrater/src/slurm-operator/internal/slurm"
)

const (
	hardwareFaultCondition    = "HardwareFaultDetected"
	hardwareDegradedCondition = "HardwareDegraded"
	hardwareDegradedTaint     = "orchestration.gputpu.io/hardware-degraded"
	recoveryIncidentKey       = "orchestration.gputpu.io/recovery-incident"
	recoveryPhaseKey          = "orchestration.gputpu.io/recovery-phase"
	recoveryRequestKey        = "orchestration.gputpu.io/recovery-request"
	rebootRequestKey          = "orchestration.gputpu.io/reboot-request"
	rebootAckKey              = "orchestration.gputpu.io/reboot-ack"
	inventoryKey              = "orchestration.gputpu.io/inventory-v1"
	inventoryHashKey          = "orchestration.gputpu.io/inventory-hash"
	inventoryBootIDKey        = "orchestration.gputpu.io/inventory-boot-id"
	workerRecoveryAnnotation  = "orchestration.gputpu.io/recovery-incident"
	workerNodeIndex           = "recovery.spec.nodeName"
	recoveryStateVersion      = 1
	checkpointGrace           = 120 * time.Second
	readyStabilization        = 30 * time.Second
	verifierTimeout           = 2 * time.Minute
	recoveryRequeue           = 5 * time.Second
	maxRecoveryStateBytes     = 900 << 10
)

type recoveryPhase string

const (
	phaseIsolated        recoveryPhase = "Isolated"
	phaseDraining        recoveryPhase = "Draining"
	phaseCheckpointing   recoveryPhase = "Checkpointing"
	phaseRequeued        recoveryPhase = "Requeued"
	phaseRebootRequested recoveryPhase = "RebootRequested"
	phaseRebooting       recoveryPhase = "Rebooting"
	phaseVerifying       recoveryPhase = "Verifying"
	phaseManualRepair    recoveryPhase = "ManualRepair"
)

type recoveryState struct {
	Version             int               `json:"version"`
	Incident            string            `json:"incident"`
	NodeName            string            `json:"nodeName"`
	NodeUID             types.UID         `json:"nodeUID"`
	Phase               recoveryPhase     `json:"phase"`
	StartedAt           metav1.Time       `json:"startedAt"`
	PhaseStartedAt      metav1.Time       `json:"phaseStartedAt"`
	PreBootID           string            `json:"preBootID"`
	PreInventory        string            `json:"preInventory,omitempty"`
	PreInventoryHash    string            `json:"preInventoryHash,omitempty"`
	Clusters            []recoveryCluster `json:"clusters,omitempty"`
	Workers             []recoveryWorker  `json:"workers,omitempty"`
	Jobs                []recoveryJob     `json:"jobs,omitempty"`
	RebootRequestedAt   *metav1.Time      `json:"rebootRequestedAt,omitempty"`
	NewBootID           string            `json:"newBootID,omitempty"`
	ReadySince          *metav1.Time      `json:"readySince,omitempty"`
	VerificationStarted *metav1.Time      `json:"verificationStarted,omitempty"`
	VerificationPassed  bool              `json:"verificationPassed,omitempty"`
	TerminalReason      string            `json:"terminalReason,omitempty"`
	ManualRequest       string            `json:"manualRequest,omitempty"`
}

type recoveryCluster struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

type recoveryWorker struct {
	ClusterNamespace string `json:"clusterNamespace"`
	ClusterName      string `json:"clusterName"`
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	Claim            string `json:"claim,omitempty"`
}

type recoveryJob struct {
	ClusterNamespace   string       `json:"clusterNamespace"`
	ClusterName        string       `json:"clusterName"`
	ID                 uint32       `json:"id"`
	SignalRequestedAt  *metav1.Time `json:"signalRequestedAt,omitempty"`
	SignalSent         bool         `json:"signalSent,omitempty"`
	CheckpointSeen     bool         `json:"checkpointSeen,omitempty"`
	RequeueRequestedAt *metav1.Time `json:"requeueRequestedAt,omitempty"`
	RequeueSent        bool         `json:"requeueSent,omitempty"`
	Evacuated          bool         `json:"evacuated,omitempty"`
}

type RecoveryReconciler struct {
	*ClusterReconciler
	Namespace        string
	RebootTimeout    time.Duration
	BootIDAnnotation string
	Recorder         record.EventRecorder
	Now              func() time.Time
}

type clusterSnapshot struct {
	cluster *orchestrationv1alpha1.HeterogeneousCluster
	client  *slurm.Client
	nodes   map[string]slurm.Node
	jobs    []slurm.Job
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceclaims,verbs=get;list;watch;create;delete

func (r *RecoveryReconciler) SetupWithManager(manager ctrl.Manager) error {
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, workerNodeIndex, func(object client.Object) []string {
		pod := object.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(manager).Named("hardware-recovery").For(&corev1.Node{}).Complete(r)
}

func (r *RecoveryReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	if err := r.Get(ctx, request.NamespacedName, &node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	state, stateObject, err := r.loadState(ctx, &node)
	if err != nil {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	if state != nil {
		observeRecovery(state)
	}
	managed := node.Labels["orchestration.gputpu.io/compute"] == "true" || state != nil || node.Annotations[recoveryIncidentKey] != ""
	if !managed {
		return ctrl.Result{}, nil
	}
	rawStatus := nodeConditionStatus(node.Status.Conditions, hardwareFaultCondition)
	if state == nil {
		switch {
		case rawStatus == corev1.ConditionTrue:
			state, err = newRecoveryState(&node, r.now())
		case node.Annotations[recoveryIncidentKey] != "":
			state, err = newRecoveryState(&node, r.now())
			if err == nil {
				state.Phase = phaseManualRepair
				state.TerminalReason = "durable recovery state is missing"
			}
		default:
			return ctrl.Result{}, r.setHealthy(ctx, &node)
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		stateObject, err = r.saveState(ctx, &node, nil, state)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if state.Phase == phaseManualRepair {
		requestID := node.Annotations[recoveryRequestKey]
		if rawStatus == corev1.ConditionFalse && requestID != "" && requestID != state.ManualRequest && validIncident(requestID) {
			state, err = newRecoveryState(&node, r.now())
			if err != nil {
				return ctrl.Result{}, err
			}
			state.Incident, state.ManualRequest = requestID, requestID
			stateObject, err = r.saveState(ctx, &node, stateObject, state)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	if err := r.ensureIsolation(ctx, &node, state); err != nil {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	if state.Phase == phaseManualRepair {
		return ctrl.Result{}, nil
	}

	switch state.Phase {
	case phaseIsolated, phaseDraining, phaseCheckpointing, phaseRequeued:
		return r.reconcileEvacuation(ctx, &node, stateObject, state)
	case phaseRebootRequested, phaseRebooting:
		return r.reconcileReboot(ctx, &node, stateObject, state)
	case phaseVerifying:
		return r.reconcileVerification(ctx, &node, stateObject, state, rawStatus)
	default:
		return ctrl.Result{}, fmt.Errorf("unsupported recovery phase %q", state.Phase)
	}
}

func newRecoveryState(node *corev1.Node, now time.Time) (*recoveryState, error) {
	incident, err := newIncident()
	if err != nil {
		return nil, err
	}
	stamp := metav1.NewTime(now.UTC())
	return &recoveryState{
		Version: recoveryStateVersion, Incident: incident, NodeName: node.Name, NodeUID: node.UID,
		Phase: phaseIsolated, StartedAt: stamp, PhaseStartedAt: stamp, PreBootID: node.Status.NodeInfo.BootID,
		PreInventory: node.Annotations[inventoryKey], PreInventoryHash: node.Annotations[inventoryHashKey],
	}, nil
}

func newIncident() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	raw := hex.EncodeToString(value)
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:], nil
}

func validIncident(value string) bool {
	parts := strings.Split(value, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 || len(validation.IsValidLabelValue(value)) != 0 {
		return false
	}
	_, err := hex.DecodeString(strings.Join(parts, ""))
	return err == nil
}

func (r *RecoveryReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r *RecoveryReconciler) bootID(node *corev1.Node) string {
	if r.BootIDAnnotation != "" && node.Annotations[r.BootIDAnnotation] != "" {
		return node.Annotations[r.BootIDAnnotation]
	}
	return node.Status.NodeInfo.BootID
}

func recoveryStateName(node *corev1.Node) string {
	return "hardware-recovery-" + strings.ToLower(string(node.UID))
}

func (r *RecoveryReconciler) loadState(ctx context.Context, node *corev1.Node) (*recoveryState, *corev1.ConfigMap, error) {
	var object corev1.ConfigMap
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: recoveryStateName(node)}, &object)
	if apierrors.IsNotFound(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	compressed := object.BinaryData["state.json.gz"]
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, &object, fmt.Errorf("decode recovery state: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxRecoveryStateBytes+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(data) > maxRecoveryStateBytes {
		return nil, &object, errors.New("recovery state exceeds its bounded size")
	}
	var state recoveryState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != recoveryStateVersion || state.NodeName != node.Name || state.NodeUID != node.UID || !validIncident(state.Incident) {
		return nil, &object, errors.New("recovery state is invalid or belongs to another Node")
	}
	return &state, &object, nil
}

func (r *RecoveryReconciler) saveState(ctx context.Context, node *corev1.Node, existing *corev1.ConfigMap, state *recoveryState) (*corev1.ConfigMap, error) {
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded) > maxRecoveryStateBytes {
		return nil, errors.New("recovery state exceeds its bounded size")
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(encoded); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil || compressed.Len() > maxRecoveryStateBytes {
		return nil, errors.New("compressed recovery state exceeds its bounded size")
	}
	object := existing
	if object == nil {
		object = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: recoveryStateName(node), Namespace: r.Namespace}}
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, object, func() error {
		object.Labels = map[string]string{"orchestration.gputpu.io/recovery-node": node.Name, "orchestration.gputpu.io/recovery-incident": state.Incident}
		object.Data = nil
		object.BinaryData = map[string][]byte{"state.json.gz": compressed.Bytes()}
		return controllerutil.SetOwnerReference(node, object, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	observeRecovery(state)
	return object, nil
}

func (r *RecoveryReconciler) setPhase(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState, phase recoveryPhase) (*corev1.ConfigMap, error) {
	state.Phase = phase
	state.PhaseStartedAt = metav1.NewTime(r.now())
	object, err := r.saveState(ctx, node, object, state)
	if err != nil {
		return nil, err
	}
	return object, r.ensureIsolation(ctx, node, state)
}

func (r *RecoveryReconciler) enterManualRepair(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState, reason string) (ctrl.Result, error) {
	state.TerminalReason = reason
	state.ManualRequest = node.Annotations[recoveryRequestKey]
	object, err := r.setPhase(ctx, node, object, state, phaseManualRepair)
	if err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(object, corev1.EventTypeWarning, "HardwareRecoveryStopped", reason)
	}
	return ctrl.Result{}, nil
}

func nodeConditionStatus(conditions []corev1.NodeCondition, conditionType string) corev1.ConditionStatus {
	for _, condition := range conditions {
		if string(condition.Type) == conditionType {
			return condition.Status
		}
	}
	return corev1.ConditionUnknown
}

func (r *RecoveryReconciler) ensureIsolation(ctx context.Context, node *corev1.Node, state *recoveryState) error {
	copy := node.DeepCopy()
	if copy.Annotations == nil {
		copy.Annotations = map[string]string{}
	}
	copy.Annotations[recoveryIncidentKey] = state.Incident
	copy.Annotations[recoveryPhaseKey] = string(state.Phase)
	if state.Phase == phaseRebootRequested || state.Phase == phaseRebooting {
		copy.Annotations[rebootRequestKey] = state.Incident
	}
	wanted := corev1.Taint{Key: hardwareDegradedTaint, Value: "true", Effect: corev1.TaintEffectNoSchedule}
	if !slices.Contains(copy.Spec.Taints, wanted) {
		copy.Spec.Taints = append(copy.Spec.Taints, wanted)
	}
	if !maps.Equal(copy.Annotations, node.Annotations) || !slices.Equal(copy.Spec.Taints, node.Spec.Taints) {
		if err := r.Patch(ctx, copy, client.MergeFrom(node)); err != nil {
			return err
		}
		*node = *copy
	}
	reason := "RecoveryInProgress"
	message := fmt.Sprintf("hardware recovery incident %s is %s", state.Incident, state.Phase)
	if state.Phase == phaseManualRepair {
		reason, message = "ManualRepair", state.TerminalReason
	}
	return r.setNodeCondition(ctx, node.Name, corev1.ConditionTrue, reason, message)
}

func (r *RecoveryReconciler) setHealthy(ctx context.Context, node *corev1.Node) error {
	copy := node.DeepCopy()
	copy.Spec.Taints = slices.DeleteFunc(copy.Spec.Taints, func(taint corev1.Taint) bool { return taint.Key == hardwareDegradedTaint })
	if copy.Annotations != nil {
		for _, key := range []string{recoveryIncidentKey, recoveryPhaseKey, recoveryRequestKey, rebootRequestKey, rebootAckKey} {
			delete(copy.Annotations, key)
		}
	}
	if !maps.Equal(copy.Annotations, node.Annotations) || !slices.Equal(copy.Spec.Taints, node.Spec.Taints) {
		if err := r.Patch(ctx, copy, client.MergeFrom(node)); err != nil {
			return err
		}
	}
	return r.setNodeCondition(ctx, node.Name, corev1.ConditionFalse, "HardwareHealthy", "hardware recovery is not active")
}

func (r *RecoveryReconciler) setNodeCondition(ctx context.Context, name string, status corev1.ConditionStatus, reason, message string) error {
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &node); err != nil {
		return err
	}
	copy := node.DeepCopy()
	now := metav1.NewTime(r.now())
	found := false
	for i := range copy.Status.Conditions {
		condition := &copy.Status.Conditions[i]
		if string(condition.Type) != hardwareDegradedCondition {
			continue
		}
		found = true
		if condition.Status == status && condition.Reason == reason && condition.Message == message {
			return nil
		}
		condition.LastTransitionTime = now
		condition.Status, condition.Reason, condition.Message, condition.LastHeartbeatTime = status, reason, message, now
	}
	if !found {
		copy.Status.Conditions = append(copy.Status.Conditions, corev1.NodeCondition{Type: corev1.NodeConditionType(hardwareDegradedCondition), Status: status, Reason: reason, Message: message, LastHeartbeatTime: now, LastTransitionTime: now})
	}
	return r.Status().Patch(ctx, copy, client.MergeFrom(&node))
}
