package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/varrahan/hetero-cluster-orchestrater/src/shared/hardware"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *RecoveryReconciler) reconcileReboot(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState) (ctrl.Result, error) {
	var current corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: node.Name}, &current); err != nil {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	requestedAt := state.PhaseStartedAt.Time
	if state.RebootRequestedAt != nil {
		requestedAt = state.RebootRequestedAt.Time
	}
	if r.now().Sub(requestedAt) >= r.RebootTimeout {
		return r.enterManualRepair(ctx, &current, object, state, "node did not complete its reboot before the infrastructure timeout")
	}
	if state.Phase == phaseRebootRequested {
		if current.Annotations[rebootAckKey] != state.Incident {
			return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
		}
		if _, err := r.setPhase(ctx, &current, object, state, phaseRebooting); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	currentBootID := r.bootID(&current)
	if currentBootID == "" || currentBootID == state.PreBootID {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if state.NewBootID == "" {
		state.NewBootID = currentBootID
		state.ReadySince = nil
		var err error
		object, err = r.saveState(ctx, &current, object, state)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else if state.NewBootID != currentBootID {
		return r.enterManualRepair(ctx, &current, object, state, "node rebooted more than once during one incident")
	}
	if nodeConditionStatus(current.Status.Conditions, string(corev1.NodeReady)) != corev1.ConditionTrue {
		if state.ReadySince != nil {
			state.ReadySince = nil
			_, _ = r.saveState(ctx, &current, object, state)
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if state.ReadySince == nil {
		now := metav1.NewTime(r.now())
		state.ReadySince = &now
		if _, err := r.saveState(ctx, &current, object, state); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if r.now().Sub(state.ReadySince.Time) < readyStabilization {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if current.Annotations[inventoryBootIDKey] != state.NewBootID || current.Annotations[inventoryHashKey] == "" {
		if r.now().Sub(state.ReadySince.Time) >= verifierTimeout {
			return r.enterManualRepair(ctx, &current, object, state, "DRA did not publish inventory for the new boot")
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if state.PreInventoryHash == "" || current.Annotations[inventoryHashKey] != state.PreInventoryHash {
		return r.enterManualRepair(ctx, &current, object, state, "post-reboot DRA inventory does not match the pre-fault inventory")
	}
	if state.VerificationStarted == nil {
		now := metav1.NewTime(r.now())
		state.VerificationStarted = &now
		var err error
		object, err = r.saveState(ctx, &current, object, state)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.ensureVerifierResources(ctx, &current, object, state); err != nil {
		return r.enterManualRepair(ctx, &current, object, state, "create hardware verifiers: "+err.Error())
	}
	if _, err := r.setPhase(ctx, &current, object, state, phaseVerifying); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
}

func (r *RecoveryReconciler) reconcileVerification(ctx context.Context, node *corev1.Node, object *corev1.ConfigMap, state *recoveryState, rawStatus corev1.ConditionStatus) (ctrl.Result, error) {
	if state.VerificationStarted == nil {
		return r.enterManualRepair(ctx, node, object, state, "verification start timestamp is missing")
	}
	if r.now().Sub(state.VerificationStarted.Time) >= verifierTimeout {
		return r.enterManualRepair(ctx, node, object, state, "hardware verification timed out")
	}
	inventory, err := decodeRecoveryInventory(state.PreInventory, state.PreBootID)
	if err != nil {
		return r.enterManualRepair(ctx, node, object, state, err.Error())
	}
	if !state.VerificationPassed {
		allSucceeded := true
		for _, cell := range inventory.Cells {
			name := verifierName(state.Incident, cell.NUMA)
			var job batchv1.Job
			if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: name}, &job); err != nil {
				if apierrors.IsNotFound(err) {
					if err := r.ensureVerifierResources(ctx, node, object, state); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
				}
				return ctrl.Result{}, err
			}
			if job.Status.Failed != 0 {
				return r.enterManualRepair(ctx, node, object, state, fmt.Sprintf("hardware verifier for NUMA node %d failed", cell.NUMA))
			}
			allSucceeded = allSucceeded && job.Status.Succeeded == 1
		}
		if !allSucceeded || rawStatus != corev1.ConditionFalse {
			return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
		}
		state.VerificationPassed = true
		if _, err := r.saveState(ctx, node, object, state); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	if rawStatus != corev1.ConditionFalse {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, nil
	}
	clean, err := r.deleteVerifierResources(ctx, state, inventory)
	if err != nil || !clean {
		return ctrl.Result{RequeueAfter: recoveryRequeue}, err
	}
	if err := r.setHealthy(ctx, node); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(object, corev1.EventTypeNormal, "HardwareRecoveryComplete", "incident %s passed inventory and per-NUMA verification", state.Incident)
	}
	if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	forgetRecovery(state)
	return ctrl.Result{}, nil
}

func (r *RecoveryReconciler) ensureVerifierResources(ctx context.Context, node *corev1.Node, owner *corev1.ConfigMap, state *recoveryState) error {
	inventory, err := decodeRecoveryInventory(state.PreInventory, state.PreBootID)
	if err != nil {
		return err
	}
	for _, cell := range inventory.Cells {
		claim, job, err := r.verifierObjects(node, owner, state, cell)
		if err != nil {
			return err
		}
		var existingClaim resourceapi.ResourceClaim
		err = r.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, &existingClaim)
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		} else if err != nil {
			return err
		} else if existingClaim.Labels[recoveryIncidentKey] != state.Incident || !metav1.IsControlledBy(&existingClaim, owner) {
			return fmt.Errorf("verifier ResourceClaim %q belongs to another incident", claim.Name)
		}
		var existingJob batchv1.Job
		err = r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, &existingJob)
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		} else if err != nil {
			return err
		} else if existingJob.Labels[recoveryIncidentKey] != state.Incident || !metav1.IsControlledBy(&existingJob, owner) {
			return fmt.Errorf("verifier Job %q belongs to another incident", job.Name)
		}
	}
	return nil
}

func decodeRecoveryInventory(raw, bootID string) (hardware.Inventory, error) {
	var inventory hardware.Inventory
	if err := json.Unmarshal([]byte(raw), &inventory); err != nil || inventory.Version != 1 || inventory.BootID != bootID || len(inventory.Cells) == 0 {
		return inventory, errors.New("pre-fault DRA inventory is missing or invalid")
	}
	for _, cell := range inventory.Cells {
		if cell.NUMA < 0 || cell.CPUs < 1 || cell.MemoryUnits < 1 || cell.MemoryUnitBytes < 1 {
			return inventory, fmt.Errorf("pre-fault DRA inventory has invalid NUMA node %d", cell.NUMA)
		}
	}
	return inventory, nil
}

func (r *RecoveryReconciler) verifierObjects(node *corev1.Node, owner *corev1.ConfigMap, state *recoveryState, cell hardware.Cell) (*resourceapi.ResourceClaim, *batchv1.Job, error) {
	name := verifierName(state.Incident, cell.NUMA)
	cpuCount := int64(1)
	memoryBytes := cell.MemoryUnitBytes
	sharedMemory := int64(64 << 20)
	for _, profile := range cell.OpenTPU {
		if profile.Count < 1 || profile.CPUCores < 1 || profile.MemoryBytes < 1 || (profile.MatrixSize != 8 && profile.MatrixSize != 16) {
			return nil, nil, fmt.Errorf("invalid OpenTPU verifier profile %q", profile.Profile)
		}
		cpuCount += profile.CPUCores
		memoryBytes += profile.MemoryBytes
		sharedMemory += profile.SharedMemory
	}
	if cpuCount > cell.CPUs {
		return nil, nil, fmt.Errorf("NUMA node %d has %d CPUs but verifier footprints need %d", cell.NUMA, cell.CPUs, cpuCount)
	}
	memoryCount := (memoryBytes + cell.MemoryUnitBytes - 1) / cell.MemoryUnitBytes
	if memoryCount > cell.MemoryUnits {
		return nil, nil, fmt.Errorf("NUMA node %d lacks memory for verifier footprints", cell.NUMA)
	}
	requests := []resourceapi.DeviceRequest{
		exactRequest("cpu", "cpu.orchestration.gputpu.io", cpuCount, kindNUMASelector("cpu", cell.NUMA)),
		exactRequest("memory", "memory.orchestration.gputpu.io", memoryCount, memoryNUMASelector(cell.NUMA, cell.MemoryUnitBytes)),
	}
	if len(cell.GPUs) != 0 {
		requests = append(requests, exactRequest("gpu", "nvidia.orchestration.gputpu.io", int64(len(cell.GPUs)), kindNUMASelector("gpu", cell.NUMA)))
	}
	for i, profile := range cell.OpenTPU {
		requests = append(requests, exactRequest(fmt.Sprintf("opentpu-%d", i), "opentpu.orchestration.gputpu.io", 1, openTPUNUMASelector(profile.Profile, cell.NUMA)))
	}
	match := resourceapi.FullyQualifiedName(driverName + "/numaNode")
	labels := map[string]string{recoveryIncidentKey: state.Incident, "orchestration.gputpu.io/recovery-node": node.Name, "orchestration.gputpu.io/verifier": "true"}
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: labels}, Spec: resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{Requests: requests, Constraints: []resourceapi.DeviceConstraint{{MatchAttribute: &match}}}}}
	if err := controllerutil.SetOwnerReference(owner, claim, r.Scheme); err != nil {
		return nil, nil, err
	}
	gpuUUIDs := make([]string, 0, len(cell.GPUs))
	for _, gpu := range cell.GPUs {
		gpuUUIDs = append(gpuUUIDs, gpu.UUID)
	}
	slices.Sort(gpuUUIDs)
	profiles, _ := json.Marshal(cell.OpenTPU)
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(cpuCount, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(memoryCount*cell.MemoryUnitBytes, resource.BinarySI),
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: labels}}
	job.Spec.BackoffLimit = ptr.To[int32](0)
	job.Spec.ActiveDeadlineSeconds = ptr.To[int64](int64(verifierTimeout.Seconds()))
	job.Spec.Template = corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: "slurm-worker",
		NodeSelector:       map[string]string{"kubernetes.io/hostname": node.Name, "orchestration.gputpu.io/compute": "true"},
		Tolerations: []corev1.Toleration{
			{Key: "orchestration.gputpu.io/compute", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
			{Key: hardwareDegradedTaint, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
		},
		ResourceClaims: []corev1.PodResourceClaim{{Name: "verification", ResourceClaimName: ptr.To(name)}},
		Containers: []corev1.Container{{
			Name: "verifier", Image: r.WorkerImage, Command: []string{"/usr/local/bin/hardware-verifier"},
			Env: []corev1.EnvVar{
				{Name: "EXPECTED_GPU_UUIDS", Value: strings.Join(gpuUUIDs, ",")},
				{Name: "OPENTPU_VERIFY_PROFILES", Value: string(profiles)},
				{Name: "MEMORY_CHECK_BYTES", Value: strconv.FormatInt(min(memoryBytes, 64<<20), 10)},
				{Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
			},
			Resources:       corev1.ResourceRequirements{Requests: resources.DeepCopy(), Limits: resources.DeepCopy(), Claims: []corev1.ResourceClaim{{Name: "verification"}}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "shared-memory", MountPath: "/dev/shm"}},
		}},
		Volumes: []corev1.Volume{{Name: "shared-memory", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(sharedMemory, resource.BinarySI)}}}},
	}}
	if err := controllerutil.SetOwnerReference(owner, job, r.Scheme); err != nil {
		return nil, nil, err
	}
	return claim, job, nil
}

func kindNUMASelector(kind string, numa int) resourceapi.DeviceSelector {
	return celSelector(fmt.Sprintf(`device.driver == %q && device.attributes[%q].kind == %q && device.attributes[%q]["numaNode"] == %d`, driverName, driverName, kind, driverName, numa))
}

func memoryNUMASelector(numa int, unit int64) resourceapi.DeviceSelector {
	return celSelector(fmt.Sprintf(`device.driver == %q && device.attributes[%q].kind == "memory" && device.attributes[%q]["numaNode"] == %d && device.attributes[%q]["unitBytes"] == %d`, driverName, driverName, driverName, numa, driverName, unit))
}

func openTPUNUMASelector(profile string, numa int) resourceapi.DeviceSelector {
	return celSelector(fmt.Sprintf(`device.driver == %q && device.attributes[%q].kind == "opentpu" && device.attributes[%q]["numaNode"] == %d && device.attributes[%q]["profile"] == %s`, driverName, driverName, driverName, numa, driverName, strconv.Quote(profile)))
}

func verifierName(incident string, numa int) string {
	return fmt.Sprintf("hardware-verify-%s-n%d", strings.ReplaceAll(incident[:13], "-", ""), numa)
}

func (r *RecoveryReconciler) deleteVerifierResources(ctx context.Context, state *recoveryState, inventory hardware.Inventory) (bool, error) {
	complete := true
	for _, cell := range inventory.Cells {
		name := verifierName(state.Incident, cell.NUMA)
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace}}
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err == nil {
			complete = false
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
		claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.Namespace}}
		if err := r.Delete(ctx, claim); err == nil {
			complete = false
		} else if !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return complete, nil
}
