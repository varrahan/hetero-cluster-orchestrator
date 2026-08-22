package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionControlPlaneReady = "ControlPlaneReady"
	ConditionAccountingReady   = "AccountingReady"
	ConditionWorkersReady      = "WorkersReady"
)

// HeterogeneousClusterSpec is the desired configuration for one Slurm cluster.
// +kubebuilder:validation:XValidation:rule="self.workerPools.all(x, self.workerPools.filter(y, y.partition == x.partition).size() == 1)",message="worker pool partitions must be unique"
type HeterogeneousClusterSpec struct {
	ControlPlane   ControlPlaneSpec   `json:"controlPlane"`
	Authentication AuthenticationSpec `json:"authentication"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=name
	WorkerPools []WorkerPoolSpec `json:"workerPools"`
}

type ControlPlaneSpec struct {
	Controllers ControllersSpec `json:"controllers"`
	Accounting  AccountingSpec  `json:"accounting"`
	Login       LoginSpec       `json:"login"`
}

type ControllersSpec struct {
	// Image is used by every Slurm control-plane component.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="stateSaveClaim is immutable"
	StateSaveClaim string `json:"stateSaveClaim"`
}

type AccountingSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	DatabaseSecretRef string `json:"databaseSecretRef"`
}

type LoginSpec struct {
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=32
	Replicas int32 `json:"replicas"`
}

type AuthenticationSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	MungeKeySecretRef string `json:"mungeKeySecretRef"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	JWTKeySecretRef string `json:"jwtKeySecretRef"`
}

type WorkerPoolSpec struct {
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9-]{0,62}$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]{0,63}$`
	// +kubebuilder:validation:MaxLength=64
	Partition    string            `json:"partition"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +kubebuilder:default:="1Gi"
	// Fixed memory devices are expressed as positive whole MiB or GiB values.
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi)$`
	// +kubebuilder:validation:MaxLength=12
	MemoryUnit string      `json:"memoryUnit"`
	Scaling    ScalingSpec `json:"scaling"`
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=gres
	Profiles []WorkerProfile `json:"profiles,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.minReady <= self.maxWorkers",message="minReady cannot exceed maxWorkers"
type ScalingSpec struct {
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum=0
	MinReady int32 `json:"minReady"`
	// +kubebuilder:validation:Minimum=1
	// Eight pools plus their admission catalog nodes stay within Slurm's 65,536-node limit.
	// +kubebuilder:validation:Maximum=8191
	MaxWorkers int32 `json:"maxWorkers"`
	// +kubebuilder:default:="5m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="idleTimeout must be positive"
	IdleTimeout metav1.Duration `json:"idleTimeout"`
}

type WorkerProfile struct {
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9-]{0,62}$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern=`^(gpu|tpu):[a-z0-9_]+$`
	// +kubebuilder:validation:MaxLength=64
	Gres string `json:"gres"`
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=253
	DeviceClassName string `json:"deviceClassName"`
}

type HeterogeneousClusterStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +listType=map
	// +listMapKey=name
	WorkerPools []WorkerPoolStatus `json:"workerPools,omitempty"`
}

type WorkerPoolStatus struct {
	Name     string `json:"name"`
	Ready    int32  `json:"ready,omitempty"`
	Pending  int32  `json:"pending,omitempty"`
	Draining int32  `json:"draining,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
type HeterogeneousCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HeterogeneousClusterSpec   `json:"spec"`
	Status            HeterogeneousClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type HeterogeneousClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HeterogeneousCluster `json:"items"`
}
