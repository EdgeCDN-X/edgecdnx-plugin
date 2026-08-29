package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type NodeQualitySpec struct {
	NodeName       string `json:"nodeName"`
	Location       string `json:"location"`
	StaticCapacity int32  `json:"staticCapacity"`
	Enabled        bool   `json:"enabled"`
}

type NodeQualityStatus struct {
	ObservedAt          *metav1.Time       `json:"observedAt,omitempty"`
	SampleCount         int64              `json:"sampleCount,omitempty"`
	LatencyEWMAMillis   float64            `json:"latencyEWMAMillis,omitempty"`
	ErrorEWMA           float64            `json:"errorEWMA,omitempty"`
	CacheHitRatio       float64            `json:"cacheHitRatio,omitempty"`
	ActiveRequests      int32              `json:"activeRequests,omitempty"`
	ConcurrencyLimit    int32              `json:"concurrencyLimit,omitempty"`
	QualityScore        float64            `json:"qualityScore,omitempty"`
	EffectiveWeight     int32              `json:"effectiveWeight,omitempty"`
	State               string             `json:"state,omitempty"`
	StateSince          *metav1.Time       `json:"stateSince,omitempty"`
	EjectionCount       int32              `json:"ejectionCount,omitempty"`
	ConsecutiveFailures int32              `json:"consecutiveFailures,omitempty"`
	ConsecutiveHealthy  int32              `json:"consecutiveHealthy,omitempty"`
	RecoveryStep        int32              `json:"recoveryStep,omitempty"`
	LastFailureType     string             `json:"lastFailureType,omitempty"`
	EjectedUntil        *metav1.Time       `json:"ejectedUntil,omitempty"`
	Reason              string             `json:"reason,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NodeQuality struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NodeQualitySpec   `json:"spec,omitempty"`
	Status            NodeQualityStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NodeQualityList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeQuality `json:"items"`
}

func init() { SchemeBuilder.Register(&NodeQuality{}, &NodeQualityList{}) }

func (in *NodeQuality) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NodeQuality)
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	if in.Status.ObservedAt != nil {
		t := in.Status.ObservedAt.DeepCopy()
		out.Status.ObservedAt = t
	}
	if in.Status.StateSince != nil {
		t := in.Status.StateSince.DeepCopy()
		out.Status.StateSince = t
	}
	if in.Status.EjectedUntil != nil {
		t := in.Status.EjectedUntil.DeepCopy()
		out.Status.EjectedUntil = t
	}
	return out
}

func (in *NodeQualityList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NodeQualityList)
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]NodeQuality, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *(in.Items[i].DeepCopyObject().(*NodeQuality))
	}
	return out
}
