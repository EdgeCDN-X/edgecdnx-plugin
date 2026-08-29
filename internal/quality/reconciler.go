package quality

import (
	"context"
	"fmt"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type Reconciler struct {
	client.Client
	APIReader client.Reader
	Provider  MetricsProvider
	Interval  time.Duration
	Engine    Engine
	Clock     Clock
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var nq adaptivev1alpha1.NodeQuality
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, &nq); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now := r.now()
	if !nq.Spec.Enabled {
		status, err := r.Engine.Evaluate(nq.Spec, nq.Status, NodeSample{Timestamp: now}, Cohort{}, now)
		if err != nil {
			return ctrl.Result{}, err
		}
		nq.Status = status
		r.setConditions(&nq, false, "Disabled", "metrics are not queried for disabled nodes")
		return ctrl.Result{}, r.updateStatus(ctx, &nq)
	}
	cohort, err := r.cohort(ctx, &nq)
	if err != nil {
		return ctrl.Result{}, err
	}
	sample, err := r.Provider.QueryNode(ctx, nq.Spec.NodeName, now)
	if err != nil {
		nq.Status = r.Engine.MarkMetricsUnavailable(nq.Spec, nq.Status, cohort, now)
		r.setConditions(&nq, false, "QueryFailed", err.Error())
		return ctrl.Result{RequeueAfter: r.Interval}, r.updateStatus(ctx, &nq)
	}
	previousState, previousReason := nq.Status.State, nq.Status.Reason
	status, err := r.Engine.Evaluate(nq.Spec, nq.Status, sample, cohort, now)
	if err != nil {
		r.setCondition(&nq, "MetricsAvailable", metav1.ConditionFalse, "InvalidSample", err.Error())
		return ctrl.Result{RequeueAfter: r.Interval}, r.updateStatus(ctx, &nq)
	}
	nq.Status = status
	if status.State == StateDegraded && status.Reason == "ejection blocked by maximum percentage" {
		if previousState != status.State || previousReason != status.Reason {
			ejectionOverflowTotal.WithLabelValues(nq.Spec.Location, nq.Spec.NodeName).Inc()
			ctrl.LoggerFrom(ctx).Info("ejection blocked by safety limit", "node", nq.Spec.NodeName, "location", nq.Spec.Location, "enabled", cohort.TotalEnabled, "ejected", cohort.Ejected, "fallbackAvailable", cohort.FallbackAvailable)
		}
	}
	if previousState != status.State {
		stateTransitionsTotal.WithLabelValues(nq.Spec.Location, nq.Spec.NodeName, previousState, status.State).Inc()
	}
	r.setConditions(&nq, true, "QuerySucceeded", "real Prometheus samples were read")
	if err := r.updateStatus(ctx, &nq); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}
	return ctrl.Result{RequeueAfter: r.Interval}, nil
}

func (r *Reconciler) cohort(ctx context.Context, current *adaptivev1alpha1.NodeQuality) (Cohort, error) {
	var list adaptivev1alpha1.NodeQualityList
	if err := r.List(ctx, &list, client.InNamespace(current.Namespace)); err != nil {
		return Cohort{}, err
	}
	var cohort Cohort
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.Location != current.Spec.Location || !item.Spec.Enabled {
			continue
		}
		cohort.TotalEnabled++
		if item.Status.State == StateEjected {
			cohort.Ejected++
		}
	}
	var location infrastructurev1alpha1.Location
	if err := r.Get(ctx, types.NamespacedName{Name: current.Spec.Location, Namespace: current.Namespace}, &location); err != nil {
		return Cohort{}, fmt.Errorf("read Location %s fallback configuration: %w", current.Spec.Location, err)
	}
	cohort.FallbackAvailable = len(location.Spec.FallbackLocations) > 0
	return cohort, nil
}

func (r *Reconciler) setConditions(nq *adaptivev1alpha1.NodeQuality, available bool, reason, message string) {
	metricsStatus := metav1.ConditionFalse
	if available {
		metricsStatus = metav1.ConditionTrue
	}
	r.setCondition(nq, "MetricsAvailable", metricsStatus, reason, message)
	quality := nq.Status.State != StateUnknown && nq.Status.State != "" && available
	r.setBoolCondition(nq, "QualityComputed", quality, "EngineEvaluated", "EWMA, state and bounded weight evaluated")
	eligible := nq.Status.EffectiveWeight > 0 && nq.Status.State != StateEjected && nq.Status.State != StateDisabled
	r.setBoolCondition(nq, "RoutingEligible", eligible, "WeightEvaluated", fmt.Sprintf("effective weight is %d", nq.Status.EffectiveWeight))
	r.setBoolCondition(nq, "Stale", nq.Status.State == StateStale, "StateEvaluated", nq.Status.Reason)
	r.setBoolCondition(nq, "Ejected", nq.Status.State == StateEjected, "StateEvaluated", nq.Status.Reason)
	r.setBoolCondition(nq, "Recovering", nq.Status.State == StateRecovering, "StateEvaluated", nq.Status.Reason)
}

func (r *Reconciler) setBoolCondition(nq *adaptivev1alpha1.NodeQuality, kind string, value bool, reason, message string) {
	status := metav1.ConditionFalse
	if value {
		status = metav1.ConditionTrue
	}
	r.setCondition(nq, kind, status, reason, message)
}

func (r *Reconciler) setCondition(nq *adaptivev1alpha1.NodeQuality, kind string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&nq.Status.Conditions, metav1.Condition{Type: kind, Status: status, Reason: reason, Message: message, ObservedGeneration: nq.Generation})
}

func (r *Reconciler) updateStatus(ctx context.Context, nq *adaptivev1alpha1.NodeQuality) error {
	desired := nq.Status
	desired.Conditions = append([]metav1.Condition(nil), nq.Status.Conditions...)
	key := types.NamespacedName{Name: nq.Name, Namespace: nq.Namespace}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest adaptivev1alpha1.NodeQuality
		if err := reader.Get(ctx, key, &latest); err != nil {
			return err
		}
		latest.Status = desired
		return r.Status().Update(ctx, &latest)
	})
}
func (r *Reconciler) now() time.Time {
	if r.Clock == nil {
		return time.Now()
	}
	return r.Clock.Now()
}
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&adaptivev1alpha1.NodeQuality{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
