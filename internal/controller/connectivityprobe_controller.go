/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	monitoringv1alpha1 "github.com/idanherman/connectivity-probe-operator/api/v1alpha1"
)

const (
	agentPortTCP    = 8081
	agentPortHTTP   = 8082
	requeueInterval = 30 * time.Second
)

// ConnectivityProbeReconciler reconciles a ConnectivityProbe object.
type ConnectivityProbeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=monitoring.connectivity-probe.io,resources=connectivityprobes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.connectivity-probe.io,resources=connectivityprobes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=prometheusrules,verbs=get;list;watch;create;update;patch;delete

func (r *ConnectivityProbeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	probe := &monitoringv1alpha1.ConnectivityProbe{}
	if err := r.Get(ctx, req.NamespacedName, probe); err != nil {
		if errors.IsNotFound(err) {
			log.Info("ConnectivityProbe resource deleted, nothing to do")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := r.reconcileServiceAccount(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile ServiceAccount")
		return ctrl.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile headless Service")
		return ctrl.Result{}, err
	}

	if err := r.reconcileDaemonSet(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile DaemonSet")
		return ctrl.Result{}, err
	}

	if err := r.reconcileServiceMonitor(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile ServiceMonitor")
		return ctrl.Result{}, err
	}

	if err := r.reconcilePrometheusRule(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile PrometheusRule")
		return ctrl.Result{}, err
	}

	// --- Update status ---
	if err := r.updateStatus(ctx, probe); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *ConnectivityProbeReconciler) reconcileServiceAccount(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agentServiceAccount(probe),
			Namespace: probe.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		return controllerutil.SetControllerReference(probe, sa, r.Scheme)
	})
	return err
}

// --------------------------------------------------------------------------
// Headless Service — used by agents for DNS-based peer discovery
// --------------------------------------------------------------------------

func (r *ConnectivityProbeReconciler) reconcileHeadlessService(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-mesh-headless", probe.Name),
			Namespace: probe.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(probe, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = agentLabels(probe)
		svc.Spec = corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 agentLabels(probe),
			Ports: []corev1.ServicePort{
				{Name: "tcp-mesh", Port: agentPortTCP, TargetPort: intstr.FromInt(agentPortTCP), Protocol: corev1.ProtocolTCP},
				{Name: "http", Port: agentPortHTTP, TargetPort: intstr.FromInt(agentPortHTTP), Protocol: corev1.ProtocolTCP},
			},
		}
		return nil
	})
	return err
}

// --------------------------------------------------------------------------
// DaemonSet — one mesh agent per selected node
// --------------------------------------------------------------------------

func (r *ConnectivityProbeReconciler) reconcileDaemonSet(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-mesh-agent", probe.Name),
			Namespace: probe.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ds, func() error {
		if err := controllerutil.SetControllerReference(probe, ds, r.Scheme); err != nil {
			return err
		}

		labels := agentLabels(probe)
		probeInterval := probe.Spec.ContinuousProbes.Interval
		if probeInterval == "" {
			probeInterval = "200ms"
		}
		peerResolveInterval := probe.Spec.ContinuousProbes.PeerResolveInterval
		if peerResolveInterval == "" {
			peerResolveInterval = "10s"
		}
		reconnectDelay := probe.Spec.ContinuousProbes.ReconnectDelay
		if reconnectDelay == "" {
			reconnectDelay = "1s"
		}

		httpPort := corev1.ContainerPort{Name: "http", ContainerPort: agentPortHTTP, Protocol: corev1.ProtocolTCP}
		if probe.Spec.ExternalSentinel != nil && probe.Spec.ExternalSentinel.Enabled {
			metricsHP := probe.Spec.ExternalSentinel.MetricsHostPort
			if metricsHP == 0 {
				metricsHP = 9200
			}
			httpPort.HostPort = metricsHP
		}

		runAsNonRoot := true
		allowPrivilegeEscalation := false
		containers := []corev1.Container{
			{
				Name:            "mesh-agent",
				Image:           probe.Spec.Image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot:             &runAsNonRoot,
					AllowPrivilegeEscalation: &allowPrivilegeEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				Env: []corev1.EnvVar{
					{Name: "PROBE_INTERVAL", Value: probeInterval},
					{Name: "PEER_RESOLVE_INTERVAL", Value: peerResolveInterval},
					{Name: "RECONNECT_DELAY", Value: reconnectDelay},
					{Name: "LATENCY_BUCKETS", Value: latencyBucketsEnv(probe)},
					{Name: "PEER_SERVICE", Value: fmt.Sprintf("%s-mesh-headless.%s.svc.cluster.local", probe.Name, probe.Namespace)},
					envFromFieldPath("POD_IP", "status.podIP"),
					envFromFieldPath("NODE_NAME", "spec.nodeName"),
					envFromFieldPath("POD_NAME", "metadata.name"),
					envFromFieldPath("NAMESPACE", "metadata.namespace"),
				},
				Ports: []corev1.ContainerPort{
					{Name: "tcp-mesh", ContainerPort: agentPortTCP, Protocol: corev1.ProtocolTCP},
					httpPort,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(agentPortHTTP)}},
					InitialDelaySeconds: 5,
					PeriodSeconds:       10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt(agentPortHTTP)}},
					InitialDelaySeconds: 3,
					PeriodSeconds:       5,
				},
			},
		}

		if probe.Spec.Resources.Limits != nil || probe.Spec.Resources.Requests != nil {
			containers[0].Resources = probe.Spec.Resources
		}

		ds.Spec = appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: agentServiceAccount(probe),
					NodeSelector:       probe.Spec.NodeSelector,
					Tolerations:        probe.Spec.Tolerations,
					PriorityClassName:  probe.Spec.PriorityClassName,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   &runAsNonRoot,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: containers,
				},
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
			},
		}
		return nil
	})
	return err
}

// --------------------------------------------------------------------------
// Status
// --------------------------------------------------------------------------

func (r *ConnectivityProbeReconciler) updateStatus(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	log := logf.FromContext(ctx)
	dsName := fmt.Sprintf("%s-mesh-agent", probe.Name)
	ds := &appsv1.DaemonSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: dsName, Namespace: probe.Namespace}, ds); err != nil {
		return err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(probe.Namespace), client.MatchingLabels(agentLabels(probe))); err != nil {
		return err
	}

	var edgesUp, edgesDown int32
	scrapeErrors := 0
	running := int32(0)
	httpClient := &http.Client{Timeout: 2 * time.Second}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		running++
		wg.Add(1)
		go func(name, ip string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			up, down, err := scrapeEdgeCounts(ctx, httpClient, ip)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				scrapeErrors++
				log.V(1).Info("agent status scrape failed", "pod", name, "error", err.Error())
				return
			}
			edgesUp += up
			edgesDown += down
		}(pod.Name, pod.Status.PodIP)
	}
	wg.Wait()

	probe.Status.DaemonSetName = dsName
	probe.Status.MeshSize = running
	probe.Status.EdgesUp = edgesUp
	probe.Status.EdgesDown = edgesDown
	probe.Status.EdgesTotal = edgesUp + edgesDown
	if probe.Status.EdgesTotal == 0 {
		if running <= 1 && scrapeErrors == 0 {
			probe.Status.MeshHealthPercent = 100
		} else {
			probe.Status.MeshHealthPercent = 0
		}
	} else {
		probe.Status.MeshHealthPercent = edgesUp * 100 / probe.Status.EdgesTotal
	}
	now := metav1.Now()
	probe.Status.LastReconcileTime = &now

	probe.Status.SentinelEndpoints = nil
	if probe.Spec.ExternalSentinel != nil && probe.Spec.ExternalSentinel.Enabled {
		hp := probe.Spec.ExternalSentinel.MetricsHostPort
		if hp == 0 {
			hp = 9200
		}
		endpoints := make([]string, 0, len(pods.Items))
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" {
				continue
			}
			node := &corev1.Node{}
			if err := r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
				continue
			}
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP {
					endpoints = append(endpoints, fmt.Sprintf("%s:%d", addr.Address, hp))
					break
				}
			}
		}
		probe.Status.SentinelEndpoints = endpoints
	}

	dsReady := ds.Status.DesiredNumberScheduled > 0 && ds.Status.NumberReady == ds.Status.DesiredNumberScheduled
	dsStatus := metav1.ConditionFalse
	dsReason := "Progressing"
	dsMessage := fmt.Sprintf("%d/%d agents ready", ds.Status.NumberReady, ds.Status.DesiredNumberScheduled)
	if dsReady {
		dsStatus = metav1.ConditionTrue
		dsReason = "Ready"
	}
	meta.SetStatusCondition(&probe.Status.Conditions, metav1.Condition{
		Type:               monitoringv1alpha1.ConditionDaemonSetReady,
		Status:             dsStatus,
		Reason:             dsReason,
		Message:            dsMessage,
		ObservedGeneration: probe.Generation,
	})

	healthy := probe.Status.EdgesTotal > 0 && probe.Status.EdgesDown == 0
	healthStatus := metav1.ConditionFalse
	healthReason := "EdgesDown"
	healthMessage := fmt.Sprintf("%d of %d edges are down", probe.Status.EdgesDown, probe.Status.EdgesTotal)
	if running <= 1 && probe.Status.EdgesTotal == 0 && scrapeErrors == 0 {
		healthy = true
	}
	if healthy {
		healthStatus = metav1.ConditionTrue
		healthReason = "MeshHealthy"
		healthMessage = fmt.Sprintf("%d edges up", probe.Status.EdgesUp)
	}
	meta.SetStatusCondition(&probe.Status.Conditions, metav1.Condition{
		Type:               monitoringv1alpha1.ConditionMeshHealthy,
		Status:             healthStatus,
		Reason:             healthReason,
		Message:            healthMessage,
		ObservedGeneration: probe.Generation,
	})

	degraded := probe.Status.EdgesUp > 0 && probe.Status.EdgesDown > 0
	degradedStatus := metav1.ConditionFalse
	degradedReason := "NotDegraded"
	degradedMessage := "mesh is fully up or fully down"
	if degraded {
		degradedStatus = metav1.ConditionTrue
		degradedReason = "PartialOutage"
		degradedMessage = fmt.Sprintf("%d edges up, %d edges down", probe.Status.EdgesUp, probe.Status.EdgesDown)
	}
	meta.SetStatusCondition(&probe.Status.Conditions, metav1.Condition{
		Type:               monitoringv1alpha1.ConditionDegraded,
		Status:             degradedStatus,
		Reason:             degradedReason,
		Message:            degradedMessage,
		ObservedGeneration: probe.Generation,
	})

	return r.Status().Update(ctx, probe)
}

func latencyBucketsEnv(probe *monitoringv1alpha1.ConnectivityProbe) string {
	buckets := probe.Spec.Metrics.LatencyBuckets
	if len(buckets) == 0 {
		buckets = []string{"0.0005", "0.001", "0.002", "0.005", "0.01", "0.025", "0.05", "0.075", "0.1", "0.15", "0.2", "0.3", "0.5", "1"}
	}
	return strings.Join(buckets, ",")
}

type agentEdgeStatus struct {
	Connections map[string]struct {
		TCP string `json:"tcp"`
	} `json:"connections"`
}

func scrapeEdgeCounts(ctx context.Context, client *http.Client, podIP string) (up, down int32, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d/status", podIP, agentPortHTTP), nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, 0, err
	}
	var payload agentEdgeStatus
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, 0, err
	}
	for _, conn := range payload.Connections {
		switch conn.TCP {
		case "connected":
			up++
		case "disconnected":
			down++
		}
	}
	return up, down, nil
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func (r *ConnectivityProbeReconciler) reconcileServiceMonitor(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	obj := monitoringObject("ServiceMonitor", probe)
	if !probe.Spec.Metrics.Enabled {
		return r.deleteMonitoringObject(ctx, obj)
	}
	interval := probe.Spec.Metrics.ScrapeInterval
	if interval == "" {
		interval = "15s"
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(probe, obj, r.Scheme); err != nil {
			return err
		}
		obj.Object["spec"] = map[string]any{
			"selector": map[string]any{"matchLabels": agentLabels(probe)},
			"endpoints": []any{
				map[string]any{
					"port":     "http",
					"path":     "/metrics",
					"interval": interval,
					"scheme":   "http",
				},
			},
		}
		return nil
	})
	if meta.IsNoMatchError(err) {
		logf.FromContext(ctx).Info("ServiceMonitor CRD is not installed; skipping")
		return nil
	}
	return err
}

func (r *ConnectivityProbeReconciler) reconcilePrometheusRule(ctx context.Context, probe *monitoringv1alpha1.ConnectivityProbe) error {
	obj := monitoringObject("PrometheusRule", probe)
	if probe.Spec.Alerting == nil || !probe.Spec.Alerting.Enabled {
		return r.deleteMonitoringObject(ctx, obj)
	}
	alert := probe.Spec.Alerting
	forDuration := time.Duration(alert.ConsecutiveFailures) * 15 * time.Second
	if forDuration < 15*time.Second {
		forDuration = 15 * time.Second
	}
	rules := []any{
		map[string]any{
			"alert": "ConnectivityProbeEdgeDown",
			"expr":  "connectivity_probe_tcp_up == 0",
			"for":   forDuration.String(),
			"labels": map[string]any{
				"severity": "warning",
			},
			"annotations": map[string]any{
				"summary":     "Connectivity mesh edge is down",
				"description": "TCP echo from {{ $labels.src_node }} to {{ $labels.dst_node }} is failing.",
			},
		},
	}
	if seconds, ok := durationSeconds(alert.LatencyWarningThreshold); ok {
		rules = append(rules, latencyAlert("ConnectivityProbeLatencyWarning", "warning", seconds))
	}
	if seconds, ok := durationSeconds(alert.LatencyCriticalThreshold); ok {
		rules = append(rules, latencyAlert("ConnectivityProbeLatencyCritical", "critical", seconds))
	}
	threshold := alert.MeshHealthPercentThreshold
	if threshold == 0 {
		threshold = 100
	}
	rules = append(rules, map[string]any{
		"alert": "ConnectivityProbeMeshDegraded",
		"expr": fmt.Sprintf(
			"(sum(connectivity_probe_tcp_up) / clamp_min(count(connectivity_probe_tcp_up), 1)) * 100 < %d",
			threshold,
		),
		"for": forDuration.String(),
		"labels": map[string]any{
			"severity": "critical",
		},
		"annotations": map[string]any{
			"summary": fmt.Sprintf("Connectivity mesh health is below %d%%", threshold),
		},
	})

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		if err := controllerutil.SetControllerReference(probe, obj, r.Scheme); err != nil {
			return err
		}
		obj.Object["spec"] = map[string]any{
			"groups": []any{
				map[string]any{
					"name":  "connectivity-probe",
					"rules": rules,
				},
			},
		}
		return nil
	})
	if meta.IsNoMatchError(err) {
		logf.FromContext(ctx).Info("PrometheusRule CRD is not installed; skipping")
		return nil
	}
	return err
}

func (r *ConnectivityProbeReconciler) deleteMonitoringObject(ctx context.Context, obj *unstructured.Unstructured) error {
	err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if errors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, obj))
}

func monitoringObject(kind string, probe *monitoringv1alpha1.ConnectivityProbe) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    kind,
	})
	obj.SetName(fmt.Sprintf("%s-mesh", probe.Name))
	obj.SetNamespace(probe.Namespace)
	obj.SetLabels(agentLabels(probe))
	return obj
}

func latencyAlert(name, severity string, seconds float64) map[string]any {
	return map[string]any{
		"alert": name,
		"expr": fmt.Sprintf(
			"histogram_quantile(0.99, sum by (le, src_node, dst_node) (rate(connectivity_probe_tcp_latency_seconds_bucket[2m]))) > %s",
			strconv.FormatFloat(seconds, 'f', -1, 64),
		),
		"for": "2m",
		"labels": map[string]any{
			"severity": severity,
		},
		"annotations": map[string]any{
			"summary": fmt.Sprintf("Connectivity mesh p99 latency is above %ss", strconv.FormatFloat(seconds, 'f', -1, 64)),
		},
	}
}

func durationSeconds(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, false
	}
	return d.Seconds(), true
}

func agentServiceAccount(probe *monitoringv1alpha1.ConnectivityProbe) string {
	return probe.Name + "-agent"
}

func agentLabels(probe *monitoringv1alpha1.ConnectivityProbe) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "connectivity-probe-agent",
		"app.kubernetes.io/instance":   probe.Name,
		"app.kubernetes.io/managed-by": "connectivity-probe-operator",
		"app.kubernetes.io/component":  "mesh-agent",
	}
}

func envFromFieldPath(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectivityProbeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&monitoringv1alpha1.ConnectivityProbe{}).
		Owns(&appsv1.DaemonSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Named("connectivityprobe").
		Complete(r)
}
