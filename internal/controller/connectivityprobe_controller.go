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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	monitoringv1alpha1 "github.com/idanherman/connectivity-probe-operator/api/v1alpha1"
)

const (
	agentPortTCP     = 8081
	agentPortHTTP    = 8082
	agentPortWS      = 8080
	finalizerName    = "monitoring.connectivity-probe.io/finalizer"
	requeueInterval  = 30 * time.Second
)

// ConnectivityProbeReconciler reconciles a ConnectivityProbe object.
type ConnectivityProbeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=monitoring.connectivity-probe.io,resources=connectivityprobes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.connectivity-probe.io,resources=connectivityprobes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=monitoring.connectivity-probe.io,resources=connectivityprobes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
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

	// --- Reconcile Headless Service (peer discovery) ---
	if err := r.reconcileHeadlessService(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile headless Service")
		return ctrl.Result{}, err
	}

	// --- Reconcile DaemonSet ---
	if err := r.reconcileDaemonSet(ctx, probe); err != nil {
		log.Error(err, "failed to reconcile DaemonSet")
		return ctrl.Result{}, err
	}

	// --- Update status ---
	if err := r.updateStatus(ctx, probe); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
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
		svc.Spec = corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  agentLabels(probe),
			Ports: []corev1.ServicePort{
				{Name: "tcp-mesh", Port: agentPortTCP, TargetPort: intstr.FromInt(agentPortTCP), Protocol: corev1.ProtocolTCP},
				{Name: "http", Port: agentPortHTTP, TargetPort: intstr.FromInt(agentPortHTTP), Protocol: corev1.ProtocolTCP},
				{Name: "ws", Port: agentPortWS, TargetPort: intstr.FromInt(agentPortWS), Protocol: corev1.ProtocolTCP},
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

		containers := []corev1.Container{
			{
				Name:  "mesh-agent",
				Image: probe.Spec.Image,
				Env: []corev1.EnvVar{
					{Name: "PROBE_INTERVAL", Value: probeInterval},
					{Name: "PEER_RESOLVE_INTERVAL", Value: peerResolveInterval},
					{Name: "PEER_SERVICE", Value: fmt.Sprintf("%s-mesh-headless.%s.svc.cluster.local", probe.Name, probe.Namespace)},
					envFromFieldPath("POD_IP", "status.podIP"),
					envFromFieldPath("NODE_NAME", "spec.nodeName"),
					envFromFieldPath("POD_NAME", "metadata.name"),
					envFromFieldPath("NAMESPACE", "metadata.namespace"),
				},
				Ports: []corev1.ContainerPort{
					{Name: "tcp-mesh", ContainerPort: agentPortTCP, Protocol: corev1.ProtocolTCP},
					{Name: "http", ContainerPort: agentPortHTTP, Protocol: corev1.ProtocolTCP},
					{Name: "ws", ContainerPort: agentPortWS, Protocol: corev1.ProtocolTCP},
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

		// HostPort for external sentinel
		if probe.Spec.ExternalSentinel != nil && probe.Spec.ExternalSentinel.Enabled {
			metricsHP := probe.Spec.ExternalSentinel.MetricsHostPort
			if metricsHP == 0 {
				metricsHP = 9200
			}
			containers[0].Ports = append(containers[0].Ports, corev1.ContainerPort{
				Name:          "sentinel-metrics",
				ContainerPort: agentPortHTTP,
				HostPort:      metricsHP,
				Protocol:      corev1.ProtocolTCP,
			})
		}

		ds.Spec = appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					NodeSelector:      probe.Spec.NodeSelector,
					Tolerations:       probe.Spec.Tolerations,
					PriorityClassName: probe.Spec.PriorityClassName,
					Containers:        containers,
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
	dsName := fmt.Sprintf("%s-mesh-agent", probe.Name)
	ds := &appsv1.DaemonSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: dsName, Namespace: probe.Namespace}, ds); err != nil {
		return err
	}

	probe.Status.DaemonSetName = dsName
	probe.Status.MeshSize = ds.Status.DesiredNumberScheduled
	now := metav1.Now()
	probe.Status.LastReconcileTime = &now

	// Sentinel endpoints: list node IPs with HostPort
	if probe.Spec.ExternalSentinel != nil && probe.Spec.ExternalSentinel.Enabled {
		nodeList := &corev1.NodeList{}
		if err := r.List(ctx, nodeList); err == nil {
			hp := probe.Spec.ExternalSentinel.MetricsHostPort
			if hp == 0 {
				hp = 9200
			}
			endpoints := make([]string, 0, len(nodeList.Items))
			for _, node := range nodeList.Items {
				for _, addr := range node.Status.Addresses {
					if addr.Type == corev1.NodeInternalIP {
						endpoints = append(endpoints, fmt.Sprintf("%s:%d", addr.Address, hp))
						break
					}
				}
			}
			probe.Status.SentinelEndpoints = endpoints
		}
	}

	return r.Status().Update(ctx, probe)
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

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
		Named("connectivityprobe").
		Complete(r)
}
