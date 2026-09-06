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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --------------------------------------------------------------------------
// Spec: desired state
// --------------------------------------------------------------------------

// ConnectivityProbeSpec defines the desired state of a connectivity probe mesh.
type ConnectivityProbeSpec struct {

	// Image is the container image for the mesh agent DaemonSet.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// ContinuousProbes configures the always-on lightweight probing mesh.
	// +optional
	ContinuousProbes ContinuousProbeSpec `json:"continuousProbes,omitempty"`

	// BandwidthTests configures optional scheduled iPerf3 bandwidth tests.
	// +optional
	BandwidthTests *BandwidthTestSpec `json:"bandwidthTests,omitempty"`

	// ExternalSentinel configures HostPort exposure for out-of-cluster scrapers
	// that must keep monitoring even when the API server is unavailable.
	// +optional
	ExternalSentinel *ExternalSentinelSpec `json:"externalSentinel,omitempty"`

	// NodeSelector restricts which nodes run the mesh agent.
	// An empty selector (the default) schedules on every node.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow the mesh agent to schedule on tainted nodes
	// (e.g. control-plane, infra).
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Resources for the mesh agent container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// PriorityClassName for the mesh agent pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// Metrics configures Prometheus metrics exposure and ServiceMonitor.
	// +optional
	Metrics MetricsSpec `json:"metrics,omitempty"`

	// Alerting configures PrometheusRule thresholds.
	// +optional
	Alerting *AlertingSpec `json:"alerting,omitempty"`
}

// ContinuousProbeSpec defines parameters for the always-on probe mesh.
type ContinuousProbeSpec struct {
	// Interval between probes. Must be >= 50ms.
	// +kubebuilder:default="200ms"
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s)$`
	Interval string `json:"interval,omitempty"`

	// Protocols to probe. At least one required.
	// +kubebuilder:default={"tcp"}
	// +kubebuilder:validation:MinItems=1
	Protocols []ProbeProtocol `json:"protocols,omitempty"`

	// ReconnectDelay before retrying a failed peer connection.
	// +kubebuilder:default="1s"
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s)$`
	ReconnectDelay string `json:"reconnectDelay,omitempty"`

	// PeerResolveInterval controls how often the agent re-resolves the
	// headless Service DNS to discover new/removed peers.
	// +kubebuilder:default="10s"
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m)$`
	PeerResolveInterval string `json:"peerResolveInterval,omitempty"`
}

// ProbeProtocol is a supported probe type.
// +kubebuilder:validation:Enum=tcp;http;websocket
type ProbeProtocol string

const (
	ProbeProtocolTCP       ProbeProtocol = "tcp"
	ProbeProtocolHTTP      ProbeProtocol = "http"
	ProbeProtocolWebSocket ProbeProtocol = "websocket"
)

// BandwidthTestSpec configures scheduled iPerf3 tests.
type BandwidthTestSpec struct {
	// Enabled toggles bandwidth testing on or off.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// Schedule is a cron expression (e.g. "0 */4 * * *").
	// +kubebuilder:default="0 */4 * * *"
	Schedule string `json:"schedule,omitempty"`

	// MaxBandwidth caps each iPerf3 test (e.g. "100M").
	// +kubebuilder:default="100M"
	MaxBandwidth string `json:"maxBandwidth,omitempty"`

	// Concurrency is the number of node-pairs tested simultaneously.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	Concurrency int32 `json:"concurrency,omitempty"`

	// TestDuration per node-pair.
	// +kubebuilder:default="10s"
	TestDuration string `json:"testDuration,omitempty"`

	// UDPTest enables an additional UDP jitter/loss test per pair.
	// +kubebuilder:default=false
	UDPTest bool `json:"udpTest,omitempty"`
}

// ExternalSentinelSpec configures HostPort-based access for an external
// observer that monitors the mesh independently of the Kubernetes API.
type ExternalSentinelSpec struct {
	// Enabled toggles HostPort exposure.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// MetricsHostPort is the host port for the Prometheus metrics endpoint.
	// +kubebuilder:default=9200
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=65535
	MetricsHostPort int32 `json:"metricsHostPort,omitempty"`

	// HealthHostPort is the host port for the health/status endpoint.
	// +kubebuilder:default=9201
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=65535
	HealthHostPort int32 `json:"healthHostPort,omitempty"`
}

// MetricsSpec configures Prometheus integration.
type MetricsSpec struct {
	// Enabled toggles Prometheus metrics exposition on the agent.
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// ScrapeInterval for the auto-managed ServiceMonitor.
	// +kubebuilder:default="15s"
	ScrapeInterval string `json:"scrapeInterval,omitempty"`

	// LatencyBuckets for the probe-latency histogram (seconds).
	// +kubebuilder:default={0.0005,0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1.0}
	LatencyBuckets []string `json:"latencyBuckets,omitempty"`
}

// AlertingSpec configures auto-managed PrometheusRule thresholds.
type AlertingSpec struct {
	// Enabled toggles PrometheusRule generation.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled"`

	// ConsecutiveFailures before firing a connectivity alert.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	// LatencyWarningThreshold fires a warning when p99 exceeds this (e.g. "50ms").
	// +optional
	LatencyWarningThreshold string `json:"latencyWarningThreshold,omitempty"`

	// LatencyCriticalThreshold fires a critical alert when p99 exceeds this (e.g. "200ms").
	// +optional
	LatencyCriticalThreshold string `json:"latencyCriticalThreshold,omitempty"`

	// MeshHealthPercentThreshold fires when mesh health drops below this value.
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MeshHealthPercentThreshold int32 `json:"meshHealthPercentThreshold,omitempty"`
}

// --------------------------------------------------------------------------
// Status: observed state
// --------------------------------------------------------------------------

// ConnectivityProbeStatus defines the observed state of the mesh.
type ConnectivityProbeStatus struct {

	// MeshSize is the number of nodes participating in the mesh.
	MeshSize int32 `json:"meshSize,omitempty"`

	// EdgesTotal is the total number of edges (node-pairs) in the mesh.
	EdgesTotal int32 `json:"edgesTotal,omitempty"`

	// EdgesUp is the number of edges currently healthy.
	EdgesUp int32 `json:"edgesUp,omitempty"`

	// EdgesDown is the number of edges currently failing.
	EdgesDown int32 `json:"edgesDown,omitempty"`

	// MeshHealthPercent is EdgesUp / EdgesTotal * 100.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MeshHealthPercent int32 `json:"meshHealthPercent,omitempty"`

	// SentinelEndpoints lists node-ip:port pairs available for external scraping.
	// Populated only when spec.externalSentinel.enabled is true.
	// +optional
	SentinelEndpoints []string `json:"sentinelEndpoints,omitempty"`

	// DaemonSetName is the name of the managed DaemonSet.
	DaemonSetName string `json:"daemonSetName,omitempty"`

	// LastReconcileTime is the timestamp of the last successful reconciliation.
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// Conditions represent the latest available observations of the resource's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition types for ConnectivityProbe.
const (
	// ConditionMeshHealthy indicates all mesh edges are up.
	ConditionMeshHealthy = "MeshHealthy"
	// ConditionDaemonSetReady indicates the DaemonSet has reached desired count.
	ConditionDaemonSetReady = "DaemonSetReady"
	// ConditionDegraded indicates partial mesh failure.
	ConditionDegraded = "Degraded"
)

// --------------------------------------------------------------------------
// Root types
// --------------------------------------------------------------------------

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cprobe
// +kubebuilder:printcolumn:name="Mesh Size",type=integer,JSONPath=`.status.meshSize`
// +kubebuilder:printcolumn:name="Edges Up",type=integer,JSONPath=`.status.edgesUp`
// +kubebuilder:printcolumn:name="Edges Down",type=integer,JSONPath=`.status.edgesDown`
// +kubebuilder:printcolumn:name="Health %",type=integer,JSONPath=`.status.meshHealthPercent`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ConnectivityProbe is the Schema for the connectivityprobes API.
// It declares a full-mesh connectivity monitoring deployment across cluster nodes.
type ConnectivityProbe struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConnectivityProbeSpec   `json:"spec,omitempty"`
	Status ConnectivityProbeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ConnectivityProbeList contains a list of ConnectivityProbe.
type ConnectivityProbeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ConnectivityProbe `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ConnectivityProbe{}, &ConnectivityProbeList{})
}
