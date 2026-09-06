# connectivity-probe-operator

A Kubernetes operator that deploys and manages a full-mesh connectivity monitoring system across cluster nodes with sub-second probe sensitivity.

## Overview

**connectivity-probe-operator** provides always-on, real-time visibility into node-to-node network health inside a Kubernetes/OpenShift cluster. It deploys a lightweight DaemonSet mesh where every agent continuously probes every other agent, measuring **latency**, **jitter**, **packet loss**, and **connectivity state** at sub-second intervals.

Think of it as a production-grade evolution of `openshift-network-diagnostics` — but with full N×N mesh topology, sub-second sensitivity, and optional bandwidth testing via iPerf3.

### Key Features

- **Full mesh architecture** — every node probes every other node (N×N), not hub-and-spoke
- **Sub-second probing** — configurable down to 50ms intervals for TCP/HTTP echo probes
- **Prometheus-native** — exposes latency histograms, jitter, loss, and connectivity gauges; auto-manages ServiceMonitor and PrometheusRule resources
- **External sentinel mode** — HostPort-based metrics exposure that survives API server outages, enabling external monitoring that works when the cluster itself is degraded
- **Scheduled bandwidth tests** — optional iPerf3 sweeps with bandwidth caps and sequential execution to avoid noisy-neighbor impact
- **Operator-managed lifecycle** — CRD-driven: a single `ConnectivityProbe` resource controls the entire deployment (DaemonSet, Services, ServiceMonitor, PrometheusRule)
- **Minimal resource footprint** — designed to run at 10m CPU / 32Mi memory per node without impacting workloads

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  ConnectivityProbe Operator (this controller)            │
│  - Watches ConnectivityProbe CR                          │
│  - Reconciles DaemonSet, headless Service, ServiceMonitor│
│  - Maintains sentinel endpoint list in CR .status        │
└──────────────────────┬───────────────────────────────────┘
                       │ owns
        ┌──────────────┼──────────────────┐
        ▼              ▼                  ▼
   DaemonSet      ServiceMonitor    PrometheusRule
   (mesh agent)   (15s scrape)      (alert thresholds)
        │
        ├── Continuous: TCP/HTTP probes @ configurable interval
        ├── Exposes: latency histograms, jitter, loss, connectivity
        ├── Scheduled: iPerf3 bandwidth tests (optional)
        └── HostPort: API-server-independent sentinel access
```

### Mesh Agent

Each agent (one per node via DaemonSet) performs:

1. **Peer discovery** — resolves the headless Service DNS to find all other agents
2. **TCP mesh** — maintains persistent connections to every peer, sends echo probes at the configured interval
3. **Metrics aggregation** — computes p50/p95/p99 latency, jitter, and loss over a rolling window
4. **Prometheus exposition** — serves `/metrics` for in-cluster Prometheus scraping
5. **HostPort sentinel** — optionally exposes metrics on a HostPort for external scrapers that bypass kube-proxy

## Quick Start

### Prerequisites

- Go 1.27+
- Access to a Kubernetes 1.28+ cluster
- `kubectl` or `oc` configured

### Install the CRD and deploy the operator

```sh
make install
make deploy IMG=<your-registry>/connectivity-probe-operator:latest
```

### Create a ConnectivityProbe

```sh
kubectl apply -f config/samples/monitoring_v1alpha1_connectivityprobe.yaml
```

### Check mesh health

```sh
kubectl get connectivityprobes
# NAME                MESH SIZE   EDGES UP   EDGES DOWN   HEALTH %   AGE
# connectivity-mesh   5           20         0            100        2m
```

## Configuration

See [`config/samples/monitoring_v1alpha1_connectivityprobe.yaml`](config/samples/monitoring_v1alpha1_connectivityprobe.yaml) for a fully commented example CR.

## Project Structure

```
├── api/v1alpha1/          # CRD type definitions (ConnectivityProbe)
├── internal/controller/   # Operator reconciliation logic
├── agent/                 # Mesh agent (DaemonSet workload)
│   ├── cmd/               # Agent entrypoint
│   └── pkg/
│       ├── discovery/     # DNS-based peer discovery
│       ├── mesh/          # TCP probe mesh implementation
│       └── metrics/       # Prometheus metrics exposition
├── config/                # Kustomize manifests (CRD, RBAC, samples)
└── Dockerfile             # Operator image
```

## License

Copyright 2026. Licensed under the Apache License, Version 2.0.
