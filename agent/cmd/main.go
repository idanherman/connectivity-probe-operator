// Package main is the entrypoint for the connectivity-probe mesh agent.
// Each instance runs as a DaemonSet pod, one per node, forming a full TCP mesh
// with all other agents for continuous sub-second connectivity probing.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("connectivity-probe-agent starting...")
	fmt.Printf("  POD_IP=%s  NODE_NAME=%s  NAMESPACE=%s\n",
		os.Getenv("POD_IP"),
		os.Getenv("NODE_NAME"),
		os.Getenv("NAMESPACE"),
	)
	fmt.Println("TODO: implement mesh agent — see agent/pkg/ for planned packages")

	// Planned startup sequence:
	// 1. Parse config from env vars (PROBE_INTERVAL, PEER_SERVICE, etc.)
	// 2. Start HTTP server (:8082) — /metrics, /status, /healthz, /readyz
	// 3. Start WebSocket server (:8080) — ping/pong for external probes
	// 4. Start TCP mesh listener (:8081)
	// 5. Begin peer discovery loop (DNS resolve headless Service)
	// 6. For each discovered peer, launch probe goroutine (TCP echo at PROBE_INTERVAL)
	// 7. Aggregate latency histograms, jitter, loss rate per edge
	// 8. Expose aggregated metrics for Prometheus scrape
	select {}
}
