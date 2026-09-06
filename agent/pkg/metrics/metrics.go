// Package metrics implements Prometheus metrics exposition for the mesh agent.
// Metrics are aggregated from sub-second probe results into histograms and
// gauges suitable for 15s Prometheus scrape intervals.
//
// Exposed metrics:
//   - connectivity_probe_peer_info        (gauge)  — agent identity
//   - connectivity_probe_tcp_up           (gauge)  — per-edge up/down
//   - connectivity_probe_tcp_latency      (histogram) — per-edge RTT
//   - connectivity_probe_tcp_jitter       (gauge)  — per-edge jitter
//   - connectivity_probe_tcp_loss_ratio   (gauge)  — per-edge loss rate
//   - connectivity_probe_disconnects_total (counter) — per-edge disconnect count
//   - connectivity_probe_edge             (gauge)  — convenience metric for Grafana Node Graph
package metrics
