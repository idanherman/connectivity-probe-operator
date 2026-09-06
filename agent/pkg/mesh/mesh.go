// Package mesh implements the TCP probe mesh between agent pods.
// Each agent maintains a persistent TCP connection to every peer,
// sending echo probes at the configured sub-second interval and
// tracking per-edge latency, jitter, and connection state.
package mesh
