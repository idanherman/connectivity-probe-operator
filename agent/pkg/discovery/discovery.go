// Package discovery implements DNS-based peer discovery for the mesh agent.
// It periodically resolves the headless Service to find all peer pod IPs,
// detects additions/removals, and notifies the mesh manager.
package discovery
