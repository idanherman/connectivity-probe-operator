// Command mesh-agent runs one side of the connectivity mesh.
// A DaemonSet schedules one process per node. Each process discovers the
// others through headless-Service DNS and measures TCP echo latency.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultInterval  = 200 * time.Millisecond
	defaultResolve   = 10 * time.Second
	defaultReconnect = time.Second
	tcpPort          = "8081"
	httpPort         = "8082"
	maxHistory       = 200
	latencyWindow    = 32
)

var defaultLatencyBuckets = []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 1}

var promLabelEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("mesh-agent ")

	agent := newAgent()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !agent.ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"status": "ok", "pod": agent.podName, "node": agent.nodeName})
	})
	mux.HandleFunc("/status", agent.handleStatus)
	mux.HandleFunc("/history", agent.handleHistory)
	mux.HandleFunc("/metrics", agent.handleMetrics)
	mux.HandleFunc("/admin/clear_history", agent.handleClearHistory)

	server := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("http listening on :%s", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	go agent.serveTCP(ctx)
	go agent.discover(ctx)

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

type outage struct {
	PeerIP     string `json:"peer_ip"`
	PeerNode   string `json:"peer_node,omitempty"`
	Started    string `json:"started"`
	Ended      string `json:"ended"`
	DurationMs int64  `json:"duration_ms"`
}

type edge struct {
	state        string
	since        time.Time
	downSince    time.Time
	disconnects  uint64
	peerNode     string
	ok           uint64
	failed       uint64
	latency      []time.Duration
	bucketCounts []uint64
	latencySum   float64
	latencyCount uint64
}

type agent struct {
	podIP        string
	podName      string
	nodeName     string
	peerService  string
	interval     time.Duration
	resolveEvery time.Duration
	reconnect    time.Duration
	buckets      []float64

	mu         sync.Mutex
	tcpReady   bool
	discovered bool
	edges      map[string]*edge
	tasks      map[string]context.CancelFunc
	history    []outage
}

func newAgent() *agent {
	return &agent{
		podIP:        os.Getenv("POD_IP"),
		podName:      envOr("POD_NAME", hostname()),
		nodeName:     envOr("NODE_NAME", hostname()),
		peerService:  os.Getenv("PEER_SERVICE"),
		interval:     parseDurationEnv("PROBE_INTERVAL", defaultInterval),
		resolveEvery: parseDurationEnv("PEER_RESOLVE_INTERVAL", defaultResolve),
		reconnect:    parseDurationEnv("RECONNECT_DELAY", defaultReconnect),
		buckets:      parseBuckets(os.Getenv("LATENCY_BUCKETS")),
		edges:        map[string]*edge{},
		tasks:        map[string]context.CancelFunc{},
	}
}

func (a *agent) serveTCP(ctx context.Context) {
	ln, err := net.Listen("tcp", ":"+tcpPort)
	if err != nil {
		log.Fatalf("tcp listen: %v", err)
	}
	log.Printf("tcp mesh listening on :%s", tcpPort)
	a.mu.Lock()
	a.tcpReady = true
	a.mu.Unlock()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("tcp accept: %v", err)
			continue
		}
		go echoConn(conn)
	}
}

func echoConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, line); err != nil {
			return
		}
	}
}

func (a *agent) discover(ctx context.Context) {
	if a.peerService == "" {
		log.Printf("PEER_SERVICE is empty; serving endpoints without peers")
		<-ctx.Done()
		return
	}
	a.syncPeers(ctx)
	ticker := time.NewTicker(a.resolveEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.stopAll()
			return
		case <-ticker.C:
			a.syncPeers(ctx)
		}
	}
}

func (a *agent) syncPeers(ctx context.Context) {
	ips, err := resolvePeers(a.peerService, a.podIP)
	if err != nil {
		log.Printf("peer resolve %s: %v", a.peerService, err)
		return
	}
	a.mu.Lock()
	a.discovered = true
	a.mu.Unlock()
	want := map[string]struct{}{}
	for _, ip := range ips {
		want[ip] = struct{}{}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for ip, cancel := range a.tasks {
		if _, ok := want[ip]; ok {
			continue
		}
		cancel()
		delete(a.tasks, ip)
		delete(a.edges, ip)
	}
	for ip := range want {
		if _, ok := a.tasks[ip]; ok {
			continue
		}
		peerCtx, cancel := context.WithCancel(ctx)
		a.tasks[ip] = cancel
		if _, ok := a.edges[ip]; !ok {
			a.edges[ip] = a.newEdge()
		}
		go a.probePeer(peerCtx, ip)
	}
}

func (a *agent) stopAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for ip, cancel := range a.tasks {
		cancel()
		delete(a.tasks, ip)
	}
}

func (a *agent) probePeer(ctx context.Context, ip string) {
	for {
		if ctx.Err() != nil {
			return
		}
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip, tcpPort))
		if err != nil {
			a.fail(ip)
			if !sleep(ctx, a.reconnect) {
				return
			}
			continue
		}
		a.learnPeerNode(ip)
		err = a.pingLoop(ctx, conn, ip)
		_ = conn.Close()
		if err != nil && ctx.Err() == nil {
			a.fail(ip)
		}
		if !sleep(ctx, a.reconnect) {
			return
		}
	}
}

func (a *agent) pingLoop(ctx context.Context, conn net.Conn, ip string) error {
	reader := bufio.NewReader(conn)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		deadline := start.Add(max(a.interval*2, 500*time.Millisecond))
		_ = conn.SetDeadline(deadline)
		if _, err := fmt.Fprintf(conn, "ping %d\n", start.UnixNano()); err != nil {
			return err
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return err
		}
		a.success(ip, time.Since(start))
		if !sleep(ctx, a.interval) {
			return ctx.Err()
		}
	}
}

func (a *agent) success(ip string, rtt time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.edges[ip]
	if e == nil {
		return
	}
	if e.state == "disconnected" && !e.downSince.IsZero() {
		ended := time.Now()
		a.history = append(a.history, outage{
			PeerIP:     ip,
			PeerNode:   e.peerNode,
			Started:    e.downSince.UTC().Format(time.RFC3339Nano),
			Ended:      ended.UTC().Format(time.RFC3339Nano),
			DurationMs: ended.Sub(e.downSince).Milliseconds(),
		})
		if len(a.history) > maxHistory {
			a.history = a.history[len(a.history)-maxHistory:]
		}
		e.downSince = time.Time{}
	}
	if e.state != "connected" {
		e.state = "connected"
		e.since = time.Now()
	}
	e.ok++
	e.latencyCount++
	secs := rtt.Seconds()
	e.latencySum += secs
	for i, bound := range a.buckets {
		if secs <= bound {
			e.bucketCounts[i]++
			break
		}
	}
	e.latency = append(e.latency, rtt)
	if len(e.latency) > latencyWindow {
		e.latency = e.latency[len(e.latency)-latencyWindow:]
	}
}

func (a *agent) fail(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.edges[ip]
	if e == nil {
		return
	}
	e.failed++
	if e.state == "connected" {
		e.disconnects++
		e.state = "disconnected"
		e.downSince = time.Now()
		e.since = e.downSince
		return
	}
	if e.state == "" {
		e.state = "disconnected"
		e.since = time.Now()
	}
}

func (a *agent) learnPeerNode(ip string) {
	client := http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(ip, httpPort) + "/status")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var payload struct {
		Self struct {
			NodeName string `json:"node_name"`
		} `json:"self"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return
	}
	if payload.Self.NodeName == "" {
		return
	}
	a.mu.Lock()
	if e := a.edges[ip]; e != nil {
		e.peerNode = payload.Self.NodeName
	}
	a.mu.Unlock()
}

func (a *agent) handleStatus(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	conns := map[string]map[string]any{}
	nodes := map[string]string{}
	totals := map[string]uint64{}
	for ip, e := range a.edges {
		state := e.state
		if state == "" {
			state = "unknown"
		}
		conns[ip] = map[string]any{
			"tcp":        state,
			"since":      e.since.UTC().Format(time.RFC3339Nano),
			"rtt_ms":     latestRTT(e),
			"loss_ratio": lossRatio(e),
		}
		if e.peerNode != "" {
			nodes[ip] = e.peerNode
		}
		totals[ip] = e.disconnects
	}
	writeJSON(w, map[string]any{
		"self": map[string]string{
			"pod_name":  a.podName,
			"pod_ip":    a.podIP,
			"node_name": a.nodeName,
		},
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
		"connections":       conns,
		"peer_nodes":        nodes,
		"disconnect_totals": totals,
	})
}

func (a *agent) handleHistory(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := append([]outage(nil), a.history...)
	writeJSON(w, items)
}

func (a *agent) handleClearHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a.mu.Lock()
	a.history = nil
	a.mu.Unlock()
	writeJSON(w, map[string]any{"ok": true, "pod": a.podName})
}

func (a *agent) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, a.renderMetrics())
}

func (a *agent) renderMetrics() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	src := promEscape(firstNonEmpty(a.nodeName, a.podName, a.podIP, "unknown"))
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP connectivity_probe_peer_info Identity of this mesh agent.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_peer_info gauge\n")
	fmt.Fprintf(&b, "connectivity_probe_peer_info{node=%q,pod=%q,pod_ip=%q} 1\n",
		promEscape(a.nodeName), promEscape(a.podName), promEscape(a.podIP))
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_up 1 when the TCP echo to a peer is succeeding.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_up gauge\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_disconnects_total Disconnects after a successful connect.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_disconnects_total counter\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_loss_ratio Failed probes divided by attempted probes since start.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_loss_ratio gauge\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_probes_total TCP echo attempts.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_probes_total counter\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_probe_failures_total TCP echo attempts that failed.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_probe_failures_total counter\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_rtt_seconds Latest successful TCP echo round-trip.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_rtt_seconds gauge\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_jitter_seconds Mean absolute difference of recent round-trips.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_jitter_seconds gauge\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_tcp_latency_seconds TCP echo round-trip time.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_tcp_latency_seconds histogram\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_edge Edge up (1) or down (0) for the topology panel.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_edge gauge\n")
	fmt.Fprintf(&b, "# HELP connectivity_probe_node 1 when every edge from this node is up, otherwise 0.\n")
	fmt.Fprintf(&b, "# TYPE connectivity_probe_node gauge\n")

	ips := make([]string, 0, len(a.edges))
	for ip := range a.edges {
		ips = append(ips, ip)
	}
	sort.Strings(ips)
	upN, downN := 0, 0
	for _, ip := range ips {
		e := a.edges[ip]
		if e.state == "" {
			continue
		}
		dst := promEscape(firstNonEmpty(e.peerNode, ip))
		ipLabel := promEscape(ip)
		up := 0
		if e.state == "connected" {
			up = 1
		}
		if up == 1 {
			upN++
		} else {
			downN++
		}
		labels := fmt.Sprintf("src_node=%q,dst_node=%q,dst_ip=%q", src, dst, ipLabel)
		fmt.Fprintf(&b, "connectivity_probe_tcp_up{%s} %d\n", labels, up)
		fmt.Fprintf(&b, "connectivity_probe_disconnects_total{%s} %d\n", labels, e.disconnects)
		fmt.Fprintf(&b, "connectivity_probe_tcp_probes_total{%s} %d\n", labels, e.ok+e.failed)
		fmt.Fprintf(&b, "connectivity_probe_tcp_probe_failures_total{%s} %d\n", labels, e.failed)
		fmt.Fprintf(&b, "connectivity_probe_tcp_loss_ratio{%s} %s\n", labels, formatFloat(lossRatio(e)))
		fmt.Fprintf(&b, "connectivity_probe_tcp_rtt_seconds{%s} %s\n", labels, formatFloat(latestSeconds(e)))
		fmt.Fprintf(&b, "connectivity_probe_tcp_jitter_seconds{%s} %s\n", labels, formatFloat(jitter(e)))
		cumulative := uint64(0)
		for i, bound := range a.buckets {
			cumulative += e.bucketCounts[i]
			fmt.Fprintf(&b, "connectivity_probe_tcp_latency_seconds_bucket{%s,le=%q} %d\n",
				labels, formatFloat(bound), cumulative)
		}
		fmt.Fprintf(&b, "connectivity_probe_tcp_latency_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, e.latencyCount)
		fmt.Fprintf(&b, "connectivity_probe_tcp_latency_seconds_sum{%s} %s\n", labels, formatFloat(e.latencySum))
		fmt.Fprintf(&b, "connectivity_probe_tcp_latency_seconds_count{%s} %d\n", labels, e.latencyCount)
		edgeID := promEscape(firstNonEmpty(a.nodeName, a.podName) + "->" + firstNonEmpty(e.peerNode, ip))
		fmt.Fprintf(&b, "connectivity_probe_edge{id=%q,source=%q,target=%q} %d\n", edgeID, src, dst, up)
	}
	health := 1
	if downN > 0 {
		health = 0
	}
	subtitle := fmt.Sprintf("%d up", upN)
	if downN > 0 {
		subtitle = fmt.Sprintf("%d up, %d down", upN, downN)
	}
	fmt.Fprintf(&b, "connectivity_probe_node{id=%q,title=%q,subtitle=%q} %d\n", src, src, subtitle, health)
	return b.String()
}

func (a *agent) newEdge() *edge {
	return &edge{bucketCounts: make([]uint64, len(a.buckets))}
}

func (a *agent) ready() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.tcpReady {
		return false
	}
	if a.peerService == "" {
		return true
	}
	return a.discovered
}

func resolvePeers(host, self string) ([]string, error) {
	ips, err := net.LookupHost(host)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ips))
	seen := map[string]struct{}{}
	for _, ip := range ips {
		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.To4() == nil {
			continue
		}
		ip = parsed.To4().String()
		if ip == self {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		out = append(out, ip)
	}
	sort.Strings(out)
	return out, nil
}

func latestSeconds(e *edge) float64 {
	if len(e.latency) == 0 {
		return 0
	}
	return e.latency[len(e.latency)-1].Seconds()
}

func latestRTT(e *edge) float64 {
	if len(e.latency) == 0 {
		return 0
	}
	return float64(e.latency[len(e.latency)-1].Microseconds()) / 1000
}

func lossRatio(e *edge) float64 {
	total := e.ok + e.failed
	if total == 0 {
		return 0
	}
	return float64(e.failed) / float64(total)
}

func jitter(e *edge) float64 {
	if len(e.latency) < 2 {
		return 0
	}
	var sum float64
	for i := 1; i < len(e.latency); i++ {
		sum += math.Abs(e.latency[i].Seconds() - e.latency[i-1].Seconds())
	}
	return sum / float64(len(e.latency)-1)
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(payload)
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		log.Printf("invalid %s=%q, using %s", key, raw, fallback)
		return fallback
	}
	return d
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func promEscape(value string) string {
	return promLabelEscaper.Replace(value)
}

func parseBuckets(raw string) []float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return append([]float64(nil), defaultLatencyBuckets...)
	}
	parts := strings.Split(raw, ",")
	out := make([]float64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || value <= 0 {
			log.Printf("ignoring invalid LATENCY_BUCKETS %q", raw)
			return append([]float64(nil), defaultLatencyBuckets...)
		}
		out = append(out, value)
	}
	return out
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
