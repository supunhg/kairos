package telemetry

import (
	"context"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	EventsTotal    *prometheus.CounterVec
	EventsLatency  *prometheus.HistogramVec
	ActiveGroups   prometheus.Gauge
	GroupsTotal    prometheus.Counter
	SnapshotsTotal prometheus.Counter
	MessagesSent   *prometheus.CounterVec
	MessagesRecv   *prometheus.CounterVec
	ActivePeers    prometheus.Gauge
	Registry       *prometheus.Registry
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		EventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kairos_events_total",
				Help: "Total events processed by type",
			},
			[]string{"type", "node"},
		),
		EventsLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "kairos_events_latency_seconds",
				Help:    "Event processing latency",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"type"},
		),
		ActiveGroups: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "kairos_groups_active",
				Help: "Number of active groups",
			},
		),
		GroupsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "kairos_groups_total",
				Help: "Total groups created",
			},
		),
		SnapshotsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "kairos_snapshots_total",
				Help: "Total snapshots taken",
			},
		),
		MessagesSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kairos_messages_sent_total",
				Help: "Total messages sent by type",
			},
			[]string{"type"},
		),
		MessagesRecv: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kairos_messages_received_total",
				Help: "Total messages received by type",
			},
			[]string{"type"},
		),
		ActivePeers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "kairos_peers_active",
				Help: "Number of active peer connections",
			},
		),
		Registry: reg,
	}
	reg.MustRegister(
		m.EventsTotal,
		m.EventsLatency,
		m.ActiveGroups,
		m.GroupsTotal,
		m.SnapshotsTotal,
		m.MessagesSent,
		m.MessagesRecv,
		m.ActivePeers,
	)
	return m
}

type MetricsServer struct {
	server *http.Server
	mu     sync.Mutex
}

func NewMetricsServer(addr string, m *Metrics) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return &MetricsServer{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (s *MetricsServer) Start() error {
	return s.server.ListenAndServe()
}

func (s *MetricsServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *MetricsServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return s.server.Addr
	}
	return ""
}

func EventTypeLabel(payloadType string) string {
	switch payloadType {
	case "kairos.v1.TextInsert":
		return "text_insert"
	case "kairos.v1.TextDelete":
		return "text_delete"
	case "kairos.v1.MapSet":
		return "map_set"
	case "kairos.v1.MapDelete":
		return "map_delete"
	default:
		return "unknown"
	}
}
