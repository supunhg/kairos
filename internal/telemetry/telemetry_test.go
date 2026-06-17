package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/supunhg/kairos/internal/sync"
)

func TestTelemetryNoopExporter(t *testing.T) {
	cfg := Config{
		ServiceName:  "test",
		ExporterType: ExporterNone,
	}
	tel, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	if tel.Enabled() {
		t.Fatal("expected telemetry to be disabled")
	}
}

func TestTelemetryStdoutExporter(t *testing.T) {
	var buf strings.Builder
	cfg := Config{
		ServiceName:    "test",
		ServiceVersion: "0.1.0",
		ExporterType:   ExporterStdout,
		Writer:         &buf,
	}
	tel, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	if !tel.Enabled() {
		t.Fatal("expected telemetry to be enabled")
	}

	_, span := tel.StartSpan(context.Background(), "test-span")
	span.End()

	time.Sleep(100 * time.Millisecond)
}

func TestInstrumentationWithEngine(t *testing.T) {
	tel, err := New(Config{ExporterType: ExporterNone})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	metrics := NewMetrics()
	inst := NewInstrumentation(tel, metrics)

	engine := sync.NewEngine("test-node",
		sync.WithTelemetry(inst),
	)
	ctx := context.Background()

	_, _ = engine.TextInsert(ctx, "doc1", 0, "Hello")
	_, _ = engine.MapSet(ctx, "map1", "key", "value")

	// Metrics should have been incremented
	if metrics.EventsTotal == nil {
		t.Fatal("nil metrics")
	}
}

func TestMetricsNew(t *testing.T) {
	m := NewMetrics()
	if m.Registry == nil {
		t.Fatal("expected registry")
	}
	// Verify all metrics are registered
	metrics, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) == 0 {
		t.Fatal("expected at least one metric")
	}
}

func TestEventTypeLabel(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"kairos.v1.TextInsert", "text_insert"},
		{"kairos.v1.TextDelete", "text_delete"},
		{"kairos.v1.MapSet", "map_set"},
		{"kairos.v1.MapDelete", "map_delete"},
		{"unknown.type", "unknown"},
	}
	for _, c := range cases {
		got := EventTypeLabel(c.input)
		if got != c.want {
			t.Errorf("EventTypeLabel(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}
