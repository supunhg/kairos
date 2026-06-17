package telemetry

import (
	"context"
	"time"

	"github.com/supunhg/kairos/internal/sync"
	"go.opentelemetry.io/otel/attribute"
)

type Instrumentation struct {
	tel     *Telemetry
	metrics *Metrics
	attrs   []attribute.KeyValue
}

func NewInstrumentation(tel *Telemetry, metrics *Metrics) *Instrumentation {
	return &Instrumentation{
		tel:     tel,
		metrics: metrics,
	}
}

func (inst *Instrumentation) EventCreated(ctx context.Context, groupID, eventType string) {
	label := EventTypeLabel(eventType)
	inst.metrics.EventsTotal.WithLabelValues(label, "local").Inc()

	if !inst.tel.Enabled() {
		return
	}
	_, span := inst.tel.StartSpan(ctx, "event.create",
		attribute.String("group_id", groupID),
		attribute.String("event_type", eventType),
	)
	defer span.End()
}

func (inst *Instrumentation) EventApplied(ctx context.Context, groupID, eventType string) {
	label := EventTypeLabel(eventType)
	inst.metrics.EventsTotal.WithLabelValues(label, "remote").Inc()

	if !inst.tel.Enabled() {
		return
	}
	_, span := inst.tel.StartSpan(ctx, "event.apply",
		attribute.String("group_id", groupID),
		attribute.String("event_type", eventType),
	)
	defer span.End()
}

func (inst *Instrumentation) GroupCreated(_ context.Context, groupID string, groupType sync.GroupType) {
	inst.metrics.ActiveGroups.Inc()
	inst.metrics.GroupsTotal.Inc()
}

func (inst *Instrumentation) SnapshotTaken(_ context.Context, groupID string, eventCount int) {
	// Snapshot metric not yet implemented
}

type timedEvent struct {
	typ   string
	start time.Time
}

func (inst *Instrumentation) recordLatency(eventType string, start time.Time) {
	label := EventTypeLabel(eventType)
	inst.metrics.EventsLatency.WithLabelValues(label).Observe(time.Since(start).Seconds())
}
