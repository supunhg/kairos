package telemetry

import (
	"context"
	"io"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.26.0"
	apitrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type ExporterType int

const (
	ExporterNone    ExporterType = iota
	ExporterStdout
)

type Config struct {
	ServiceName    string
	ServiceVersion string
	ExporterType   ExporterType
	Writer         io.Writer
	SampleRate     float64
}

func DefaultConfig() Config {
	return Config{
		ServiceName:    "kairos",
		ServiceVersion: "0.1.0",
		ExporterType:   ExporterStdout,
		Writer:         os.Stderr,
		SampleRate:     1.0,
	}
}

type Telemetry struct {
	Tracer   apitrace.Tracer
	config   Config
	provider *sdktrace.TracerProvider
}

func New(cfg Config) (*Telemetry, error) {
	if cfg.ExporterType == ExporterNone {
		return &Telemetry{
			Tracer: noop.NewTracerProvider().Tracer(cfg.ServiceName),
			config: cfg,
		}, nil
	}

	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(cfg.Writer),
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(cfg.ServiceName),
		semconv.ServiceVersionKey.String(cfg.ServiceVersion),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRate))),
	)

	otel.SetTracerProvider(tp)

	return &Telemetry{
		Tracer:   tp.Tracer(cfg.ServiceName),
		config:   cfg,
		provider: tp,
	}, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t.provider != nil {
		return t.provider.Shutdown(ctx)
	}
	return nil
}

func (t *Telemetry) StartSpan(ctx context.Context, name string, opts ...attribute.KeyValue) (context.Context, apitrace.Span) {
	return t.Tracer.Start(ctx, name,
		apitrace.WithAttributes(opts...),
	)
}

func (t *Telemetry) Enabled() bool {
	return t.config.ExporterType != ExporterNone
}
