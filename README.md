# KAIROS Go

**Durable State. Timeless Runtime.**

A distributed stateful runtime platform built in Go — treat synchronized, durable application state as a first-class primitive.

## Architecture

```
┌────────────────────────────────────────────┐
│  SDK           Reactive API (pkg/sdk)       │
├────────────────────────────────────────────┤
│  Sync Engine   CRDTs + subscriptions        │
├────────────────────────────────────────────┤
│  Event Log    Append-only + WAL + Index     │
├────────────────────────────────────────────┤
│  Persistence  Snapshots + Replay + Manifest │
├────────────────────────────────────────────┤
│  Transport    QUIC (primary) + WebSocket    │
├────────────────────────────────────────────┤
│  Security     Ed25519 + E2E encryption      │
├────────────────────────────────────────────┤
│  Observability  OpenTelemetry + Prometheus   │
└────────────────────────────────────────────┘
```

Full spec: [ARCHITECTURE.md](./ARCHITECTURE.md)

## Quick Start

```bash
# Build
go build -o kairos ./cmd/kairos

# Terminal 1 — Node A
./kairos --node nodeA --addr :8443

# Terminal 2 — Node B (connects to A)
./kairos --node nodeB --addr :8444 --peer localhost:8443

# Type in either terminal — changes sync in real-time
```

## SDK Usage

```go
import kairos "github.com/supunhg/kairos/pkg/sdk"

client := kairos.New("my-node")
if err := client.Connect(ctx, "192.168.1.10:8443"); err != nil {
    log.Fatal(err)
}

sess, _ := client.Join(ctx, "workspace:project-1")
doc, _ := sess.Document(ctx, "spec.md")

// Local edit → syncs to peers
doc.Insert(ctx, 0, "Hello world")

// Subscribe to remote changes
unsub := doc.Subscribe(ctx, func(ev *kairos.Event) {
    text := doc.Text(ctx)
    log.Printf("Doc: %s", text)
})
defer unsub()
```

## Development

```bash
# Dependencies
go mod tidy

# Generate protobuf
task proto          # or: protoc --go_out=api/v1 --go_opt=paths=source_relative -I api api/v1/event.proto

# Run all tests
go test -race -count=1 ./...

# Run with verbose output
go test -race -count=1 -v ./...

# Build CLI
go build -o kairos ./cmd/kairos

# Lint (requires golangci-lint)
golangci-lint run

# CI pipeline (tidy → vet → test → build)
task ci
```

## Project Structure

```
├── api/v1/                 # Protobuf schema + generated Go
├── cmd/kairos/             # CLI entry point
├── cmd/kairos-replay/      # Event replay CLI tool
├── internal/
│   ├── agent/              # Agent runtime (memory, blackboards, supervisor)
│   ├── crdt/               # CRDT types (RGA, LWWMap, GCounter, etc.)
│   ├── crypto/             # Event signing, capability tokens, E2E encryption
│   ├── eventlog/           # Append-only event store
│   ├── identity/           # Ed25519 identity management
│   ├── persistence/        # Snapshots, replay, manifests, crash recovery
│   ├── sync/               # Sync engine (events ↔ CRDTs)
│   ├── telemetry/          # OpenTelemetry tracing + Prometheus metrics
│   ├── transport/          # Transport abstraction + registry
│   │   ├── quic/           # QUIC transport (quic-go)
│   │   └── websocket/      # WebSocket transport (raw TCP)
│   └── wal/                # Write-ahead log
├── pkg/sdk/                # Public API
└── examples/               # Example applications
```

## CRDT Types

| Type | Description |
|------|-------------|
| `LWWRegister` | Last-Writer-Wins Register |
| `GCounter` | Grow-Only Counter |
| `PNCounter` | Positive-Negative Counter |
| `LWWMap` | Last-Writer-Wins Map |
| `RGA` | Replicated Growable Array (text) |

## Features

| Feature | Status | Description |
|---------|--------|-------------|
| Event sourcing | ✅ | Append-only event log with WAL durability |
| CRDT sync | ✅ | Conflict-free replicated data types |
| QUIC transport | ✅ | Primary transport with TLS + TOFU |
| WebSocket transport | ✅ | Fallback transport over TCP |
| Ed25519 identity | ✅ | Node identity + event signing |
| E2E encryption | ✅ | X25519 key exchange + AES-256-GCM |
| Capability tokens | ✅ | Issue/verify access tokens |
| Agent runtime | ✅ | Memory, blackboards, supervisor |
| Observability | ✅ | OpenTelemetry + Prometheus metrics |
| Crash recovery | ✅ | WAL replay + manifest reconciliation |
| Snapshots | ✅ | Periodic state snapshots with gzip |

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | ✅ | Event log, WAL, persistence, snapshots |
| 2 | ✅ | CRDT types + sync engine + subscriptions |
| 3 | ✅ | QUIC + WebSocket transports + registry |
| 4 | ✅ | Go SDK + CLI |
| 5 | ✅ | Security (Ed25519, event signing, capability tokens, TLS persistence) |
| 6 | ✅ | Agent runtime (memory, blackboards, supervisor) |
| 7 | ✅ | Observability (tracing, metrics, replay CLI) |
| 8 | ✅ | Crash recovery + WAL replay + manifest reconciliation |
| 9 | 🔲 | Chaos testing (Jepsen-style), fuzzing, soak tests |
| 10 | 🔲 | Schema-driven delta compression |
| 11 | 🔲 | Reactive sync primitives in SDK |

## License

MIT
