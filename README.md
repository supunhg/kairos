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
│  Transport    QUIC (primary)                │
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
client.Connect(ctx, "192.168.1.10:8443")

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
├── internal/
│   ├── crdt/               # CRDT types (RGA, LWWMap, GCounter, etc.)
│   ├── eventlog/           # Append-only event store
│   ├── persistence/        # Snapshots, replay, manifests
│   ├── sync/               # Sync engine (events ↔ CRDTs)
│   ├── transport/quic/     # QUIC transport (quic-go)
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

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | ✅ | Event log, WAL, persistence, snapshots |
| 2 | ✅ | CRDT types + sync engine + subscriptions |
| 3 | ✅ | QUIC transport |
| 4 | ✅ | Go SDK + CLI |
| 5 | 🔜 | Security (Ed25519, E2E encryption) |
| 6 | 🔜 | Agent runtime (memory, blackboards) |
| 7 | 🔜 | Observability (tracing, metrics, replay) |

## License

MIT
