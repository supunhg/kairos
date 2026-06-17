package sync

import (
	"context"
	"testing"
	"time"

	"github.com/kairos-io/kairos-go/api/v1"
)

func TestTextInsertAndContent(t *testing.T) {
	e := NewEngine("node1")
	ctx := context.Background()

	ev, err := e.TextInsert(ctx, "doc1", 0, "Hello")
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event")
	}

	content := e.TextContent("doc1")
	if content != "Hello" {
		t.Fatalf("expected 'Hello', got '%s'", content)
	}
}

func TestSequentialInserts(t *testing.T) {
	e := NewEngine("node1")
	ctx := context.Background()

	e.TextInsert(ctx, "doc1", 0, "Hel")
	e.TextInsert(ctx, "doc1", 3, "lo World")

	content := e.TextContent("doc1")
	if content != "Hello World" {
		t.Fatalf("expected 'Hello World', got '%s'", content)
	}
}

func TestApplyRemoteEvent(t *testing.T) {
	a := NewEngine("nodeA")
	b := NewEngine("nodeB")
	ctx := context.Background()

	ev, err := a.TextInsert(ctx, "shared", 0, "Hello from A")
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Apply(ctx, []*v1.Event{ev}); err != nil {
		t.Fatal(err)
	}

	content := b.TextContent("shared")
	if content != "Hello from A" {
		t.Fatalf("expected 'Hello from A', got '%s'", content)
	}
}

func TestSubscribe(t *testing.T) {
	e := NewEngine("node1")
	ctx := context.Background()

	received := make(chan *v1.Event, 1)
	unsub := e.Subscribe("doc1", func(ev *v1.Event) {
		received <- ev
	})
	defer unsub()

	e.TextInsert(ctx, "doc1", 0, "test")

	select {
	case ev := <-received:
		if ev.PayloadType != "kairos.v1.TextInsert" {
			t.Fatalf("expected TextInsert, got %s", ev.PayloadType)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestVersionVector(t *testing.T) {
	e := NewEngine("node1")
	ctx := context.Background()

	e.TextInsert(ctx, "doc1", 0, "a")
	e.TextInsert(ctx, "doc1", 1, "b")

	vv := e.GetVersionVector("doc1")
	if ts, ok := vv["node1"]; !ok || ts == 0 {
		t.Fatal("expected version vector entry for node1")
	}
}

func TestConcurrentEdits(t *testing.T) {
	a := NewEngine("nodeA")
	b := NewEngine("nodeB")
	ctx := context.Background()

	// Both nodes edit different groups, then exchange events
	evA, _ := a.TextInsert(ctx, "shared", 0, "Hello from A")
	evB, _ := b.TextInsert(ctx, "shared", 0, "Hello from B")

	if err := b.Apply(ctx, []*v1.Event{evA}); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(ctx, []*v1.Event{evB}); err != nil {
		t.Fatal(err)
	}
}
