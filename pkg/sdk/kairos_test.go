package kairos

import (
	"context"
	"testing"
)

func TestNewClient(t *testing.T) {
	c := New("test-node")
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.nodeID != "test-node" {
		t.Fatalf("expected test-node, got %s", c.nodeID)
	}
	c.Close()
}

func TestJoinAndDocument(t *testing.T) {
	c := New("test-node")
	sess, err := c.Join(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.ID() != "session-1" {
		t.Fatalf("expected session-1, got %s", sess.ID())
	}

	doc, err := sess.Document(context.Background(), "doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if doc.ID() != "session-1/doc-1" {
		t.Fatalf("expected session-1/doc-1, got %s", doc.ID())
	}

	c.Close()
}

func TestTextInsertAndRead(t *testing.T) {
	c := New("test-node")
	defer c.Close()

	ctx := context.Background()
	sess, _ := c.Join(ctx, "test-session")
	doc, _ := sess.Document(ctx, "test-doc")

	ev, err := doc.Insert(ctx, 0, "Hello")
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.PayloadType != "kairos.v1.TextInsert" {
		t.Fatalf("expected TextInsert, got %s", ev.PayloadType)
	}

	text := doc.Text(ctx)
	if text != "Hello" {
		t.Fatalf("expected 'Hello', got '%s'", text)
	}
}

func TestSubscribe(t *testing.T) {
	c := New("test-node")
	defer c.Close()

	ctx := context.Background()
	sess, _ := c.Join(ctx, "sub-session")
	doc, _ := sess.Document(ctx, "sub-doc")

	received := make(chan *Event, 1)
	unsub := doc.Subscribe(ctx, func(ev *Event) {
		received <- ev
	})
	defer unsub()

	doc.Insert(ctx, 0, "test-event")

	select {
	case ev := <-received:
		if ev == nil {
			t.Fatal("expected event")
		}
	default:
		t.Fatal("expected event via subscribe")
	}
}

func TestMultipleDocuments(t *testing.T) {
	c := New("test-node")
	defer c.Close()

	ctx := context.Background()
	sess, _ := c.Join(ctx, "multi-session")

	doc1, _ := sess.Document(ctx, "doc-1")
	doc2, _ := sess.Document(ctx, "doc-2")

	doc1.Insert(ctx, 0, "Text A")
	doc2.Insert(ctx, 0, "Text B")

	if doc1.Text(ctx) != "Text A" {
		t.Fatalf("expected 'Text A', got '%s'", doc1.Text(ctx))
	}
	if doc2.Text(ctx) != "Text B" {
		t.Fatalf("expected 'Text B', got '%s'", doc2.Text(ctx))
	}
}
