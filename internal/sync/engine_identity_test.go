package sync

import (
	"context"
	"testing"

	v1 "github.com/supunhg/kairos/api/v1"
	"github.com/supunhg/kairos/internal/crypto"
	"github.com/supunhg/kairos/internal/identity"
)

func createTestEvent(groupID, originator, payloadType string, payload []byte) *v1.Event {
	return &v1.Event{
		Id:           groupID + "-test",
		GroupId:      groupID,
		Originator:   originator,
		PayloadType:  payloadType,
		Payload:      payload,
		HlcTimestamp: 1,
	}
}

type testSigner struct {
	*identity.Identity
}

func (s *testSigner) Sign(event *v1.Event) error {
	return crypto.SignEvent(s.Identity, event)
}

type testVerifier struct{}

func (testVerifier) Verify(event *v1.Event) error {
	return crypto.VerifyEvent(event)
}

func TestEngineWithSigning(t *testing.T) {
	ident, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine("node1",
		WithSigner(&testSigner{ident}),
		WithVerifier(testVerifier{}),
	)
	ctx := context.Background()

	ev, err := e.TextInsert(ctx, "doc1", 0, "Hello")
	if err != nil {
		t.Fatal(err)
	}

	if ev.Originator != ident.ID() {
		t.Fatalf("expected originator %s, got %s", ident.ID(), ev.Originator)
	}

	if len(ev.Signature) == 0 {
		t.Fatal("expected event to be signed")
	}

	content := e.TextContent("doc1")
	if content != "Hello" {
		t.Fatalf("expected 'Hello', got '%s'", content)
	}
}

func TestEngineRejectsUnsignedEvent(t *testing.T) {
	ident, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine("node1",
		WithSigner(&testSigner{ident}),
		WithVerifier(testVerifier{}),
	)
	ctx := context.Background()

	_, err = e.TextInsert(ctx, "doc1", 0, "Hello")
	if err != nil {
		t.Fatal(err)
	}

	// Create an unsigned event
	unsignedEv := createTestEvent("doc1", "node2", "kairos.v1.TextInsert", []byte("fake"))

	err = e.Apply(ctx, []*v1.Event{unsignedEv})
	if err == nil {
		t.Fatal("expected Apply to reject unsigned event")
	}
}

func TestEngineRejectsTamperedEvent(t *testing.T) {
	signerIdent, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	_, err = identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	e := NewEngine("node1",
		WithSigner(&testSigner{signerIdent}),
		WithVerifier(testVerifier{}),
	)
	ctx := context.Background()

	ev, err := e.TextInsert(ctx, "doc1", 0, "Hello")
	if err != nil {
		t.Fatal(err)
	}

	ev.Payload = []byte("tampered")

	// Apply with engine that has verify but different signer
	e2 := NewEngine("node2",
		WithVerifier(testVerifier{}),
	)
	err = e2.Apply(ctx, []*v1.Event{ev})
	if err == nil {
		t.Fatal("expected Apply to reject tampered event")
	}
}

func TestEngineRemoteSyncWithSigning(t *testing.T) {
	identA, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	identB, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	a := NewEngine("nodeA",
		WithSigner(&testSigner{identA}),
		WithVerifier(testVerifier{}),
	)
	b := NewEngine("nodeB",
		WithSigner(&testSigner{identB}),
		WithVerifier(testVerifier{}),
	)
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
