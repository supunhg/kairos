package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	v1 "github.com/supunhg/kairos/api/v1"
	"github.com/supunhg/kairos/internal/identity"
)

func TestSignAndVerifyEvent(t *testing.T) {
	ident, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	ev := &v1.Event{
		Id:           "test-1",
		CausalDeps:   []string{"dep1", "dep2"},
		PayloadType:  "kairos.v1.TextInsert",
		Payload:      []byte("test payload"),
		HlcTimestamp: 1234567890,
	}

	if err := SignEvent(ident, ev); err != nil {
		t.Fatal(err)
	}

	if ev.Originator != ident.ID() {
		t.Fatalf("expected originator %s, got %s", ident.ID(), ev.Originator)
	}

	if len(ev.Signature) == 0 {
		t.Fatal("expected signature")
	}

	if err := VerifyEvent(ev); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestVerifyTamperedEvent(t *testing.T) {
	ident, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	ev := &v1.Event{
		Id:           "test-1",
		PayloadType:  "kairos.v1.TextInsert",
		Payload:      []byte("test payload"),
		HlcTimestamp: 1234567890,
	}

	if err := SignEvent(ident, ev); err != nil {
		t.Fatal(err)
	}

	ev.Payload = []byte("tampered payload")

	if err := VerifyEvent(ev); err == nil {
		t.Fatal("expected verification failure for tampered event")
	}
}

func TestVerifyMissingSignature(t *testing.T) {
	ev := &v1.Event{
		Id: "test-1",
	}

	if err := VerifyEvent(ev); err == nil {
		t.Fatal("expected verification failure for unsigned event")
	}
}

func TestVerifyEventsBatch(t *testing.T) {
	ident, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	events := []*v1.Event{
		{Id: "1", PayloadType: "kairos.v1.TextInsert", Payload: []byte("a"), HlcTimestamp: 1},
		{Id: "2", PayloadType: "kairos.v1.TextInsert", Payload: []byte("b"), HlcTimestamp: 2},
	}

	for _, ev := range events {
		if err := SignEvent(ident, ev); err != nil {
			t.Fatal(err)
		}
	}

	if err := VerifyEvents(events); err != nil {
		t.Fatalf("batch verification failed: %v", err)
	}
}

func TestCapabilityTokenRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub := priv.Public().(ed25519.PublicKey)

	cap := Capability{
		Resource:    "session/test-session",
		Permissions: []string{"read", "write"},
		Issuer:      "node1",
		Subject:     "node2",
		Expires:     0,
	}

	token, err := IssueCapability(cap, priv)
	if err != nil {
		t.Fatal(err)
	}

	if err := VerifyCapability(token, pub); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestCapabilityTokenTampered(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	cap := Capability{
		Resource:    "session/test-session",
		Permissions: []string{"read", "write"},
		Issuer:      "node1",
	}

	token, err := IssueCapability(cap, priv)
	if err != nil {
		t.Fatal(err)
	}

	if err := VerifyCapability(token, otherPub); err == nil {
		t.Fatal("expected verification failure with wrong public key")
	}
}

func TestCapabilityTokenExpired(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub := priv.Public().(ed25519.PublicKey)

	cap := Capability{
		Resource:    "session/test-session",
		Permissions: []string{"read"},
		Issuer:      "node1",
		Expires:     1,
	}

	token, err := IssueCapability(cap, priv)
	if err != nil {
		t.Fatal(err)
	}

	if err := VerifyCapability(token, pub); err == nil {
		t.Fatal("expected verification failure for expired token")
	}
}

func TestCapabilityTokenEncodeDecode(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub := priv.Public().(ed25519.PublicKey)

	cap := Capability{
		Resource:    "session/test-session",
		Permissions: []string{"read", "write"},
		Issuer:      "node1",
		Subject:     "node2",
	}

	token, err := IssueCapability(cap, priv)
	if err != nil {
		t.Fatal(err)
	}

	encoded := token.Encode()
	decoded, err := DecodeCapabilityToken(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Cap.Resource != cap.Resource {
		t.Fatalf("resource mismatch: %s vs %s", decoded.Cap.Resource, cap.Resource)
	}

	if err := VerifyCapability(decoded, pub); err != nil {
		t.Fatalf("verification failed after decode: %v", err)
	}
}

func TestNonceGeneration(t *testing.T) {
	n1, err := GenerateNonce()
	if err != nil {
		t.Fatal(err)
	}
	n2, err := GenerateNonce()
	if err != nil {
		t.Fatal(err)
	}
	if n1 == n2 {
		t.Fatal("expected unique nonces")
	}
}
