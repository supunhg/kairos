package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/supunhg/kairos/api/v1"
	"github.com/supunhg/kairos/internal/identity"
)

func SignEvent(ident *identity.Identity, ev *v1.Event) error {
	data := eventSignBytes(ev)
	sig := ed25519.Sign(ident.PrivateKey, data)
	ev.Signature = sig
	ev.Originator = ident.ID()
	return nil
}

func VerifyEvent(ev *v1.Event) error {
	if len(ev.Signature) == 0 {
		return fmt.Errorf("event %s: missing signature", ev.Id)
	}
	pub, err := parsePublicKey(ev.Originator)
	if err != nil {
		return fmt.Errorf("event %s: %w", ev.Id, err)
	}
	data := eventSignBytes(ev)
	if !ed25519.Verify(pub, data, ev.Signature) {
		return fmt.Errorf("event %s: invalid signature", ev.Id)
	}
	return nil
}

func VerifyEvents(events []*v1.Event) error {
	for _, ev := range events {
		if err := VerifyEvent(ev); err != nil {
			return err
		}
	}
	return nil
}

func eventSignBytes(ev *v1.Event) []byte {
	parts := []string{
		ev.Id,
		strings.Join(ev.CausalDeps, ","),
		ev.PayloadType,
		string(ev.Payload),
		fmt.Sprintf("%d", ev.HlcTimestamp),
	}
	return []byte(strings.Join(parts, "|"))
}

func parsePublicKey(originator string) (ed25519.PublicKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(originator)
	if err != nil {
		return nil, fmt.Errorf("decode originator: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key length: %d", len(raw))
	}
	return ed25519.PublicKey(raw), nil
}

func GenerateNonce() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
