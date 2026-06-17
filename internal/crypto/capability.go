package crypto

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

type Capability struct {
	Resource    string   `json:"resource"`
	Permissions []string `json:"permissions"`
	Issuer      string   `json:"issuer"`
	Subject     string   `json:"subject,omitempty"`
	Expires     int64    `json:"expires,omitempty"`
}

type CapabilityToken struct {
	Cap   Capability `json:"cap"`
	Sig   []byte     `json:"sig"`
	raw   string
}

func IssueCapability(cap Capability, priv ed25519.PrivateKey) (*CapabilityToken, error) {
	raw := serializeCap(cap)
	sig := ed25519.Sign(priv, []byte(raw))
	return &CapabilityToken{
		Cap: cap,
		Sig: sig,
		raw: raw,
	}, nil
}

func VerifyCapability(token *CapabilityToken, pub ed25519.PublicKey) error {
	if token.Cap.Expires > 0 && time.Now().Unix() > token.Cap.Expires {
		return fmt.Errorf("capability expired")
	}
	raw := serializeCap(token.Cap)
	if !ed25519.Verify(pub, []byte(raw), token.Sig) {
		return fmt.Errorf("invalid capability signature")
	}
	return nil
}

func serializeCap(cap Capability) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d",
		cap.Resource,
		strings.Join(cap.Permissions, ","),
		cap.Issuer,
		cap.Subject,
		cap.Expires,
	)
}

func (t *CapabilityToken) Encode() string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(t.Cap.Resource + "\x00" +
			strings.Join(t.Cap.Permissions, ",") + "\x00" +
			t.Cap.Issuer + "\x00" +
			t.Cap.Subject + "\x00" +
			fmt.Sprintf("%d", t.Cap.Expires) + "\x00" +
			base64.RawURLEncoding.EncodeToString(t.Sig),
		),
	)
}

func DecodeCapabilityToken(encoded string) (*CapabilityToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode token: %w", err)
	}
	parts := strings.SplitN(string(raw), "\x00", 6)
	if len(parts) != 6 {
		return nil, fmt.Errorf("invalid token format")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[5])
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}
	return &CapabilityToken{
		Cap: Capability{
			Resource:    parts[0],
			Permissions: strings.Split(parts[1], ","),
			Issuer:      parts[2],
			Subject:     parts[3],
		},
		Sig: sig,
	}, nil
}
