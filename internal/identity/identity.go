// Package identity provides ed25519 key management and file persistence.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
)

type Identity struct {
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
	id         string
}

func Generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	id := base64.RawURLEncoding.EncodeToString(pub)
	return &Identity{
		PublicKey:  pub,
		PrivateKey: priv,
		id:         id,
	}, nil
}

func (i *Identity) ID() string {
	return i.id
}

func (i *Identity) Sign(data []byte) []byte {
	return ed25519.Sign(i.PrivateKey, data)
}

func (i *Identity) Verify(data []byte, sig []byte) bool {
	return ed25519.Verify(i.PublicKey, data, sig)
}

func (i *Identity) MarshalPrivate() ([]byte, error) {
	encoded := base64.RawURLEncoding.EncodeToString(i.PrivateKey)
	return []byte(encoded), nil
}

func (i *Identity) MarshalPublic() string {
	return base64.RawURLEncoding.EncodeToString(i.PublicKey)
}

func UnmarshalPrivate(data []byte) (*Identity, error) {
	raw, err := base64.RawURLEncoding.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode private key: %w", err)
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key length: %d", len(raw))
	}
	priv := ed25519.PrivateKey(raw)
	pub := priv.Public().(ed25519.PublicKey)
	id := base64.RawURLEncoding.EncodeToString(pub)
	return &Identity{
		PublicKey:  pub,
		PrivateKey: priv,
		id:         id,
	}, nil
}

func IdentityFromPublicKey(pub ed25519.PublicKey) string {
	return base64.RawURLEncoding.EncodeToString(pub)
}

func SaveIdentityFile(path string, ident *Identity) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := ident.MarshalPrivate()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func LoadIdentityFile(path string) (*Identity, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: identity file path from known location
	if err != nil {
		return nil, err
	}
	return UnmarshalPrivate(data)
}

func DefaultIdentityPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".kairos/identity.key"
	}
	return filepath.Join(home, ".kairos", "identity.key")
}
