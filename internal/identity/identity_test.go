package identity

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateIdentity(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if len(id.PublicKey) != ed25519.PublicKeySize {
		t.Fatalf("expected %d byte public key, got %d", ed25519.PublicKeySize, len(id.PublicKey))
	}
	if len(id.PrivateKey) != ed25519.PrivateKeySize {
		t.Fatalf("expected %d byte private key, got %d", ed25519.PrivateKeySize, len(id.PrivateKey))
	}
	if id.ID() == "" {
		t.Fatal("expected non-empty identity string")
	}
}

func TestSignAndVerify(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world")
	sig := id.Sign(data)
	if len(sig) == 0 {
		t.Fatal("expected signature")
	}
	if !id.Verify(data, sig) {
		t.Fatal("signature verification failed")
	}

	// Wrong data should fail
	if id.Verify([]byte("wrong data"), sig) {
		t.Fatal("verification should fail for wrong data")
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}

	data, err := id.MarshalPrivate()
	if err != nil {
		t.Fatal(err)
	}

	restored, err := UnmarshalPrivate(data)
	if err != nil {
		t.Fatal(err)
	}

	if id.ID() != restored.ID() {
		t.Fatalf("identity ID mismatch: %s vs %s", id.ID(), restored.ID())
	}

	if !id.Verify([]byte("test"), restored.Sign([]byte("test"))) {
		t.Fatal("restored identity should produce verifiable signatures")
	}
}

func TestSaveLoadIdentityFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.key")

	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveIdentityFile(path, id); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadIdentityFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if id.ID() != loaded.ID() {
		t.Fatalf("ID mismatch after load: %s vs %s", id.ID(), loaded.ID())
	}
}

func TestIdentityFilePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.key")

	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveIdentityFile(path, id); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions, got %o", info.Mode().Perm())
	}
}
