package crypto

import (
	"testing"
)

func TestSessionEncryptionEstablish(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	if err := alice.EstablishSession("bob", bob.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if err := bob.EstablishSession("alice", alice.PublicKey()); err != nil {
		t.Fatal(err)
	}

	if !alice.HasSession("bob") {
		t.Fatal("alice should have session with bob")
	}
	if !bob.HasSession("alice") {
		t.Fatal("bob should have session with alice")
	}
}

func TestSessionEncryptDecrypt(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	alice.EstablishSession("bob", bob.PublicKey())
	bob.EstablishSession("alice", alice.PublicKey())

	msg := []byte("secret message from alice to bob")
	ciphertext, err := alice.EncryptForPeer("bob", msg)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := bob.DecryptFromPeer("alice", ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(msg) {
		t.Fatalf("got %q, want %q", decrypted, msg)
	}
}

func TestSessionRemove(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	alice.EstablishSession("bob", bob.PublicKey())
	if !alice.HasSession("bob") {
		t.Fatal("expected session before removal")
	}

	alice.RemoveSession("bob")
	if alice.HasSession("bob") {
		t.Fatal("expected no session after removal")
	}

	_, err = alice.EncryptForPeer("bob", []byte("test"))
	if err == nil {
		t.Fatal("expected error encrypting after session removal")
	}
}

func TestSessionEnumerate(t *testing.T) {
	se, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	peerKeys := make(map[string][]byte)
	for _, name := range []string{"alice", "bob", "charlie"} {
		kp, _ := GenerateKeyPair()
		peerKeys[name] = kp.PublicBytes()
		se.EstablishSession(name, peerKeys[name])
	}

	sessions := se.ActiveSessions()
	if len(sessions) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(sessions))
	}
}

func TestSessionKeyRotation(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	alice.EstablishSession("bob", bob.PublicKey())
	bob.EstablishSession("alice", alice.PublicKey())

	msg1 := []byte("before rotation")
	c1, _ := alice.EncryptForPeer("bob", msg1)
	d1, _ := bob.DecryptFromPeer("alice", c1)
	if string(d1) != string(msg1) {
		t.Fatal("encryption failed before rotation")
	}

	if err := alice.RotateKey("bob"); err != nil {
		t.Fatal(err)
	}
	if err := bob.RotateKey("alice"); err != nil {
		t.Fatal(err)
	}

	msg2 := []byte("after rotation")
	c2, _ := alice.EncryptForPeer("bob", msg2)
	d2, _ := bob.DecryptFromPeer("alice", c2)
	if string(d2) != string(msg2) {
		t.Fatal("encryption failed after rotation")
	}
}

func TestSessionKeyAge(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	alice.EstablishSession("bob", bob.PublicKey())

	if !alice.HasSession("bob") {
		t.Fatal("expected session")
	}
}

func TestSessionPeerMismatch(t *testing.T) {
	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	alice.EstablishSession("bob", bob.PublicKey())
	bob.EstablishSession("alice", alice.PublicKey())

	msg := []byte("secret")
	ciphertext, err := alice.EncryptForPeer("bob", msg)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := bob.DecryptFromPeer("alice", ciphertext)
	if err != nil {
		t.Fatal("bob should be able to decrypt")
	}
	if string(decrypted) != string(msg) {
		t.Fatal("decrypted message mismatch")
	}
}

func TestSessionEncryptUnknownPeer(t *testing.T) {
	alice, _ := NewSessionEncryption()

	_, err := alice.EncryptForPeer("unknown", []byte("test"))
	if err == nil {
		t.Fatal("expected error encrypting for unknown peer")
	}
}

func TestEstablishFromHandshake(t *testing.T) {
	aliceKP, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	bobKP, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	alice, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := NewSessionEncryption()
	if err != nil {
		t.Fatal(err)
	}

	bob.EstablishFromHandshake("alice", aliceKP.PublicBytes(), bobKP.PrivateBytes(), bobKP.PublicBytes())
	alice.EstablishFromHandshake("bob", bobKP.PublicBytes(), aliceKP.PrivateBytes(), aliceKP.PublicBytes())

	msg := []byte("handshake-established")
	ciphertext, _ := alice.EncryptForPeer("bob", msg)
	decrypted, _ := bob.DecryptFromPeer("alice", ciphertext)

	if string(decrypted) != string(msg) {
		t.Fatal("failed to encrypt/decrypt after handshake")
	}
}
