package crypto

import (
	"testing"
)

func TestKeyExchange(t *testing.T) {
	kp1, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	kp2, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	secret1, err := kp1.ComputeShared(kp2.public)
	if err != nil {
		t.Fatal(err)
	}
	secret2, err := kp2.ComputeShared(kp1.public)
	if err != nil {
		t.Fatal(err)
	}

	if len(secret1.secret) != 32 {
		t.Fatalf("expected 32-byte shared secret, got %d", len(secret1.secret))
	}
	if string(secret1.secret) != string(secret2.secret) {
		t.Fatal("shared secrets don't match")
	}
}

func TestKeyExchangeFromBytes(t *testing.T) {
	kp1, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	kp2, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	secret1, err := kp1.ComputeSharedFromBytes(kp2.PublicBytes())
	if err != nil {
		t.Fatal(err)
	}
	secret2, err := kp2.ComputeSharedFromBytes(kp1.PublicBytes())
	if err != nil {
		t.Fatal(err)
	}

	if string(secret1.secret) != string(secret2.secret) {
		t.Fatal("shared secrets don't match from bytes")
	}
}

func TestKeyPairSerialization(t *testing.T) {
	kp1, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	pubBytes := kp1.PublicBytes()
	privBytes := kp1.PrivateBytes()

	kp2, err := NewKeyPairFromPrivate(privBytes)
	if err != nil {
		t.Fatal(err)
	}

	if string(pubBytes) != string(kp2.PublicBytes()) {
		t.Fatal("public keys don't match after restore")
	}
}

func TestPublicKeyFormat(t *testing.T) {
	kp, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	b64 := kp.PublicBase64()
	if len(b64) == 0 {
		t.Fatal("expected non-empty base64")
	}

	pub, err := NewPublicKey(kp.PublicBytes())
	if err != nil {
		t.Fatal(err)
	}
	if string(pub.Bytes()) != string(kp.PublicBytes()) {
		t.Fatal("public key roundtrip failed")
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	plaintext := []byte("hello world, this is a secret message")

	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(plaintext) {
		t.Fatalf("decrypted text doesn't match: got %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptDecryptString(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	original := "secret-data-123"
	encoded, err := EncryptString(original, key)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecryptString(encoded, key)
	if err != nil {
		t.Fatal(err)
	}

	if decoded != original {
		t.Fatalf("got %q, want %q", decoded, original)
	}
}

func TestEncryptWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 1

	plaintext := []byte("secret")
	ciphertext, err := Encrypt(plaintext, key1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Decrypt(ciphertext, key2)
	if err == nil {
		t.Fatal("expected decryption to fail with wrong key")
	}
}

func TestEncryptLargeMessage(t *testing.T) {
	key := make([]byte, 32)

	plaintext := make([]byte, 1024*1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatal(err)
	}

	if len(decrypted) != len(plaintext) {
		t.Fatalf("length mismatch: %d vs %d", len(decrypted), len(plaintext))
	}
}

func TestComputeSharedSecretDeterministic(t *testing.T) {
	kp1, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	kp2, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	secret1a, _ := kp1.ComputeShared(kp2.public)
	secret1b, _ := kp1.ComputeShared(kp2.public)

	if string(secret1a.secret) != string(secret1b.secret) {
		t.Fatal("shared secret should be deterministic")
	}
}

func TestGenerateSalt(t *testing.T) {
	salt1, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	salt2, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}

	if len(salt1) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(salt1))
	}
	if string(salt1) == string(salt2) {
		t.Fatal("salts should be unique")
	}
}

func TestEndToEndKeyExchangeAndEncrypt(t *testing.T) {
	alice, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	bob, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	aliceSecret, _ := alice.ComputeShared(bob.public)
	bobSecret, _ := bob.ComputeShared(alice.public)

	message := []byte("E2E encrypted message from Alice to Bob")

	ciphertext, err := Encrypt(message, aliceSecret.secret)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := Decrypt(ciphertext, bobSecret.secret)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(message) {
		t.Fatalf("E2E roundtrip failed: got %q, want %q", decrypted, message)
	}
}
