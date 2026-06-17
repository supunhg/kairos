package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

type EcdhKeyPair struct {
	private *ecdh.PrivateKey
	public  *ecdh.PublicKey
}

type SharedSecret struct {
	secret []byte
}

func GenerateKeyPair() (*EcdhKeyPair, error) {
	curve := ecdh.X25519()
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	return &EcdhKeyPair{
		private: priv,
		public:  priv.PublicKey(),
	}, nil
}

func (kp *EcdhKeyPair) PublicBytes() []byte {
	return kp.public.Bytes()
}

func (kp *EcdhKeyPair) PublicBase64() string {
	return base64.RawURLEncoding.EncodeToString(kp.public.Bytes())
}

func (kp *EcdhKeyPair) PrivateBytes() []byte {
	return kp.private.Bytes()
}

func NewKeyPairFromPrivate(privBytes []byte) (*EcdhKeyPair, error) {
	curve := ecdh.X25519()
	priv, err := curve.NewPrivateKey(privBytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return &EcdhKeyPair{
		private: priv,
		public:  priv.PublicKey(),
	}, nil
}

func NewPublicKey(pubBytes []byte) (*ecdh.PublicKey, error) {
	curve := ecdh.X25519()
	return curve.NewPublicKey(pubBytes)
}

func ComputeSharedSecret(priv *ecdh.PrivateKey, pub *ecdh.PublicKey) (*SharedSecret, error) {
	secret, err := priv.ECDH(pub)
	if err != nil {
		return nil, fmt.Errorf("compute shared secret: %w", err)
	}
	return &SharedSecret{secret: secret}, nil
}

func (kp *EcdhKeyPair) ComputeShared(pub *ecdh.PublicKey) (*SharedSecret, error) {
	return ComputeSharedSecret(kp.private, pub)
}

func (kp *EcdhKeyPair) ComputeSharedFromBytes(pubBytes []byte) (*SharedSecret, error) {
	pub, err := NewPublicKey(pubBytes)
	if err != nil {
		return nil, err
	}
	return kp.ComputeShared(pub)
}

func Encrypt(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}

	ciphertext := aesGCM.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ciphertext...), nil
}

func Decrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plaintext, nil
}

func EncryptString(plaintext string, key []byte) (string, error) {
	data, err := Encrypt([]byte(plaintext), key)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func DecryptString(encoded string, key []byte) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	plain, err := Decrypt(data, key)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func GenerateSalt() ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return salt, nil
}
