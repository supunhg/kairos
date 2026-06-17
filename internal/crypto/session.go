package crypto

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

type SessionEncryption struct {
	mu       sync.RWMutex
	keyPair  *EcdhKeyPair
	sessions map[string]*SessionKey
}

type SessionKey struct {
	peerID      string
	sharedKey   []byte
	created     time.Time
	keyRotation time.Duration
}

func NewSessionEncryption() (*SessionEncryption, error) {
	kp, err := GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	return &SessionEncryption{
		keyPair:  kp,
		sessions: make(map[string]*SessionKey),
	}, nil
}

func (se *SessionEncryption) PublicKey() []byte {
	return se.keyPair.PublicBytes()
}

func (se *SessionEncryption) PublicKeyBase64() string {
	return se.keyPair.PublicBase64()
}

func (se *SessionEncryption) EstablishSession(peerID string, peerPublicKey []byte) error {
	pub, err := NewPublicKey(peerPublicKey)
	if err != nil {
		return fmt.Errorf("invalid peer public key: %w", err)
	}

	secret, err := se.keyPair.ComputeShared(pub)
	if err != nil {
		return fmt.Errorf("compute shared secret: %w", err)
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	se.sessions[peerID] = &SessionKey{
		peerID:      peerID,
		sharedKey:   secret.secret,
		created:     time.Now(),
		keyRotation: 24 * time.Hour,
	}
	return nil
}

func (se *SessionEncryption) EncryptForPeer(peerID string, plaintext []byte) ([]byte, error) {
	sk, err := se.getSession(peerID)
	if err != nil {
		return nil, err
	}
	return Encrypt(plaintext, sk.sharedKey)
}

func (se *SessionEncryption) DecryptFromPeer(peerID string, ciphertext []byte) ([]byte, error) {
	sk, err := se.getSession(peerID)
	if err != nil {
		return nil, err
	}
	return Decrypt(ciphertext, sk.sharedKey)
}

func (se *SessionEncryption) EstablishFromHandshake(peerID string, theirPublic, myPrivate, myPublic []byte) error {
	priv, err := NewKeyPairFromPrivate(myPrivate)
	if err != nil {
		return err
	}
	pub, err := NewPublicKey(theirPublic)
	if err != nil {
		return err
	}
	secret, err := priv.ComputeShared(pub)
	if err != nil {
		return err
	}

	se.mu.Lock()
	defer se.mu.Unlock()
	se.sessions[peerID] = &SessionKey{
		peerID:    peerID,
		sharedKey: secret.secret,
		created:   time.Now(),
	}
	return nil
}

func (se *SessionEncryption) getSession(peerID string) (*SessionKey, error) {
	se.mu.RLock()
	sk, ok := se.sessions[peerID]
	se.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no session with peer: %s", peerID)
	}
	return sk, nil
}

func (se *SessionEncryption) RemoveSession(peerID string) {
	se.mu.Lock()
	defer se.mu.Unlock()
	delete(se.sessions, peerID)
}

func (se *SessionEncryption) HasSession(peerID string) bool {
	se.mu.RLock()
	defer se.mu.RUnlock()
	_, ok := se.sessions[peerID]
	return ok
}

func (se *SessionEncryption) ActiveSessions() []string {
	se.mu.RLock()
	defer se.mu.RUnlock()
	var ids []string
	for id := range se.sessions {
		ids = append(ids, id)
	}
	return ids
}

func (se *SessionEncryption) RotateKey(peerID string) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	sk, ok := se.sessions[peerID]
	if !ok {
		return fmt.Errorf("no session with peer: %s", peerID)
	}

	h := sha256.Sum256(sk.sharedKey)
	copy(sk.sharedKey, h[:])
	sk.created = time.Now()
	return nil
}

func (e *SessionEncryption) PrivateKey() []byte {
	return e.keyPair.PrivateBytes()
}

func (sk *SessionKey) Key() []byte {
	return sk.sharedKey
}

func (sk *SessionKey) Age() time.Duration {
	return time.Since(sk.created)
}

func (sk *SessionKey) NeedsRotation(maxAge time.Duration) bool {
	return sk.Age() > maxAge
}
