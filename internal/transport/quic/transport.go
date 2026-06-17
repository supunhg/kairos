// Package quic provides QUIC-based transport for peer communication.
package quic

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/supunhg/kairos/internal/transport"
)

type MessageType = transport.MessageType

const (
	MsgEvent       = transport.MsgEvent
	MsgSyncReq     = transport.MsgSyncReq
	MsgSyncResp    = transport.MsgSyncResp
	MsgPing        = transport.MsgPing
	MsgPong        = transport.MsgPong
	MsgJoin        = transport.MsgJoin
	MsgLeave       = transport.MsgLeave
	MsgKeyExchange = transport.MsgKeyExchange
)

type Message = transport.Message

type Listener struct {
	ln *quic.Listener
}

type Conn struct {
	conn   *quic.Conn
	stream *quic.Stream
	mu     sync.Mutex
}

func Listen(ctx context.Context, addr string) (*Listener, error) {
	tlsConf := generateTLSConfig()
	ln, err := quic.ListenAddr(addr, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln}, nil
}

func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	conn, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		_ = conn.CloseWithError(1, "")
		return nil, err
	}
	return &Conn{conn: conn, stream: stream}, nil
}

func (l *Listener) Close() error {
	return l.ln.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.ln.Addr()
}

var (
	tofuMu    sync.Mutex
	tofuStore = make(map[string]string)
)

func Dial(ctx context.Context, addr string) (*Conn, error) {
	tlsConf := &tls.Config{
		//nolint:gosec // TOFU: trust-on-first-use with VerifyPeerCertificate pinning
		InsecureSkipVerify: true,
		NextProtos:         []string{"kairos"},
	}

	//nolint:gosec // G123: session resumption disabled for TOFU
	tlsConf.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return nil
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		fingerprint := sha256.Sum256(cert.Raw)
		fp := hex.EncodeToString(fingerprint[:])

		tofuMu.Lock()
		known, exists := tofuStore[addr]
		if !exists {
			tofuStore[addr] = fp
			tofuMu.Unlock()
			return nil
		}
		tofuMu.Unlock()

		if fp != known {
			return fmt.Errorf("certificate mismatch for %s (possible MITM)", addr)
		}
		return nil
	}

	conn, err := quic.DialAddr(ctx, addr, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(1, "")
		return nil, err
	}
	return &Conn{conn: conn, stream: stream}, nil
}

func (c *Conn) Send(ctx context.Context, msg Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	groupIDLen := uint16(len(msg.GroupID)) //nolint:gosec // safe: groupID length bounded by protocol
	totalLen := 1 + 2 + int(groupIDLen) + 4 + len(msg.Payload)
	buf := make([]byte, totalLen)

	buf[0] = byte(msg.Type)
	binary.BigEndian.PutUint16(buf[1:3], groupIDLen)
	copy(buf[3:3+groupIDLen], msg.GroupID)
	binary.BigEndian.PutUint32(buf[3+groupIDLen:7+groupIDLen], uint32(len(msg.Payload))) //nolint:gosec // safe: payload bounded by protocol
	copy(buf[7+groupIDLen:], msg.Payload)

	_, err := c.stream.Write(buf)
	return err
}

func (c *Conn) Receive(ctx context.Context) (Message, error) {
	var typeGroupLen [3]byte
	if _, err := io.ReadFull(c.stream, typeGroupLen[:]); err != nil {
		return Message{}, err
	}

	msgType := MessageType(typeGroupLen[0])
	groupIDLen := binary.BigEndian.Uint16(typeGroupLen[1:3])

	groupID := make([]byte, groupIDLen)
	if groupIDLen > 0 {
		if _, err := io.ReadFull(c.stream, groupID); err != nil {
			return Message{}, err
		}
	}

	var payloadLenBuf [4]byte
	if _, err := io.ReadFull(c.stream, payloadLenBuf[:]); err != nil {
		return Message{}, err
	}
	payloadLen := binary.BigEndian.Uint32(payloadLenBuf[:])

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.stream, payload); err != nil {
			return Message{}, err
		}
	}

	return Message{
		Type:    msgType,
		GroupID: string(groupID),
		Payload: payload,
	}, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != nil {
		_ = c.stream.Close()
	}
	_ = c.conn.CloseWithError(0, "")
	return nil
}

func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"kairos"},
		MinVersion:   tls.VersionTLS13,
	}
}

func LoadOrGenerateTLSConfig(path string) (*tls.Config, error) {
	if path != "" {
		if data, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: TLS cert from known path
			block, _ := pem.Decode(data)
			if block != nil {
				if cert, err := tls.X509KeyPair(data, data); err == nil {
					return &tls.Config{
						Certificates: []tls.Certificate{cert},
						NextProtos:   []string{"kairos"},
						MinVersion:   tls.VersionTLS13,
					}, nil
				}
			}
		}
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
	if path != "" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0700); err == nil {
			certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
			keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
			_ = os.WriteFile(path, append(certPEM, keyPEM...), 0600)
		}
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"kairos"},
		MinVersion:   tls.VersionTLS13,
	}, nil
}
