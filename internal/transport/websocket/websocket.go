package websocket

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/supunhg/kairos/internal/transport"
)

func init() {
	transport.Register("ws", func() transport.Transport { return &WebSocketTransport{} })
}

type WebSocketTransport struct{}

type wsConn struct {
	conn    net.Conn
	closeCh chan struct{}
	mu      sync.Mutex
}

type wsListener struct {
	ln    net.Listener
	conns chan transport.Connection
}

type wsFrame struct {
	Type    transport.MessageType `json:"t"`
	GroupID string                `json:"g,omitempty"`
	Payload []byte                `json:"p,omitempty"`
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsUpgrade(conn net.Conn, client bool) error {
	if client {
		key := base64.RawStdEncoding.EncodeToString([]byte("kairoswsclient0000"))
		req := fmt.Sprintf("GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", key)
		if _, err := conn.Write([]byte(req)); err != nil {
			return err
		}

		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		resp := string(buf[:n])
		if !strings.Contains(resp, "101") {
			return fmt.Errorf("websocket upgrade failed: %s", strings.Split(resp, "\r\n")[0])
		}
		return nil
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	req := string(buf[:n])

	if !strings.Contains(req, "Upgrade: websocket") && !strings.Contains(req, "Upgrade: Websocket") {
		return fmt.Errorf("not a websocket upgrade request")
	}

	key := extractWSKey(req)
	if key == "" {
		return fmt.Errorf("no Sec-WebSocket-Key found")
	}

	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	if _, err := conn.Write([]byte(resp)); err != nil {
		return err
	}

	return nil
}

func extractWSKey(req string) string {
	for _, line := range strings.Split(req, "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-key:") {
			return strings.TrimSpace(line[len("sec-websocket-key:"):])
		}
	}
	return ""
}

func newWsConn(conn net.Conn) *wsConn {
	return &wsConn{
		conn:    conn,
		closeCh: make(chan struct{}),
	}
}

func (c *wsConn) Send(ctx context.Context, msg transport.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	frame := wsFrame{
		Type:    msg.Type,
		GroupID: msg.GroupID,
		Payload: msg.Payload,
	}
	data, err := encodeWSFrame(frame)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(data)
	return err
}

func (c *wsConn) Receive(ctx context.Context) (transport.Message, error) {
	for {
		frame, err := readWSFrame(c.conn)
		if err != nil {
			return transport.Message{}, err
		}

		if frame.opcode == 8 {
			return transport.Message{}, fmt.Errorf("connection closed")
		}
		if frame.opcode == 9 {
			pong := []byte{0x8A, 0x02, frame.payload[0], frame.payload[1]}
			c.mu.Lock()
			c.conn.Write(pong)
			c.mu.Unlock()
			continue
		}
		if frame.opcode == 10 {
			continue
		}

		var msg wsFrame
		if err := json.Unmarshal(frame.payload, &msg); err != nil {
			return transport.Message{}, err
		}
		return transport.Message{
			Type:    msg.Type,
			GroupID: msg.GroupID,
			Payload: msg.Payload,
		}, nil
	}
}

func (c *wsConn) Close() error {
	close(c.closeCh)
	return c.conn.Close()
}

func (l *wsListener) Accept(ctx context.Context) (transport.Connection, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *wsListener) Close() error {
	return l.ln.Close()
}

func (l *wsListener) Addr() net.Addr {
	return l.ln.Addr()
}

type wsRawFrame struct {
	opcode  byte
	payload []byte
}

func readWSFrame(conn net.Conn) (*wsRawFrame, error) {
	header := make([]byte, 2)
	if _, err := conn.Read(header); err != nil {
		return nil, err
	}

	opcode := header[0] & 0x0F
	masked := (header[1] & 0x80) != 0
	length := uint64(header[1] & 0x7F)

	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := conn.Read(ext); err != nil {
			return nil, err
		}
		length = uint64(ext[0])<<8 | uint64(ext[1])
	case 127:
		ext := make([]byte, 8)
		if _, err := conn.Read(ext); err != nil {
			return nil, err
		}
		length = 0
		for i := 0; i < 8; i++ {
			length = length<<8 | uint64(ext[i])
		}
	}

	var mask [4]byte
	if masked {
		if _, err := conn.Read(mask[:]); err != nil {
			return nil, err
		}
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := conn.Read(payload); err != nil {
			return nil, err
		}
	}

	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}

	return &wsRawFrame{opcode: opcode, payload: payload}, nil
}

func encodeWSFrame(msg wsFrame) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	length := len(data)

	var header []byte
	header = append(header, 0x81)

	switch {
	case length <= 125:
		header = append(header, byte(length))
	case length <= 65535:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		header = append(header, 127)
		for i := 7; i >= 0; i-- {
			header = append(header, byte(length>>(8*i)))
		}
	}

	return append(header, data...), nil
}

func (t *WebSocketTransport) Dial(ctx context.Context, addr string) (transport.Connection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	if err := wsUpgrade(conn, true); err != nil {
		conn.Close()
		return nil, err
	}

	return newWsConn(conn), nil
}

func (t *WebSocketTransport) Listen(ctx context.Context, addr string) (transport.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	l := &wsListener{
		ln:    ln,
		conns: make(chan transport.Connection, 64),
	}

	go l.acceptLoop()

	return l, nil
}

func (l *wsListener) acceptLoop() {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}

		if err := wsUpgrade(conn, false); err != nil {
			conn.Close()
			continue
		}

		l.conns <- newWsConn(conn)
	}
}


