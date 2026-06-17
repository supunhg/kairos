package websocket

import (
	"context"
	"testing"
	"time"

	"github.com/supunhg/kairos/internal/transport"
)

func TestWebSocketTransportDialListen(t *testing.T) {
	ws := &WebSocketTransport{}

	ln, err := ws.Listen(context.Background(), ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			return
		}

		msg, err := conn.Receive(context.Background())
		if err != nil {
			conn.Close()
			return
		}
		conn.Send(context.Background(), transport.Message{
			Type:    transport.MsgEvent,
			GroupID: msg.GroupID,
			Payload: append(msg.Payload, []byte(" reply")...),
		})
		conn.Close()
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := ws.Dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = conn.Send(context.Background(), transport.Message{
		Type:    transport.MsgEvent,
		GroupID: "doc1",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}

	msg, err := conn.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if msg.GroupID != "doc1" {
		t.Fatalf("expected groupID 'doc1', got '%s'", msg.GroupID)
	}
	if string(msg.Payload) != "hello reply" {
		t.Fatalf("expected 'hello reply', got '%s'", string(msg.Payload))
	}
}

func TestWebSocketMultipleMessages(t *testing.T) {
	ws := &WebSocketTransport{}

	ln, err := ws.Listen(context.Background(), ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			return
		}

		for {
			msg, err := conn.Receive(context.Background())
			if err != nil {
				conn.Close()
				return
			}
			conn.Send(context.Background(), transport.Message{
				Type:    msg.Type,
				GroupID: msg.GroupID,
				Payload: msg.Payload,
			})
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := ws.Dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 3; i++ {
		err := conn.Send(context.Background(), transport.Message{
			Type:    transport.MsgSyncReq,
			GroupID: "test",
			Payload: []byte("msg"),
		})
		if err != nil {
			t.Fatal(err)
		}

		msg, err := conn.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if msg.GroupID != "test" {
			t.Fatalf("expected 'test', got '%s'", msg.GroupID)
		}
	}
}

func TestWebSocketMessageTypes(t *testing.T) {
	ws := &WebSocketTransport{}

	ln, err := ws.Listen(context.Background(), ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			return
		}
		for {
			msg, err := conn.Receive(context.Background())
			if err != nil {
				conn.Close()
				return
			}
			conn.Send(context.Background(), msg)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := ws.Dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tests := []transport.MessageType{
		transport.MsgEvent,
		transport.MsgSyncReq,
		transport.MsgSyncResp,
		transport.MsgPing,
		transport.MsgPong,
		transport.MsgJoin,
		transport.MsgLeave,
		transport.MsgKeyExchange,
	}

	for _, mt := range tests {
		err := conn.Send(context.Background(), transport.Message{
			Type:    mt,
			GroupID: "test",
			Payload: []byte("data"),
		})
		if err != nil {
			t.Fatalf("send type %d: %v", mt, err)
		}
		msg, err := conn.Receive(context.Background())
		if err != nil {
			t.Fatalf("recv type %d: %v", mt, err)
		}
		if msg.Type != mt {
			t.Fatalf("type mismatch: got %d, want %d", msg.Type, mt)
		}
	}
}

func TestWebSocketRegistry(t *testing.T) {
	factory, ok := transport.Get("ws")
	if !ok {
		t.Fatal("ws transport not registered")
	}
	_ = factory()

	available := transport.Available()
	found := false
	for _, name := range available {
		if name == "ws" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("ws not in available transports")
	}
}
