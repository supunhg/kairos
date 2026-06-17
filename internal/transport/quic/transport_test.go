package quic

import (
	"context"
	"testing"
	"time"
)

func TestListenAndAccept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ln, err := Listen(ctx, ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	serverDone := make(chan error, 1)

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()

		msg, err := conn.Receive(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		if msg.Type != MsgPing {
			serverDone <- nil
			return
		}
		if string(msg.Payload) != "hello" {
			serverDone <- nil
			return
		}
		serverDone <- nil
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	err = conn.Send(ctx, Message{
		Type:    MsgPing,
		GroupID: "test",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("send error: %v", err)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for server")
	}
}

func TestSendReceiveMultiple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ln, err := Listen(ctx, ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	serverDone := make(chan error, 1)

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()

		for i := 0; i < 3; i++ {
			msg, err := conn.Receive(ctx)
			if err != nil {
				serverDone <- err
				return
			}
			if msg.Type != MessageType(i+1) && msg.Type != MsgPing && msg.Type != MsgEvent && msg.Type != MsgSyncReq {
				serverDone <- nil
				return
			}
		}
		serverDone <- nil
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	messages := []Message{
		{Type: MsgPing, GroupID: "g1", Payload: []byte("msg1")},
		{Type: MsgEvent, GroupID: "g2", Payload: []byte("msg2")},
		{Type: MsgSyncReq, GroupID: "g3", Payload: []byte("msg3")},
	}

	for _, msg := range messages {
		if err := conn.Send(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for server")
	}
}

func TestEmptyGroupID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ln, err := Listen(ctx, ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	serverDone := make(chan error, 1)

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()

		msg, err := conn.Receive(ctx)
		if err != nil {
			serverDone <- err
			return
		}
		if msg.GroupID != "" {
			serverDone <- nil
			return
		}
		if string(msg.Payload) != "data" {
			serverDone <- nil
			return
		}
		serverDone <- nil
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	err = conn.Send(ctx, Message{
		Type:    MsgPing,
		GroupID: "",
		Payload: []byte("data"),
	})
	if err != nil {
		t.Fatalf("send error: %v", err)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out")
	}
}
