package transport

import (
	"context"
	"net"
	"testing"
)

type mockTransport struct{}

func (m *mockTransport) Dial(ctx context.Context, addr string) (Connection, error) {
	return nil, nil
}

func (m *mockTransport) Listen(ctx context.Context, addr string) (Listener, error) {
	return nil, nil
}

func TestRegisterAndGet(t *testing.T) {
	registry = make(map[string]Factory)

	Register("mock", func() Transport { return &mockTransport{} })

	f, ok := Get("mock")
	if !ok {
		t.Fatal("expected 'mock' to be registered")
	}
	_ = f()

	_, ok = Get("nonexistent")
	if ok {
		t.Fatal("expected 'nonexistent' to not be registered")
	}
}

func TestAvailable(t *testing.T) {
	registry = make(map[string]Factory)

	Register("alpha", func() Transport { return &mockTransport{} })
	Register("beta", func() Transport { return &mockTransport{} })

	available := Available()
	if len(available) != 2 {
		t.Fatalf("expected 2 available transports, got %d", len(available))
	}

	found := map[string]bool{}
	for _, name := range available {
		found[name] = true
	}
	if !found["alpha"] || !found["beta"] {
		t.Fatalf("expected alpha and beta in available, got %v", available)
	}
}

func TestRegisterOverwrite(t *testing.T) {
	registry = make(map[string]Factory)

	Register("test", func() Transport { return &mockTransport{} })
	Register("test", func() Transport { return &mockTransport{} })

	available := Available()
	if len(available) != 1 {
		t.Fatalf("expected 1 transport after overwrite, got %d", len(available))
	}
}

type mockConn struct{}

func (m *mockConn) Send(ctx context.Context, msg Message) error   { return nil }
func (m *mockConn) Receive(ctx context.Context) (Message, error)  { return Message{}, nil }
func (m *mockConn) Close() error                                  { return nil }

type mockListener struct {
	addr net.Addr
}

func (m *mockListener) Accept(ctx context.Context) (Connection, error) {
	return &mockConn{}, nil
}
func (m *mockListener) Close() error                                  { return nil }
func (m *mockListener) Addr() net.Addr                                { return m.addr }
