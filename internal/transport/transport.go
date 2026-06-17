// Package transport defines the pluggable transport layer for peer communication.
package transport

import (
	"context"
	"net"
)

type MessageType byte

const (
	MsgEvent       MessageType = 0x01
	MsgSyncReq     MessageType = 0x02
	MsgSyncResp    MessageType = 0x03
	MsgPing        MessageType = 0x04
	MsgPong        MessageType = 0x05
	MsgJoin        MessageType = 0x06
	MsgLeave       MessageType = 0x07
	MsgKeyExchange MessageType = 0x08
)

type Message struct {
	Type    MessageType
	GroupID string
	Payload []byte
}

type Connection interface {
	Send(ctx context.Context, msg Message) error
	Receive(ctx context.Context) (Message, error)
	Close() error
}

type Listener interface {
	Accept(ctx context.Context) (Connection, error)
	Close() error
	Addr() net.Addr
}

type Transport interface {
	Dial(ctx context.Context, addr string) (Connection, error)
	Listen(ctx context.Context, addr string) (Listener, error)
}

type Factory func() Transport

var registry = make(map[string]Factory)

func Register(name string, f Factory) {
	registry[name] = f
}

func Get(name string) (Factory, bool) {
	f, ok := registry[name]
	return f, ok
}

func Available() []string {
	var names []string
	for name := range registry {
		names = append(names, name)
	}
	return names
}
