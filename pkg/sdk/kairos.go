package kairos

import (
	"context"
	"fmt"
	"sync"

	syncengine "github.com/supunhg/kairos/internal/sync"
	"github.com/supunhg/kairos/internal/identity"
	"github.com/supunhg/kairos/internal/crypto"
	"github.com/supunhg/kairos/internal/transport/quic"
	"github.com/supunhg/kairos/api/v1"
	"google.golang.org/protobuf/proto"
)

type identitySigner struct {
	*identity.Identity
}

func (s *identitySigner) Sign(ev *v1.Event) error {
	return crypto.SignEvent(s.Identity, ev)
}

type identityVerifier struct{}

func (identityVerifier) Verify(ev *v1.Event) error {
	return crypto.VerifyEvent(ev)
}

type Client struct {
	nodeID       string
	engine       *syncengine.Engine
	identity     *identity.Identity
	syncProto    *syncengine.SyncProtocol
	conns        map[string]*quic.Conn
	mu           sync.RWMutex
}

type Option func(*Client)

func WithIdentity(id *identity.Identity) Option {
	return func(c *Client) {
		c.identity = id
	}
}

type Session struct {
	client  *Client
	id      string
	groups  map[string]*Group
	mu      sync.RWMutex
}

type Group struct {
	id     string
	sess   *Session
	engine *syncengine.Engine
	subs   []Subscription
	mu     sync.RWMutex
}

type Subscription struct {
	Event func(event *Event)
	Close func()
}

type Event = v1.Event

func New(nodeID string, opts ...Option) *Client {
	c := &Client{
		nodeID: nodeID,
		conns:  make(map[string]*quic.Conn),
	}
	for _, opt := range opts {
		opt(c)
	}
	var engineOpts []syncengine.EngineOpt
	if c.identity != nil {
		engineOpts = append(engineOpts,
			syncengine.WithSigner(&identitySigner{c.identity}),
			syncengine.WithVerifier(identityVerifier{}),
		)
	}
	c.engine = syncengine.NewEngine(nodeID, engineOpts...)
	c.syncProto = syncengine.NewSyncProtocol(c.engine)
	return c
}

func (c *Client) SyncProtocol() *syncengine.SyncProtocol {
	return c.syncProto
}

func (c *Client) Join(ctx context.Context, sessionID string) (*Session, error) {
	return &Session{
		client: c,
		id:     sessionID,
		groups: make(map[string]*Group),
	}, nil
}

func (c *Client) Connect(ctx context.Context, addr string) error {
	conn, err := quic.Dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	c.mu.Lock()
	c.conns[addr] = conn
	c.mu.Unlock()

	req := c.syncProto.BuildSyncRequest(ctx)
	data, err := syncengine.MarshalSyncRequest(req)
	if err != nil {
		return fmt.Errorf("marshal sync req: %w", err)
	}
	if err := conn.Send(ctx, quic.Message{Type: quic.MsgSyncReq, Payload: data}); err != nil {
		return fmt.Errorf("send sync req: %w", err)
	}

	go c.receiveLoop(conn)
	return nil
}

func (c *Client) receiveLoop(conn *quic.Conn) {
	ctx := context.Background()
	for {
		msg, err := conn.Receive(ctx)
		if err != nil {
			return
		}
		switch msg.Type {
		case quic.MsgEvent:
			var ev Event
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				continue
			}
			c.engine.Apply(ctx, []*Event{&ev})

		case quic.MsgSyncReq:
			req, err := syncengine.UnmarshalSyncRequest(msg.Payload)
			if err != nil {
				continue
			}
			resp, err := c.syncProto.HandleSyncRequest(ctx, req)
			if err != nil {
				continue
			}
			data, err := syncengine.MarshalSyncResponse(resp)
			if err != nil {
				continue
			}
			conn.Send(ctx, quic.Message{Type: quic.MsgSyncResp, Payload: data})

		case quic.MsgSyncResp:
			resp, err := syncengine.UnmarshalSyncResponse(msg.Payload)
			if err != nil {
				continue
			}
			c.syncProto.HandleSyncResponse(ctx, resp)
		}
	}
}

func (c *Client) SendEvent(ctx context.Context, addr string, ev *Event) error {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("not connected to %s", addr)
	}

	payload, err := proto.Marshal(ev)
	if err != nil {
		return err
	}

	return conn.Send(ctx, quic.Message{
		Type:    quic.MsgEvent,
		GroupID: ev.GroupId,
		Payload: payload,
	})
}

func (c *Client) Engine() *syncengine.Engine {
	return c.engine
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		conn.Close()
		delete(c.conns, addr)
	}
	return nil
}

func (s *Session) Document(ctx context.Context, docID string) (*Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if g, ok := s.groups[docID]; ok {
		return g, nil
	}

	groupID := s.id + "/" + docID
	g := &Group{
		id:     groupID,
		sess:   s,
		engine: s.client.engine,
	}
	s.groups[docID] = g
	return g, nil
}

func (s *Session) ID() string {
	return s.id
}

func (g *Group) Text(ctx context.Context) string {
	return g.engine.TextContent(g.id)
}

func (g *Group) Insert(ctx context.Context, pos int, text string) (*Event, error) {
	ev, err := g.engine.TextInsert(ctx, g.id, pos, text)
	if err != nil {
		return nil, err
	}

	g.sess.client.mu.RLock()
	for addr, conn := range g.sess.client.conns {
		payload, _ := proto.Marshal(ev)
		conn.Send(ctx, quic.Message{
			Type:    quic.MsgEvent,
			GroupID: g.id,
			Payload: payload,
		})
		_ = addr
	}
	g.sess.client.mu.RUnlock()

	return ev, nil
}

func (g *Group) Delete(ctx context.Context, pos, length int) (*Event, error) {
	ev, err := g.engine.TextDelete(ctx, g.id, pos, length)
	if err != nil {
		return nil, err
	}

	g.sess.client.mu.RLock()
	for _, conn := range g.sess.client.conns {
		payload, _ := proto.Marshal(ev)
		conn.Send(ctx, quic.Message{
			Type:    quic.MsgEvent,
			GroupID: g.id,
			Payload: payload,
		})
	}
	g.sess.client.mu.RUnlock()

	return ev, nil
}

func (g *Group) Subscribe(ctx context.Context, fn func(*Event)) func() {
	return g.engine.Subscribe(g.id, fn)
}

func (g *Group) ID() string {
	return g.id
}
