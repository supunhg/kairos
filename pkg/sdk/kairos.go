// Package kairos provides the high-level client SDK for KAIROS real-time collaboration.
package kairos

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	v1 "github.com/supunhg/kairos/api/v1"
	"github.com/supunhg/kairos/internal/crypto"
	"github.com/supunhg/kairos/internal/identity"
	syncengine "github.com/supunhg/kairos/internal/sync"
	"github.com/supunhg/kairos/internal/transport"
	"github.com/supunhg/kairos/internal/transport/quic"
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

type quicConnAdapter struct {
	conn *quic.Conn
}

func (a *quicConnAdapter) Send(ctx context.Context, msg transport.Message) error {
	return a.conn.Send(ctx, msg)
}

func (a *quicConnAdapter) Receive(ctx context.Context) (transport.Message, error) {
	return a.conn.Receive(ctx)
}

func (a *quicConnAdapter) Close() error {
	return a.conn.Close()
}

type Logger interface {
	Printf(format string, v ...any)
}

type peerConn struct {
	conn      transport.Connection
	addr      string
	createdAt time.Time
}

type Client struct {
	nodeID     string
	engine     *syncengine.Engine
	identity   *identity.Identity
	syncProto  *syncengine.SyncProtocol
	encryption *crypto.SessionEncryption
	conns      map[string]*peerConn
	mu         sync.RWMutex
	logger     Logger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	opts       clientOpts
}

type clientOpts struct {
	dialTimeout   time.Duration
	reconnectMax  time.Duration
	reconnectBase time.Duration
}

type Option func(*Client)

func WithIdentity(id *identity.Identity) Option {
	return func(c *Client) {
		c.identity = id
	}
}

func WithEncryption(enc *crypto.SessionEncryption) Option {
	return func(c *Client) {
		c.encryption = enc
	}
}

func WithLogger(l Logger) Option {
	return func(c *Client) {
		c.logger = l
	}
}

func WithDialTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.opts.dialTimeout = d
	}
}

func WithReconnectBackoff(base, max time.Duration) Option {
	return func(c *Client) {
		c.opts.reconnectBase = base
		c.opts.reconnectMax = max
	}
}

type Session struct {
	client *Client
	id     string
	groups map[string]*Group
	mu     sync.RWMutex
}

type Group struct {
	id     string
	sess   *Session
	engine *syncengine.Engine
}

type Subscription struct {
	Event func(event *Event)
	Close func()
}

type Event = v1.Event

func New(nodeID string, opts ...Option) *Client {
	c := &Client{
		nodeID: nodeID,
		conns:  make(map[string]*peerConn),
		opts: clientOpts{
			dialTimeout:   10 * time.Second,
			reconnectBase: 500 * time.Millisecond,
			reconnectMax:  30 * time.Second,
		},
		logger: log.Default(),
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

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	_ = ctx

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
	dialCtx, dialCancel := context.WithTimeout(ctx, c.opts.dialTimeout)
	defer dialCancel()

	raw, err := quic.Dial(dialCtx, addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	conn := &quicConnAdapter{conn: raw}

	c.mu.Lock()
	c.conns[addr] = &peerConn{conn: conn, addr: addr, createdAt: time.Now()}
	c.mu.Unlock()

	if c.encryption != nil {
		if err := conn.Send(ctx, transport.Message{
			Type:    transport.MsgKeyExchange,
			Payload: c.encryption.PublicKey(),
		}); err != nil {
			_ = conn.Close()
			return fmt.Errorf("send key exchange: %w", err)
		}
	}

	req := c.syncProto.BuildSyncRequest(ctx)
	data, err := syncengine.MarshalSyncRequest(req)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("marshal sync req: %w", err)
	}
	if err := conn.Send(ctx, transport.Message{Type: transport.MsgSyncReq, Payload: data}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("send sync req: %w", err)
	}

	c.wg.Add(1)
	//nolint:gosec // G118: goroutine needs background context for reconnect loop
	go c.receiveLoop(conn, addr)
	return nil
}

func (c *Client) receiveLoop(conn transport.Connection, peerAddr string) {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ctx.Done()
	}()

	backoff := c.opts.reconnectBase
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := conn.Receive(ctx)
		if err != nil {
			c.log("disconnect from %s: %v", peerAddr, err)
			if c.tryReconnect(peerAddr) {
				return
			}
			backoff = min(backoff*2, c.opts.reconnectMax)
			time.Sleep(backoff)
			continue
		}
		backoff = c.opts.reconnectBase

		switch msg.Type {
		case transport.MsgKeyExchange:
			if c.encryption != nil {
				_ = c.encryption.EstablishSession(peerAddr, msg.Payload)
				if err := conn.Send(ctx, transport.Message{
					Type:    transport.MsgKeyExchange,
					Payload: c.encryption.PublicKey(),
				}); err != nil {
					c.log("send key exchange reply to %s: %v", peerAddr, err)
				}
			}

		case transport.MsgEvent:
			var ev Event
			if err := proto.Unmarshal(msg.Payload, &ev); err != nil {
				c.log("unmarshal event from %s: %v", peerAddr, err)
				continue
			}
			if err := c.engine.Apply(ctx, []*Event{&ev}); err != nil {
				c.log("apply event from %s: %v", peerAddr, err)
			}

		case transport.MsgSyncReq:
			req, err := syncengine.UnmarshalSyncRequest(msg.Payload)
			if err != nil {
				c.log("unmarshal sync req from %s: %v", peerAddr, err)
				continue
			}
			resp, err := c.syncProto.HandleSyncRequest(ctx, req)
			if err != nil {
				c.log("handle sync req from %s: %v", peerAddr, err)
				continue
			}
			data, err := syncengine.MarshalSyncResponse(resp)
			if err != nil {
				c.log("marshal sync resp for %s: %v", peerAddr, err)
				continue
			}
			if err := conn.Send(ctx, transport.Message{Type: transport.MsgSyncResp, Payload: data}); err != nil {
				c.log("send sync resp to %s: %v", peerAddr, err)
			}

		case transport.MsgSyncResp:
			resp, err := syncengine.UnmarshalSyncResponse(msg.Payload)
			if err != nil {
				c.log("unmarshal sync resp from %s: %v", peerAddr, err)
				continue
			}
			if err := c.syncProto.HandleSyncResponse(ctx, resp); err != nil {
				c.log("handle sync resp from %s: %v", peerAddr, err)
			}
		}
	}
}

func (c *Client) tryReconnect(peerAddr string) bool {
	c.mu.Lock()
	pc, exists := c.conns[peerAddr]
	c.mu.Unlock()

	if !exists {
		return true
	}

	c.log("reconnecting to %s...", peerAddr)
	_ = pc.conn.Close()

	raw, err := quic.Dial(context.Background(), peerAddr)
	if err != nil {
		c.log("reconnect to %s failed: %v", peerAddr, err)
		return false
	}

	newConn := &quicConnAdapter{conn: raw}
	c.mu.Lock()
	c.conns[peerAddr] = &peerConn{conn: newConn, addr: peerAddr, createdAt: time.Now()}
	c.mu.Unlock()

	if c.encryption != nil {
		if err := newConn.Send(context.Background(), transport.Message{
			Type:    transport.MsgKeyExchange,
			Payload: c.encryption.PublicKey(),
		}); err != nil {
			c.log("reconnect key exchange to %s: %v", peerAddr, err)
		}
	}

	req := c.syncProto.BuildSyncRequest(context.Background())
	data, err := syncengine.MarshalSyncRequest(req)
	if err != nil {
		c.log("reconnect marshal sync req for %s: %v", peerAddr, err)
		return false
	}
	if err := newConn.Send(context.Background(), transport.Message{Type: transport.MsgSyncReq, Payload: data}); err != nil {
		c.log("reconnect send sync req to %s: %v", peerAddr, err)
		return false
	}

	c.log("reconnected to %s", peerAddr)
	return true
}

func (c *Client) log(format string, v ...any) {
	if c.logger != nil {
		c.logger.Printf("[kairos] "+format, v...)
	}
}

func (c *Client) SendEvent(ctx context.Context, addr string, ev *Event) error {
	c.mu.RLock()
	pc, ok := c.conns[addr]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("not connected to %s", addr)
	}

	payload, err := proto.Marshal(ev)
	if err != nil {
		return err
	}

	return pc.conn.Send(ctx, transport.Message{
		Type:    transport.MsgEvent,
		GroupID: ev.GroupId,
		Payload: payload,
	})
}

func (c *Client) Engine() *syncengine.Engine {
	return c.engine
}

func (c *Client) Close() error {
	c.cancel()

	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, pc := range c.conns {
		_ = pc.conn.Close()
		delete(c.conns, addr)
	}
	c.wg.Wait()
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
	var lastErr error
	for _, pc := range g.sess.client.conns {
		payload, err := proto.Marshal(ev)
		if err != nil {
			lastErr = err
			continue
		}
		if err := pc.conn.Send(ctx, transport.Message{
			Type:    transport.MsgEvent,
			GroupID: g.id,
			Payload: payload,
		}); err != nil {
			lastErr = err
		}
	}
	g.sess.client.mu.RUnlock()

	return ev, lastErr
}

func (g *Group) Delete(ctx context.Context, pos, length int) (*Event, error) {
	ev, err := g.engine.TextDelete(ctx, g.id, pos, length)
	if err != nil {
		return nil, err
	}

	g.sess.client.mu.RLock()
	var lastErr error
	for _, pc := range g.sess.client.conns {
		payload, err := proto.Marshal(ev)
		if err != nil {
			lastErr = err
			continue
		}
		if err := pc.conn.Send(ctx, transport.Message{
			Type:    transport.MsgEvent,
			GroupID: g.id,
			Payload: payload,
		}); err != nil {
			lastErr = err
		}
	}
	g.sess.client.mu.RUnlock()

	return ev, lastErr
}

func (g *Group) Subscribe(ctx context.Context, fn func(*Event)) func() {
	return g.engine.Subscribe(g.id, fn)
}

func (g *Group) ID() string {
	return g.id
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
