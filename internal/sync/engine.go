package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kairos-io/kairos-go/api/v1"
	"github.com/kairos-io/kairos-go/internal/crdt"
	"google.golang.org/protobuf/proto"
)

type GroupType int

const (
	TypeDocument GroupType = iota
	TypeMap
	TypeCounter
	TypeRegister
)

type Group struct {
	ID       string
	Type     GroupType
	Doc      *crdt.RGA
	Map      *crdt.LWWMap
	Counter  *crdt.PNCounter
	Register *crdt.LWWRegister
	mu       sync.RWMutex
	version  map[string]int64
	subs     []Subscriber
}

type Subscriber func(event *v1.Event)

type Engine struct {
	mu     sync.RWMutex
	groups map[string]*Group
	nodeID string
}

func NewEngine(nodeID string) *Engine {
	return &Engine{
		groups: make(map[string]*Group),
		nodeID: nodeID,
	}
}

func (e *Engine) GetOrCreateGroup(id string, gt GroupType) *Group {
	e.mu.Lock()
	defer e.mu.Unlock()
	if g, ok := e.groups[id]; ok {
		return g
	}
	g := &Group{
		ID:      id,
		Type:    gt,
		version: make(map[string]int64),
	}
	switch gt {
	case TypeDocument:
		g.Doc = crdt.NewRGA()
	case TypeMap:
		g.Map = crdt.NewLWWMap()
	case TypeCounter:
		g.Counter = crdt.NewPNCounter()
	case TypeRegister:
		g.Register = crdt.NewLWWRegister()
	}
	e.groups[id] = g
	return g
}

func (e *Engine) Apply(ctx context.Context, events []*v1.Event) error {
	for _, ev := range events {
		if err := e.applyEvent(ev); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) applyEvent(ev *v1.Event) error {
	group := e.GetOrCreateGroup(ev.GroupId, TypeDocument)
	group.mu.Lock()
	defer group.mu.Unlock()

	switch ev.PayloadType {
	case "kairos.v1.TextInsert":
		var op v1.TextInsert
		if err := proto.Unmarshal(ev.Payload, &op); err != nil {
			return fmt.Errorf("unmarshal TextInsert: %w", err)
		}
		group.Doc.Insert(int(op.Position), op.Text, ev.Originator)

	case "kairos.v1.TextDelete":
		var op v1.TextDelete
		if err := proto.Unmarshal(ev.Payload, &op); err != nil {
			return fmt.Errorf("unmarshal TextDelete: %w", err)
		}
		group.Doc.Delete(int(op.Position), int(op.Length))

	case "kairos.v1.MapSet":
		var op v1.MapSet
		if err := proto.Unmarshal(ev.Payload, &op); err != nil {
			return fmt.Errorf("unmarshal MapSet: %w", err)
		}
		group.Map.Set(op.Key, string(op.Value), ev.Originator)

	case "kairos.v1.MapDelete":
		var op v1.MapDelete
		if err := proto.Unmarshal(ev.Payload, &op); err != nil {
			return fmt.Errorf("unmarshal MapDelete: %w", err)
		}
		group.Map.Delete(op.Key, ev.Originator)

	default:
		return fmt.Errorf("unknown payload type: %s", ev.PayloadType)
	}

	group.version[ev.Originator] = ev.HlcTimestamp
	for _, sub := range group.subs {
		sub(ev)
	}
	return nil
}

func (e *Engine) Subscribe(groupID string, fn Subscriber) func() {
	e.mu.RLock()
	group, ok := e.groups[groupID]
	e.mu.RUnlock()
	if !ok {
		group = e.GetOrCreateGroup(groupID, TypeDocument)
	}

	group.mu.Lock()
	group.subs = append(group.subs, fn)
	group.mu.Unlock()

	return func() {
		group.mu.Lock()
		defer group.mu.Unlock()
		for i, sub := range group.subs {
			if fmt.Sprintf("%p", sub) == fmt.Sprintf("%p", fn) {
				group.subs = append(group.subs[:i], group.subs[i+1:]...)
				break
			}
		}
	}
}

func (e *Engine) TextInsert(ctx context.Context, groupID string, pos int, text string) (*v1.Event, error) {
	group := e.GetOrCreateGroup(groupID, TypeDocument)
	group.mu.Lock()
	defer group.mu.Unlock()

	op := &v1.TextInsert{
		DocId:    groupID,
		Position: int64(pos),
		Text:     text,
	}
	payload, err := proto.Marshal(op)
	if err != nil {
		return nil, err
	}

	ev := &v1.Event{
		Id:           fmt.Sprintf("%s-%d", e.nodeID, time.Now().UnixNano()),
		PayloadType:  "kairos.v1.TextInsert",
		Payload:      payload,
		HlcTimestamp: time.Now().UnixNano(),
		Originator:   e.nodeID,
		GroupId:      groupID,
	}

	group.Doc.Insert(pos, text, e.nodeID)
	group.version[e.nodeID] = ev.HlcTimestamp
	for _, sub := range group.subs {
		sub(ev)
	}
	return ev, nil
}

func (e *Engine) TextDelete(ctx context.Context, groupID string, pos, length int) (*v1.Event, error) {
	group := e.GetOrCreateGroup(groupID, TypeDocument)
	group.mu.Lock()
	defer group.mu.Unlock()

	op := &v1.TextDelete{
		DocId:    groupID,
		Position: int64(pos),
		Length:   int64(length),
	}
	payload, err := proto.Marshal(op)
	if err != nil {
		return nil, err
	}

	ev := &v1.Event{
		Id:           fmt.Sprintf("%s-%d", e.nodeID, time.Now().UnixNano()),
		PayloadType:  "kairos.v1.TextDelete",
		Payload:      payload,
		HlcTimestamp: time.Now().UnixNano(),
		Originator:   e.nodeID,
		GroupId:      groupID,
	}

	group.Doc.Delete(pos, length)
	group.version[e.nodeID] = ev.HlcTimestamp
	for _, sub := range group.subs {
		sub(ev)
	}
	return ev, nil
}

func (e *Engine) MapSet(ctx context.Context, groupID, key, value string) (*v1.Event, error) {
	group := e.GetOrCreateGroup(groupID, TypeMap)
	group.mu.Lock()
	defer group.mu.Unlock()

	op := &v1.MapSet{
		MapId: groupID,
		Key:   key,
		Value: []byte(value),
	}
	payload, err := proto.Marshal(op)
	if err != nil {
		return nil, err
	}

	ev := &v1.Event{
		Id:           fmt.Sprintf("%s-%d", e.nodeID, time.Now().UnixNano()),
		PayloadType:  "kairos.v1.MapSet",
		Payload:      payload,
		HlcTimestamp: time.Now().UnixNano(),
		Originator:   e.nodeID,
		GroupId:      groupID,
	}

	group.Map.Set(key, value, e.nodeID)
	group.version[e.nodeID] = ev.HlcTimestamp
	for _, sub := range group.subs {
		sub(ev)
	}
	return ev, nil
}

func (e *Engine) GetVersionVector(groupID string) map[string]int64 {
	e.mu.RLock()
	group, ok := e.groups[groupID]
	e.mu.RUnlock()
	if !ok {
		return nil
	}
	group.mu.RLock()
	defer group.mu.RUnlock()
	vv := make(map[string]int64)
	for k, v := range group.version {
		vv[k] = v
	}
	return vv
}

func (e *Engine) TextContent(groupID string) string {
	e.mu.RLock()
	group, ok := e.groups[groupID]
	e.mu.RUnlock()
	if !ok {
		return ""
	}
	group.mu.RLock()
	defer group.mu.RUnlock()
	return group.Doc.Text()
}

func (e *Engine) MapGet(groupID, key string) any {
	e.mu.RLock()
	group, ok := e.groups[groupID]
	e.mu.RUnlock()
	if !ok {
		return nil
	}
	return group.Map.Get(key)
}

func (e *Engine) GroupCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.groups)
}

func (e *Engine) GroupIDs() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var ids []string
	for id := range e.groups {
		ids = append(ids, id)
	}
	return ids
}

var (
	ErrGroupNotFound = errors.New("group not found")
	ErrInvalidOp     = errors.New("invalid operation")
)
