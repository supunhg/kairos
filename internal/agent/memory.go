package agent

import (
	"context"
	"sync"

	v1 "github.com/supunhg/kairos/api/v1"
	syncengine "github.com/supunhg/kairos/internal/sync"
	"google.golang.org/protobuf/proto"
)

type AgentMemory struct {
	engine  *syncengine.Engine
	groupID string
	mu      sync.RWMutex
	local   map[string]string
}

func NewAgentMemory(engine *syncengine.Engine, groupID string) *AgentMemory {
	engine.GetOrCreateGroup(groupID, syncengine.TypeMap)
	return &AgentMemory{
		engine:  engine,
		groupID: groupID,
		local:   make(map[string]string),
	}
}

func (m *AgentMemory) Set(ctx context.Context, key, value string) error {
	m.mu.Lock()
	m.local[key] = value
	m.mu.Unlock()
	_, err := m.engine.MapSet(ctx, m.groupID, key, value)
	return err
}

func (m *AgentMemory) Get(_ context.Context, key string) (string, bool) {
	m.mu.RLock()
	v, ok := m.local[key]
	m.mu.RUnlock()
	if ok {
		return v, true
	}
	val := m.engine.MapGet(m.groupID, key)
	if val == nil {
		return "", false
	}
	s, ok := val.(string)
	if !ok {
		return "", false
	}
	m.mu.Lock()
	m.local[key] = s
	m.mu.Unlock()
	return s, true
}

func (m *AgentMemory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.local, key)
	m.mu.Unlock()
	return nil
}

func (m *AgentMemory) Subscribe(_ context.Context, fn func(key, value string)) func() {
	return m.engine.Subscribe(m.groupID, func(ev *v1.Event) {
		if ev.PayloadType != "kairos.v1.MapSet" {
			return
		}
		var ms v1.MapSet
		if err := proto.Unmarshal(ev.Payload, &ms); err != nil {
			return
		}
		value := string(ms.Value)
		m.mu.Lock()
		m.local[ms.Key] = value
		m.mu.Unlock()
		fn(ms.Key, value)
	})
}

func (m *AgentMemory) Snapshot() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make(map[string]string, len(m.local))
	for k, v := range m.local {
		snap[k] = v
	}
	return snap
}
