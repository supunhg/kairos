package agent

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Status int

const (
	StatusCreated  Status = iota
	StatusRunning
	StatusStopped
	StatusPaused
	StatusFailed
)

func (s Status) String() string {
	switch s {
	case StatusCreated:
		return "created"
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	case StatusPaused:
		return "paused"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type EventHandler func(ctx context.Context, event interface{})

type Agent struct {
	ID         string
	Memory     *AgentMemory
	Blackboard *Blackboard
	status     Status
	handler    EventHandler
	mu         sync.RWMutex
	lastActive time.Time
	metadata   map[string]string
}

type AgentOpt func(*Agent)

func WithEventHandler(h EventHandler) AgentOpt {
	return func(a *Agent) {
		a.handler = h
	}
}

func WithMetadata(key, value string) AgentOpt {
	return func(a *Agent) {
		a.metadata[key] = value
	}
}

func NewAgent(id string, memory *AgentMemory, board *Blackboard, opts ...AgentOpt) *Agent {
	a := &Agent{
		ID:         id,
		Memory:     memory,
		Blackboard: board,
		status:     StatusCreated,
		lastActive: time.Now(),
		metadata:   make(map[string]string),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Agent) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status == StatusRunning {
		return fmt.Errorf("agent %s already running", a.ID)
	}
	a.status = StatusRunning
	a.lastActive = time.Now()
	if a.handler != nil {
		go func() {
			a.handler(ctx, "started")
		}()
	}
	return nil
}

func (a *Agent) Stop(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status != StatusRunning {
		return fmt.Errorf("agent %s is not running", a.ID)
	}
	a.status = StatusStopped
	if a.handler != nil {
		a.handler(ctx, "stopped")
	}
	return nil
}

func (a *Agent) Pause(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status != StatusRunning {
		return fmt.Errorf("agent %s is not running", a.ID)
	}
	a.status = StatusPaused
	return nil
}

func (a *Agent) Resume(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status != StatusPaused {
		return fmt.Errorf("agent %s is not paused", a.ID)
	}
	a.status = StatusRunning
	return nil
}

func (a *Agent) Status() Status {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.status
}

func (a *Agent) LastActive() time.Time {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastActive
}

func (a *Agent) SetMetadata(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.metadata[key] = value
}

func (a *Agent) GetMetadata(key string) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	v, ok := a.metadata[key]
	return v, ok
}

func (a *Agent) Send(ctx context.Context, content string) (*BlackboardMessage, error) {
	return a.Blackboard.Post(ctx, a.ID, content)
}
