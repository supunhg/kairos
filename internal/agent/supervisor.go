package agent

import (
	"context"
	"fmt"
	"sync"
)

type Supervisor struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

func NewSupervisor() *Supervisor {
	return &Supervisor{
		agents: make(map[string]*Agent),
	}
}

func (s *Supervisor) Register(agent *Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.agents[agent.ID]; exists {
		return fmt.Errorf("agent %s already registered", agent.ID)
	}
	s.agents[agent.ID] = agent
	return nil
}

func (s *Supervisor) Unregister(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.agents[id]; !exists {
		return fmt.Errorf("agent %s not found", id)
	}
	delete(s.agents, id)
	return nil
}

func (s *Supervisor) Spawn(ctx context.Context, agent *Agent) error {
	if err := s.Register(agent); err != nil {
		return err
	}
	return agent.Start(ctx)
}

func (s *Supervisor) Stop(ctx context.Context, id string) error {
	s.mu.RLock()
	agent, exists := s.agents[id]
	s.mu.RUnlock()
	if !exists {
		return fmt.Errorf("agent %s not found", id)
	}
	return agent.Stop(ctx)
}

func (s *Supervisor) Get(id string) (*Agent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.agents[id]
	return a, ok
}

func (s *Supervisor) List() []*Agent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*Agent, 0, len(s.agents))
	for _, a := range s.agents {
		list = append(list, a)
	}
	return list
}

func (s *Supervisor) ListByStatus(status Status) []*Agent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var list []*Agent
	for _, a := range s.agents {
		if a.Status() == status {
			list = append(list, a)
		}
	}
	return list
}

func (s *Supervisor) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.agents)
}

func (s *Supervisor) StopAll(ctx context.Context) []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for id, a := range s.agents {
		if err := a.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
		delete(s.agents, id)
	}
	return errs
}
