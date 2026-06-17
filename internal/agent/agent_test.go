package agent

import (
	"context"
	"testing"
	"time"

	"github.com/supunhg/kairos/internal/sync"
)

func TestAgentMemorySetGet(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-mem-1")
	ctx := context.Background()

	if err := mem.Set(ctx, "name", "test-agent"); err != nil {
		t.Fatal(err)
	}

	val, ok := mem.Get(ctx, "name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if val != "test-agent" {
		t.Fatalf("expected 'test-agent', got '%s'", val)
	}
}

func TestAgentMemoryGetMissing(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-mem-2")
	ctx := context.Background()

	_, ok := mem.Get(ctx, "nonexistent")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestAgentMemorySnapshot(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-mem-3")
	ctx := context.Background()

	_ = mem.Set(ctx, "a", "1")
	_ = mem.Set(ctx, "b", "2")
	_ = mem.Set(ctx, "c", "3")

	snap := mem.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	if snap["a"] != "1" {
		t.Fatalf("expected '1', got '%s'", snap["a"])
	}
}

func TestBlackboardPostAndRead(t *testing.T) {
	engine := sync.NewEngine("test-node")
	board := NewBlackboard(engine, "board-1")
	ctx := context.Background()

	msg, err := board.Post(ctx, "agent1", "Hello from agent1")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sender != "agent1" {
		t.Fatalf("expected sender 'agent1', got '%s'", msg.Sender)
	}
	if msg.Content != "Hello from agent1" {
		t.Fatalf("expected 'Hello from agent1', got '%s'", msg.Content)
	}

	msgs := board.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
}

func TestBlackboardSubscribe(t *testing.T) {
	engine := sync.NewEngine("test-node")
	board := NewBlackboard(engine, "board-2")
	ctx := context.Background()

	received := make(chan BlackboardMessage, 1)
	unsub := board.Subscribe(ctx, func(msg BlackboardMessage) {
		received <- msg
	})
	defer unsub()

	_, _ = board.Post(ctx, "agent1", "ping")

	select {
	case msg := <-received:
		if msg.Content != "ping" {
			t.Fatalf("expected 'ping', got '%s'", msg.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blackboard message")
	}
}

func TestAgentLifecycle(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-life-mem")
	board := NewBlackboard(engine, "agent-life-board")

	agent := NewAgent("test-agent", mem, board)
	if agent.Status() != StatusCreated {
		t.Fatalf("expected StatusCreated, got %v", agent.Status())
	}

	ctx := context.Background()
	if err := agent.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.Status() != StatusRunning {
		t.Fatalf("expected StatusRunning, got %v", agent.Status())
	}

	if err := agent.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.Status() != StatusPaused {
		t.Fatalf("expected StatusPaused, got %v", agent.Status())
	}

	if err := agent.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.Status() != StatusRunning {
		t.Fatalf("expected StatusRunning after resume, got %v", agent.Status())
	}

	if err := agent.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.Status() != StatusStopped {
		t.Fatalf("expected StatusStopped, got %v", agent.Status())
	}
}

func TestAgentEventHandler(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-evt-mem")
	board := NewBlackboard(engine, "agent-evt-board")

	received := make(chan interface{}, 1)
	agent := NewAgent("evt-agent", mem, board,
		WithEventHandler(func(ctx context.Context, event interface{}) {
			received <- event
		}),
	)

	ctx := context.Background()
	_ = agent.Start(ctx)

	select {
	case evt := <-received:
		if evt != "started" {
			t.Fatalf("expected 'started', got '%v'", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for start event")
	}
}

func TestAgentSendToBlackboard(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "agent-send-mem")
	board := NewBlackboard(engine, "agent-send-board")

	agent := NewAgent("sender", mem, board)
	ctx := context.Background()

	msg, err := agent.Send(ctx, "broadcast message")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sender != "sender" {
		t.Fatalf("expected sender 'sender', got '%s'", msg.Sender)
	}
}

func TestSupervisorRegister(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "sup-mem")
	board := NewBlackboard(engine, "sup-board")
	sup := NewSupervisor()

	agent := NewAgent("agent-1", mem, board)
	if err := sup.Register(agent); err != nil {
		t.Fatal(err)
	}

	if sup.Count() != 1 {
		t.Fatalf("expected 1 agent, got %d", sup.Count())
	}
}

func TestSupervisorSpawnAndStop(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "sup-spawn-mem")
	board := NewBlackboard(engine, "sup-spawn-board")
	sup := NewSupervisor()
	ctx := context.Background()

	agent := NewAgent("spawn-test", mem, board)
	if err := sup.Spawn(ctx, agent); err != nil {
		t.Fatal(err)
	}

	if agent.Status() != StatusRunning {
		t.Fatalf("expected StatusRunning, got %v", agent.Status())
	}

	if err := sup.Stop(ctx, "spawn-test"); err != nil {
		t.Fatal(err)
	}

	if agent.Status() != StatusStopped {
		t.Fatalf("expected StatusStopped, got %v", agent.Status())
	}
}

func TestSupervisorDuplicateRegistration(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "dup-mem")
	board := NewBlackboard(engine, "dup-board")
	sup := NewSupervisor()

	agent1 := NewAgent("dup-agent", mem, board)
	agent2 := NewAgent("dup-agent", mem, board)

	if err := sup.Register(agent1); err != nil {
		t.Fatal(err)
	}
	if err := sup.Register(agent2); err == nil {
		t.Fatal("expected error on duplicate registration")
	}
}

func TestSupervisorListByStatus(t *testing.T) {
	engine := sync.NewEngine("test-node")
	sup := NewSupervisor()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		id := string(rune('A' + i))
		mem := NewAgentMemory(engine, "list-mem-"+id)
		board := NewBlackboard(engine, "list-board-"+id)
		agent := NewAgent(id, mem, board)
		_ = sup.Register(agent)
		_ = agent.Start(ctx)
	}

	running := sup.ListByStatus(StatusRunning)
	if len(running) != 3 {
		t.Fatalf("expected 3 running agents, got %d", len(running))
	}
}

func TestAgentMemoryAcrossGroups(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem1 := NewAgentMemory(engine, "shared-mem")
	mem2 := NewAgentMemory(engine, "shared-mem")
	ctx := context.Background()

	_ = mem1.Set(ctx, "key", "value")

	val, ok := mem2.Get(ctx, "key")
	if !ok {
		t.Fatal("expected key to be visible from second memory instance")
	}
	if val != "value" {
		t.Fatalf("expected 'value', got '%s'", val)
	}
}

func TestAgentMetadata(t *testing.T) {
	engine := sync.NewEngine("test-node")
	mem := NewAgentMemory(engine, "meta-mem")
	board := NewBlackboard(engine, "meta-board")

	agent := NewAgent("meta-test", mem, board,
		WithMetadata("role", "worker"),
	)

	val, ok := agent.GetMetadata("role")
	if !ok {
		t.Fatal("expected metadata 'role' to exist")
	}
	if val != "worker" {
		t.Fatalf("expected 'worker', got '%s'", val)
	}

	agent.SetMetadata("version", "1.0")
	val, ok = agent.GetMetadata("version")
	if !ok {
		t.Fatal("expected metadata 'version' to exist")
	}
	if val != "1.0" {
		t.Fatalf("expected '1.0', got '%s'", val)
	}
}

func TestSupervisorStopAll(t *testing.T) {
	engine := sync.NewEngine("test-node")
	sup := NewSupervisor()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		id := string(rune('X' + i))
		mem := NewAgentMemory(engine, "stopall-mem-"+id)
		board := NewBlackboard(engine, "stopall-board-"+id)
		agent := NewAgent(id, mem, board)
		_ = sup.Spawn(ctx, agent)
	}

	errs := sup.StopAll(ctx)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d", len(errs))
	}
	if sup.Count() != 0 {
		t.Fatalf("expected 0 agents after StopAll, got %d", sup.Count())
	}
}
