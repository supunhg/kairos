package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	syncengine "github.com/supunhg/kairos/internal/sync"
	"github.com/supunhg/kairos/api/v1"
	"google.golang.org/protobuf/proto"
)

type BlackboardMessage struct {
	ID        string
	Sender    string
	Content   string
	Timestamp int64
}

type Blackboard struct {
	engine  *syncengine.Engine
	groupID string
	mu      sync.RWMutex
	messages []BlackboardMessage
}

func NewBlackboard(engine *syncengine.Engine, groupID string) *Blackboard {
	engine.GetOrCreateGroup(groupID, syncengine.TypeMap)
	return &Blackboard{
		engine:  engine,
		groupID: groupID,
	}
}

func (b *Blackboard) Post(ctx context.Context, sender, content string) (*BlackboardMessage, error) {
	id := fmt.Sprintf("%s-%d", sender, time.Now().UnixNano())
	msg := BlackboardMessage{
		ID:        id,
		Sender:    sender,
		Content:   content,
		Timestamp: time.Now().UnixNano(),
	}
	_, err := b.engine.MapSet(ctx, b.groupID, id, content)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.messages = append(b.messages, msg)
	b.mu.Unlock()
	return &msg, nil
}

func (b *Blackboard) Messages() []BlackboardMessage {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cp := make([]BlackboardMessage, len(b.messages))
	copy(cp, b.messages)
	return cp
}

func (b *Blackboard) Subscribe(_ context.Context, fn func(msg BlackboardMessage)) func() {
	return b.engine.Subscribe(b.groupID, func(ev *v1.Event) {
		if ev.PayloadType != "kairos.v1.MapSet" {
			return
		}
		var op v1.MapSet
		if err := proto.Unmarshal(ev.Payload, &op); err != nil {
			return
		}
		msg := BlackboardMessage{
			ID:        ev.Id,
			Sender:    ev.Originator,
			Content:   string(op.Value),
			Timestamp: ev.HlcTimestamp,
		}
		b.mu.Lock()
		b.messages = append(b.messages, msg)
		b.mu.Unlock()
		fn(msg)
	})
}

func (b *Blackboard) ID() string {
	return b.groupID
}
