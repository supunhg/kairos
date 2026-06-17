package eventlog

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/supunhg/kairos/api/v1"
)

type Store interface {
	Append(ctx context.Context, events []*v1.Event) error
	Get(ctx context.Context, id string) (*v1.Event, error)
	Iter(ctx context.Context, opts IterOptions) (Iterator, error)
	Close() error
	Sync() error
	Stats() Stats
}

type IterOptions struct {
	AfterID      string
	BeforeID     string
	GroupID      string
	SessionID    string
	Limit        int
	Reverse      bool
	SinceHLC     int64
	UntilHLC     int64
}

type Iterator interface {
	Next() bool
	Event() *v1.Event
	Err() error
	Close() error
}

type Stats struct {
	EventCount    uint64
	BytesWritten  uint64
	BytesRead     uint64
	OldestEventID string
	NewestEventID string
}

type AppendOnlyStore struct {
	path       string
	file       *os.File
	mu         sync.RWMutex
	index      map[string]int64
	indexMu    sync.RWMutex
	stats      Stats
	closed     bool
}

func NewAppendOnlyStore(path string, opts ...StoreOption) (*AppendOnlyStore, error) {
	s := &AppendOnlyStore{
		path:  path,
		index: make(map[string]int64),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, s.open()
}

type StoreOption func(*AppendOnlyStore)

func (s *AppendOnlyStore) open() error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	s.file = f

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := s.rebuildIndex(); err != nil {
			return err
		}
	}

	return nil
}

func (s *AppendOnlyStore) rebuildIndex() error {
	dec := NewDecoder(s.file)
	for {
		ev, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		s.index[ev.Id] = dec.LastOffset()
		atomic.AddUint64(&s.stats.EventCount, 1)
		s.stats.NewestEventID = ev.Id
		if s.stats.OldestEventID == "" {
			s.stats.OldestEventID = ev.Id
		}
	}
	return nil
}

func (s *AppendOnlyStore) Append(ctx context.Context, events []*v1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	for _, ev := range events {
		if ev.Id == "" {
			return ErrMissingPayloadType
		}
		data, err := MarshalEvent(ev)
		if err != nil {
			return err
		}

		offset, err := s.file.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}

		if _, err := s.file.Write(data); err != nil {
			return err
		}

		s.index[ev.Id] = offset
		atomic.AddUint64(&s.stats.EventCount, 1)
		atomic.AddUint64(&s.stats.BytesWritten, uint64(len(data)))
		s.stats.NewestEventID = ev.Id
		if s.stats.OldestEventID == "" {
			s.stats.OldestEventID = ev.Id
		}
	}

	return s.file.Sync()
}

func (s *AppendOnlyStore) Get(ctx context.Context, id string) (*v1.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.index[id]
	if !ok {
		return nil, ErrEventNotFound
	}

	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	dec := NewDecoder(s.file)
	return dec.Decode()
}

func (s *AppendOnlyStore) Iter(ctx context.Context, opts IterOptions) (Iterator, error) {
	if s.closed {
		return nil, ErrStoreClosed
	}
	return newIterator(s.file, s.index, opts), nil
}

func (s *AppendOnlyStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *AppendOnlyStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}

func (s *AppendOnlyStore) Stats() Stats {
	return s.stats
}
