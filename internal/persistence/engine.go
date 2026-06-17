package persistence

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kairos-io/kairos-go/api/v1"
	"github.com/kairos-io/kairos-go/internal/eventlog"
	"github.com/kairos-io/kairos-go/internal/wal"
	"google.golang.org/protobuf/proto"
)

const manifestFile = "manifest.json"

type Engine struct {
	dir   string
	mu    sync.RWMutex
	log   *eventlog.AppendOnlyStore
	wal   *wal.WAL
	man   *Manifest
	opts  Options
}

type Options struct {
	SnapshotInterval int
	Compression      bool
	RetentionCount   int
}

type Manifest struct {
	Version      int               `json:"version"`
	Snapshots    []SnapshotMeta    `json:"snapshots"`
	LastEventID  string            `json:"last_event_id"`
	LastHLC      int64             `json:"last_hlc"`
	EventCount   int64             `json:"event_count"`
}

type SnapshotMeta struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	EventID   string `json:"event_id"`
	HLC       int64  `json:"hlc"`
	Size      int64  `json:"size"`
	CreatedAt int64  `json:"created_at"`
	EventFrom int64  `json:"event_from"`
	EventTo   int64  `json:"event_to"`
}

func Open(dir string, opts Options) (*Engine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if opts.RetentionCount <= 0 {
		opts.RetentionCount = 5
	}

	e := &Engine{dir: dir, opts: opts}

	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, wal.Options{MaxSegmentSize: 64 << 20})
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	e.wal = w

	logPath := filepath.Join(dir, "events.db")
	log, err := eventlog.NewAppendOnlyStore(logPath)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("log open: %w", err)
	}
	e.log = log

	if err := e.loadManifest(); err != nil {
		w.Close()
		return nil, fmt.Errorf("manifest: %w", err)
	}

	if err := e.recover(); err != nil {
		return nil, fmt.Errorf("recover: %w", err)
	}

	return e, nil
}

func (e *Engine) Append(ctx context.Context, events []*v1.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Write(events); err != nil {
		return err
	}

	if err := e.log.Append(ctx, events); err != nil {
		return err
	}

	e.man.EventCount += int64(len(events))
	if len(events) > 0 {
		e.man.LastEventID = events[len(events)-1].Id
		e.man.LastHLC = events[len(events)-1].HlcTimestamp
	}

	shouldSnapshot := false
	if e.opts.SnapshotInterval > 0 {
		shouldSnapshot = int(e.man.EventCount)%e.opts.SnapshotInterval == 0
	}

	if shouldSnapshot {
		if err := e.takeSnapshot(); err != nil {
			return err
		}
	}

	return e.saveManifest()
}

func (e *Engine) Replay(ctx context.Context, fn func(*v1.Event) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.wal.Replay(fn)
}

func (e *Engine) ReplayFrom(ctx context.Context, afterID string, fn func(*v1.Event) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	it, err := e.log.Iter(ctx, eventlog.IterOptions{
		AfterID: afterID,
	})
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		if err := fn(it.Event()); err != nil {
			return err
		}
	}
	return it.Err()
}

func (e *Engine) ReplayRange(ctx context.Context, from, to int64, fn func(*v1.Event) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	it, err := e.log.Iter(ctx, eventlog.IterOptions{
		SinceHLC: from,
		UntilHLC: to,
	})
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		if err := fn(it.Event()); err != nil {
			return err
		}
	}
	return it.Err()
}

func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.takeSnapshot()
}

func (e *Engine) Snapshots() []SnapshotMeta {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SnapshotMeta, len(e.man.Snapshots))
	copy(out, e.man.Snapshots)
	return out
}

func (e *Engine) RestoreLatest(ctx context.Context) ([]*v1.Event, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	snap := e.latestSnapshot()
	if snap == nil {
		return nil, errors.New("no snapshots available")
	}

	var events []*v1.Event
	if err := e.restoreSnapshot(snap, &events); err != nil {
		return nil, err
	}

	return events, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.wal.Sync()
	e.wal.Close()
	e.log.Close()
	return e.saveManifest()
}

func (e *Engine) Stats() (eventCount int64, snapshotCount int) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.man.EventCount, len(e.man.Snapshots)
}

func (e *Engine) takeSnapshot() error {
	snapDir := filepath.Join(e.dir, "snapshots")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return err
	}

	id := fmt.Sprintf("snap-%020d", time.Now().UnixNano())
	path := filepath.Join(snapDir, id+".snap")
	eventFrom := int64(0)
	if len(e.man.Snapshots) > 0 {
		eventFrom = e.man.Snapshots[len(e.man.Snapshots)-1].EventTo
	}
	eventTo := e.man.EventCount

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var w io.WriteCloser = f
	if e.opts.Compression {
		w = gzip.NewWriter(f)
	}

	meta := SnapshotMeta{
		ID:        id,
		Path:      path,
		EventID:   e.man.LastEventID,
		HLC:       e.man.LastHLC,
		CreatedAt: time.Now().UnixNano(),
		EventFrom: eventFrom,
		EventTo:   eventTo,
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	if err := writeLengthPrefixed(w, metaData); err != nil {
		return err
	}

	it, err := e.log.Iter(context.Background(), eventlog.IterOptions{
		Limit: int(eventTo - eventFrom),
	})
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		data, err := proto.Marshal(it.Event())
		if err != nil {
			return err
		}
		if err := writeLengthPrefixed(w, data); err != nil {
			return err
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if e.opts.Compression {
		w.Close()
	}

	info, _ := f.Stat()
	if info != nil {
		meta.Size = info.Size()
	}

	e.man.Snapshots = append(e.man.Snapshots, meta)
	e.enforceRetention()

	return nil
}

func (e *Engine) restoreSnapshot(snap *SnapshotMeta, events *[]*v1.Event) error {
	f, err := os.Open(snap.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.ReadCloser = f
	if strings.HasSuffix(snap.Path, ".snap.gz") || e.opts.Compression {
		r, err = gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer r.Close()
	}

	metaData, err := readLengthPrefixed(r)
	if err != nil {
		return fmt.Errorf("read snapshot meta: %w", err)
	}

	var readMeta SnapshotMeta
	if err := json.Unmarshal(metaData, &readMeta); err != nil {
		return fmt.Errorf("unmarshal snapshot meta: %w", err)
	}

	for {
		data, err := readLengthPrefixed(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var ev v1.Event
		if err := proto.Unmarshal(data, &ev); err != nil {
			return err
		}
		*events = append(*events, &ev)
	}

	return nil
}

func (e *Engine) latestSnapshot() *SnapshotMeta {
	if len(e.man.Snapshots) == 0 {
		return nil
	}
	return &e.man.Snapshots[len(e.man.Snapshots)-1]
}

func (e *Engine) enforceRetention() {
	if e.opts.RetentionCount <= 0 || len(e.man.Snapshots) <= e.opts.RetentionCount {
		return
	}
	remove := len(e.man.Snapshots) - e.opts.RetentionCount
	for i := 0; i < remove; i++ {
		os.Remove(e.man.Snapshots[i].Path)
	}
	e.man.Snapshots = e.man.Snapshots[remove:]
}

func (e *Engine) recover() error {
	return nil
}

func (e *Engine) loadManifest() error {
	path := filepath.Join(e.dir, manifestFile)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			e.man = &Manifest{Version: 1}
			return nil
		}
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(&e.man)
}

func (e *Engine) saveManifest() error {
	path := filepath.Join(e.dir, manifestFile+".tmp")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(e.man); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(path, filepath.Join(e.dir, manifestFile))
}

func writeLengthPrefixed(w io.Writer, data []byte) error {
	var header [8]byte
	length := uint64(len(data))
	header[0] = byte(length >> 56)
	header[1] = byte(length >> 48)
	header[2] = byte(length >> 40)
	header[3] = byte(length >> 32)
	header[4] = byte(length >> 24)
	header[5] = byte(length >> 16)
	header[6] = byte(length >> 8)
	header[7] = byte(length)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readLengthPrefixed(r io.Reader) ([]byte, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := uint64(0)
	for i := 0; i < 8; i++ {
		length = (length << 8) | uint64(header[i])
	}
	if length > 1<<30 {
		return nil, errors.New("snapshot entry too large")
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func SnapshotFiles(dir string) ([]string, error) {
	snapDir := filepath.Join(dir, "snapshots")
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".snap") {
			files = append(files, filepath.Join(snapDir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}
