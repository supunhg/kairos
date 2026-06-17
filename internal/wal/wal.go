package wal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/supunhg/kairos/api/v1"
	"google.golang.org/protobuf/proto"
)

const (
	magicNumber  = 0x4B414952
	segmentExt   = ".wal"
	entryHeader  = 8 // crc32(4) + length(4)
	defaultSize  = 64 << 20 // 64MB segments
	defaultSync  = 1 << 20  // sync every 1MB
)

type Entry struct {
	CRC   uint32
	Event *v1.Event
}

type WAL struct {
	dir       string
	mu        sync.RWMutex
	segments  []*segment
	active    *segment
	opts      Options
	closed    bool
	maxActive int
}

type Options struct {
	MaxSegmentSize int
	SyncInterval   int
	Sync           bool
	MaxSegments    int
}

type segment struct {
	file   *os.File
	path   string
	offset int64
	size   int
}

func Open(dir string, opts Options) (*WAL, error) {
	if opts.MaxSegmentSize <= 0 {
		opts.MaxSegmentSize = defaultSize
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaultSync
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, opts: opts}
	if err := w.loadSegments(); err != nil {
		return nil, err
	}
	return w, w.openActive()
}

func (w *WAL) Write(events []*v1.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal closed")
	}

	for _, ev := range events {
		data, err := proto.Marshal(ev)
		if err != nil {
			return err
		}

		entry := make([]byte, entryHeader+len(data))
		crc := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(entry[:4], crc)
		binary.BigEndian.PutUint32(entry[4:8], uint32(len(data)))
		copy(entry[8:], data)

		if _, err := w.active.file.Write(entry); err != nil {
			return err
		}
		w.active.offset += int64(len(entry))
		w.active.size += len(entry)

		if w.active.size >= w.opts.MaxSegmentSize {
			if err := w.rotate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.active != nil {
		return w.active.file.Sync()
	}
	return nil
}

func (w *WAL) ReadFrom(offset int64, fn func(*v1.Event) error) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}

	segIdx := 0
	for i, seg := range w.segments {
		if seg.offset > offset || (i == len(w.segments)-1) {
			segIdx = i
			break
		}
		offset -= seg.offset
	}

	for i := segIdx; i < len(w.segments); i++ {
		seg := w.segments[i]
		if _, err := seg.file.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		offset = 0

		for {
			var header [entryHeader]byte
			if _, err := io.ReadFull(seg.file, header[:]); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			crc := binary.BigEndian.Uint32(header[:4])
			length := binary.BigEndian.Uint32(header[4:8])

			if length > 1<<24 {
				return fmt.Errorf("wal: entry too large: %d", length)
			}

			data := make([]byte, length)
			if _, err := io.ReadFull(seg.file, data); err != nil {
				return err
			}

			if crc32.ChecksumIEEE(data) != crc {
				return errors.New("wal: crc mismatch")
			}

			var ev v1.Event
			if err := proto.Unmarshal(data, &ev); err != nil {
				return err
			}
			if err := fn(&ev); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *WAL) Replay(fn func(*v1.Event) error) error {
	return w.ReadFrom(0, fn)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	for _, seg := range w.segments {
		seg.file.Sync()
		seg.file.Close()
	}
	w.segments = nil
	w.active = nil
	return nil
}

func (w *WAL) Stats() (segments int, size int64) {
	for _, seg := range w.segments {
		size += seg.offset
	}
	return len(w.segments), size
}

func (w *WAL) Segments() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.segments)
}

func (w *WAL) loadSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	var paths []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == segmentExt {
			paths = append(paths, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Strings(paths)

	for _, p := range paths {
		seg, err := openSegment(p)
		if err != nil {
			return err
		}
		w.segments = append(w.segments, seg)
	}

	return nil
}

func (w *WAL) openActive() error {
	if len(w.segments) == 0 {
		return w.newSegment()
	}
	last := w.segments[len(w.segments)-1]
	last.file.Close()
	f, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	last.file = f
	w.active = last
	return nil
}

func (w *WAL) rotate() error {
	if w.opts.Sync && w.active != nil {
		w.active.file.Sync()
	}
	w.maxActive++
	if err := w.newSegment(); err != nil {
		return err
	}
	w.compact()
	return nil
}

func (w *WAL) compact() {
	maxSegs := w.opts.MaxSegments
	if maxSegs <= 0 {
		return
	}
	if len(w.segments) <= maxSegs {
		return
	}
	remove := len(w.segments) - maxSegs
	for i := 0; i < remove; i++ {
		seg := w.segments[i]
		seg.file.Close()
		os.Remove(seg.path)
	}
	w.segments = w.segments[remove:]
}

func (w *WAL) newSegment() error {
	name := fmt.Sprintf("%020d-%s%s", time.Now().UnixNano(), randStr(8), segmentExt)
	path := filepath.Join(w.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	seg := &segment{
		file: f,
		path: path,
		size: 0,
	}
	w.segments = append(w.segments, seg)
	w.active = seg
	return nil
}

func openSegment(path string) (*segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{
		file:   f,
		path:   path,
		offset: info.Size(),
		size:   int(info.Size()),
	}, nil
}

func randStr(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		var buf [1]byte
		if _, err := rand.Read(buf[:]); err != nil {
			b[i] = letters[i%len(letters)]
			continue
		}
		b[i] = letters[int(buf[0])%len(letters)]
	}
	return string(b)
}


