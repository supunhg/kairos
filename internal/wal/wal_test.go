package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/supunhg/kairos/api/v1"
)

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if w.Segments() != 1 {
		t.Fatalf("expected 1 segment, got %d", w.Segments())
	}
}

func TestWriteAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	events := []*v1.Event{
		{Id: "1", PayloadType: "test", HlcTimestamp: 100, GroupId: "g1"},
		{Id: "2", PayloadType: "test", HlcTimestamp: 200, GroupId: "g1"},
	}

	if err := w.Write(events); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	w2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()

	var replayed []*v1.Event
	if err := w2.Replay(func(ev *v1.Event) error {
		replayed = append(replayed, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatalf("expected 2 events, got %d", len(replayed))
	}
	if replayed[0].Id != "1" {
		t.Fatalf("expected id 1, got %s", replayed[0].Id)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{MaxSegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("ev-%d", i)
		if err := w.Write([]*v1.Event{{
			Id:           id,
			PayloadType:  "test",
			HlcTimestamp: int64(i),
		}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	w2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()

	if w2.Segments() < 2 {
		t.Fatalf("expected multiple segments, got %d", w2.Segments())
	}

	var count int
	_ = w2.Replay(func(ev *v1.Event) error {
		count++
		return nil
	})

	if count != 1000 {
		t.Fatalf("expected 1000 events, got %d", count)
	}
}

func TestCRCIntegrity(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("crc-ev-%d", i)
		if err := w.Write([]*v1.Event{
			{Id: id, PayloadType: "test", HlcTimestamp: int64(i)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Fatal("no WAL segments")
	}
	path := filepath.Join(dir, entries[0].Name())
	data, _ := os.ReadFile(path) //nolint:gosec // G304: test file from known directory

	if len(data) > 28 {
		data[len(data)-1] ^= 0xFF
		_ = os.WriteFile(path, data, 0600) //nolint:gosec // G703: test file from known directory
	}

	w2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()

	var count int
	_ = w2.Replay(func(ev *v1.Event) error {
		count++
		return nil
	})

	if count == 10 {
		t.Fatal("expected CRC corruption to reduce replay count, got all 10")
	}
}

func TestConcurrentWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			id := fmt.Sprintf("conc-%d", n)
			ev := &v1.Event{
				Id:           id,
				PayloadType:  "concurrent",
				HlcTimestamp: int64(n),
			}
			_ = w.Write([]*v1.Event{ev})
			done <- true
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	var count int
	_ = w.Replay(func(ev *v1.Event) error {
		count++
		return nil
	})
	if count != 10 {
		t.Fatalf("expected 10 events, got %d", count)
	}
}
