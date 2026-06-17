package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/supunhg/kairos/api/v1"
)

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	e.Close()

	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal("manifest should exist after close:", err)
	}

	e2, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	ec, sc := e2.Stats()
	if ec != 0 {
		t.Fatalf("expected 0 events, got %d", ec)
	}
	if sc != 0 {
		t.Fatalf("expected 0 snapshots, got %d", sc)
	}
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}

	events := []*v1.Event{
		{Id: "e1", PayloadType: "test", HlcTimestamp: 100, GroupId: "g1"},
		{Id: "e2", PayloadType: "test", HlcTimestamp: 200, GroupId: "g1"},
	}
	if err := e.Append(nil, events); err != nil {
		t.Fatal(err)
	}
	e.Close()

	e2, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	var replayed []*v1.Event
	if err := e2.Replay(nil, func(ev *v1.Event) error {
		replayed = append(replayed, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != 2 {
		t.Fatalf("expected 2 events, got %d", len(replayed))
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 10})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 25; i++ {
		id := "ev-" + string(rune('0'+i%10))
		if err := e.Append(nil, []*v1.Event{{
			Id:           id,
			PayloadType:  "test",
			HlcTimestamp: int64(i),
			GroupId:      "g1",
		}}); err != nil {
			t.Fatal(err)
		}
	}
	e.Close()

	e2, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	ec, sc := e2.Stats()
	if sc == 0 {
		t.Fatal("expected at least 1 snapshot")
	}
	if ec == 0 {
		t.Fatal("expected events to persist")
	}

	restored, err := e2.RestoreLatest(nil)
	if err != nil {
		t.Fatalf("RestoreLatest error: %v", err)
	}
	if len(restored) == 0 {
		snaps := e2.Snapshots()
		t.Fatalf("expected restored events, have %d snapshots", len(snaps))
	}
	t.Logf("restored %d events from %d snapshots", len(restored), sc)
}

func TestSnapshotFiles(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 5})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		e.Append(nil, []*v1.Event{{
			Id:           "e-" + string(rune('0'+i%10)),
			PayloadType:  "test",
			HlcTimestamp: int64(i),
		}})
	}
	e.Close()

	files, err := SnapshotFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("expected snapshot files")
	}
}

func TestReplayRange(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		e.Append(nil, []*v1.Event{{
			Id:           "er-" + string(rune('0'+i)),
			PayloadType:  "test",
			HlcTimestamp: int64(i * 100),
		}})
	}
	e.Close()

	e2, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	var events []*v1.Event
	e2.ReplayRange(nil, 200, 500, func(ev *v1.Event) error {
		events = append(events, ev)
		return nil
	})

	if len(events) != 4 {
		t.Fatalf("expected 4 events in range 200-500, got %d", len(events))
	}
}

func TestManualSnapshot(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		id := "ms-" + string(rune('0'+i))
		if err := e.Append(nil, []*v1.Event{{
			Id:           id,
			PayloadType:  "test",
			HlcTimestamp: int64(i),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := e.Snapshot(); err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	snaps := e.Snapshots()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}

	e.Close()

	e2, err := Open(dir, Options{SnapshotInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	restored, err := e2.RestoreLatest(nil)
	if err != nil {
		t.Fatalf("RestoreLatest error: %v", err)
	}

	t.Logf("restored %d events", len(restored))
	if len(restored) != 5 {
		t.Fatalf("expected 5 events, got %d", len(restored))
	}
}
