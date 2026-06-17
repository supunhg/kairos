package eventlog

import (
	"path/filepath"
	"testing"

	"github.com/kairos-io/kairos-go/api/v1"
)

func TestAppendAndGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	s, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ev := &v1.Event{Id: "ev-1", PayloadType: "test", HlcTimestamp: 100, GroupId: "g1"}
	if err := s.Append(nil, []*v1.Event{ev}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(nil, "ev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != "ev-1" {
		t.Fatalf("expected id ev-1, got %s", got.Id)
	}
}

func TestIterateEvents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	s, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		ev := &v1.Event{
			Id:           string(rune('0' + i)),
			PayloadType:  "test",
			HlcTimestamp: int64(i),
			GroupId:      "g1",
		}
		s.Append(nil, []*v1.Event{ev})
	}
	s.Close()

	s2, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	it, err := s2.Iter(nil, IterOptions{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	var count int
	for it.Next() {
		count++
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if count != 5 {
		t.Fatalf("expected 5 events, got %d", count)
	}
}

func TestFilterByGroup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	s, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	events := []*v1.Event{
		{Id: "1", PayloadType: "test", GroupId: "g1", HlcTimestamp: 1},
		{Id: "2", PayloadType: "test", GroupId: "g2", HlcTimestamp: 2},
		{Id: "3", PayloadType: "test", GroupId: "g1", HlcTimestamp: 3},
	}
	s.Append(nil, events)

	it, err := s.Iter(nil, IterOptions{GroupID: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	var ids []string
	for it.Next() {
		ids = append(ids, it.Event().Id)
	}
	if len(ids) != 2 || ids[0] != "1" || ids[1] != "3" {
		t.Fatalf("expected [1 3], got %v", ids)
	}
}

func TestPersistenceAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	s, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		s.Append(nil, []*v1.Event{{
			Id:           "ev-" + string(rune('0'+i%10)),
			PayloadType:  "test",
			HlcTimestamp: int64(i),
		}})
	}
	s.Close()

	s2, err := NewAppendOnlyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	_, err = s2.Get(nil, "ev-2")
	if err != nil {
		t.Fatal("expected event ev-2 to persist: ", err)
	}

	st := s2.Stats()
	if st.EventCount == 0 {
		t.Fatal("expected events after restart")
	}
}

func TestAppendOnlyStoreCodec(t *testing.T) {
	ev := &v1.Event{
		Id: "test-id", PayloadType: "test-type", HlcTimestamp: 12345,
		GroupId: "g1", SessionId: "s1", Originator: "user1",
		CausalDeps: []string{"dep1", "dep2"},
	}

	data, err := MarshalEvent(ev)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Id != "test-id" {
		t.Fatalf("expected test-id, got %s", got.Id)
	}
	if got.HlcTimestamp != 12345 {
		t.Fatalf("expected 12345, got %d", got.HlcTimestamp)
	}
	if len(got.CausalDeps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(got.CausalDeps))
	}
}
