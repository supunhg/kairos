package sync

import (
	"context"
	"testing"
)

func TestSyncManagerRegisterPeer(t *testing.T) {
	engine := NewEngine("node1")
	sm := NewSyncManager(engine)

	ps := sm.RegisterPeer("peer-1")
	if ps == nil {
		t.Fatal("expected peer state")
	}
	if ps.State() != SyncIdle {
		t.Fatalf("expected SyncIdle, got %v", ps.State())
	}
}

func TestSyncManagerGetPeer(t *testing.T) {
	engine := NewEngine("node1")
	sm := NewSyncManager(engine)

	sm.RegisterPeer("peer-1")

	ps, ok := sm.GetPeer("peer-1")
	if !ok {
		t.Fatal("expected to find peer")
	}
	if ps.peerID != "peer-1" {
		t.Fatalf("expected peerID 'peer-1', got '%s'", ps.peerID)
	}
}

func TestSyncManagerGetUnknownPeer(t *testing.T) {
	engine := NewEngine("node1")
	sm := NewSyncManager(engine)

	_, ok := sm.GetPeer("unknown")
	if ok {
		t.Fatal("expected not to find unknown peer")
	}
}

func TestPeerStateVersion(t *testing.T) {
	ps := &PeerState{
		peerID:  "test",
		state:   SyncIdle,
		version: make(map[string]int64),
	}

	ps.UpdateVersion("doc1", 100)
	ps.UpdateVersion("doc2", 200)

	vv := ps.GetVersion()
	if vv["doc1"] != 100 {
		t.Fatalf("expected 100, got %d", vv["doc1"])
	}
	if vv["doc2"] != 200 {
		t.Fatalf("expected 200, got %d", vv["doc2"])
	}
}

func TestSyncManagerMergeVersionVector(t *testing.T) {
	engine := NewEngine("node1")
	ctx := context.Background()
	sm := NewSyncManager(engine)

	engine.TextInsert(ctx, "doc1", 0, "Hello")

	peerVV := map[string]int64{
		"node2": 100,
	}

	merged := sm.MergeVersionVector("peer-1", "doc1", peerVV)
	if merged["node2"] != 100 {
		t.Fatalf("expected merged to include node2:100")
	}
}

func TestSyncManagerComputeDelta(t *testing.T) {
	engine := NewEngine("node1")
	ctx := context.Background()
	sm := NewSyncManager(engine)

	engine.TextInsert(ctx, "doc1", 0, "Hello")

	sm.RegisterPeer("peer-1")

	missing, err := sm.ComputeDelta(ctx, "peer-1", "doc1")
	if err != nil {
		t.Fatal(err)
	}

	if len(missing) == 0 {
		t.Fatal("expected peer to be missing events from node1")
	}
}

func TestSyncManagerComputeDeltaAfterUpdate(t *testing.T) {
	engine := NewEngine("node1")
	ctx := context.Background()
	sm := NewSyncManager(engine)

	engine.TextInsert(ctx, "doc1", 0, "A")

	sm.RegisterPeer("peer-1")

	v1, _ := sm.ComputeDelta(ctx, "peer-1", "doc1")
	ps, _ := sm.GetPeer("peer-1")
	ps.UpdateVersion("node1", engine.GetVersionVector("doc1")["node1"])

	v2, _ := sm.ComputeDelta(ctx, "peer-1", "doc1")
	if len(v2) != 0 {
		t.Fatalf("expected 0 missing after version update, got %d", len(v2))
	}

	if len(v1) == 0 {
		t.Fatal("expected non-empty before version update")
	}
}

func TestSyncManagerMultiplePeers(t *testing.T) {
	engine := NewEngine("node1")
	sm := NewSyncManager(engine)

	sm.RegisterPeer("peer-1")
	sm.RegisterPeer("peer-2")
	sm.RegisterPeer("peer-3")

	peers := sm.KnownPeers()
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}

	if sm.PeerCount() != 3 {
		t.Fatalf("expected count 3, got %d", sm.PeerCount())
	}
}

func TestSyncManagerRemovePeer(t *testing.T) {
	engine := NewEngine("node1")
	sm := NewSyncManager(engine)

	sm.RegisterPeer("peer-1")
	sm.RemovePeer("peer-1")

	if sm.PeerCount() != 0 {
		t.Fatalf("expected 0 peers after removal")
	}
}

func TestPeerStateTransitions(t *testing.T) {
	ps := &PeerState{
		peerID:  "test",
		state:   SyncIdle,
		version: make(map[string]int64),
	}

	ps.SetState(SyncRequested)
	if ps.State() != SyncRequested {
		t.Fatalf("expected SyncRequested")
	}

	ps.SetState(SyncInProgress)
	if ps.State() != SyncInProgress {
		t.Fatalf("expected SyncInProgress")
	}

	ps.SetState(SyncComplete)
	if ps.State() != SyncComplete {
		t.Fatalf("expected SyncComplete")
	}
}
