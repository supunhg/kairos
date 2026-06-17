package sync

import (
	"context"
	"fmt"
	"sync"
)

type SyncState int

const (
	SyncIdle      SyncState = iota
	SyncRequested
	SyncInProgress
	SyncComplete
)

type PeerState struct {
	mu          sync.RWMutex
	peerID      string
	state       SyncState
	version     map[string]int64
	filter      *BloomFilter
	filterItems int
}

type SyncManager struct {
	mu     sync.RWMutex
	peers  map[string]*PeerState
	engine *Engine
}

func NewSyncManager(engine *Engine) *SyncManager {
	return &SyncManager{
		peers:  make(map[string]*PeerState),
		engine: engine,
	}
}

func (sm *SyncManager) RegisterPeer(peerID string) *PeerState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ps := &PeerState{
		peerID:  peerID,
		state:   SyncIdle,
		version: make(map[string]int64),
	}
	sm.peers[peerID] = ps
	return ps
}

func (sm *SyncManager) GetPeer(peerID string) (*PeerState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ps, ok := sm.peers[peerID]
	return ps, ok
}

func (sm *SyncManager) KnownPeers() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var ids []string
	for id := range sm.peers {
		ids = append(ids, id)
	}
	return ids
}

func (sm *SyncManager) RemovePeer(peerID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.peers, peerID)
}

func (ps *PeerState) State() SyncState {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.state
}

func (ps *PeerState) SetState(s SyncState) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state = s
}

func (ps *PeerState) UpdateVersion(groupID string, timestamp int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.version[groupID] = timestamp
}

func (ps *PeerState) GetVersion() map[string]int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	vv := make(map[string]int64)
	for k, v := range ps.version {
		vv[k] = v
	}
	return vv
}

func (ps *PeerState) BuildFilter(engine *Engine) *BloomFilter {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	totalEvents := 0
	for _, g := range engine.groups {
		totalEvents += len(g.version)
	}
	if totalEvents < 10 {
		totalEvents = 100
	}

	bf := NewBloomFilter(totalEvents, 0.001)
	for gid := range engine.groups {
		bf.AddString(gid)
		for originator := range engine.groups[gid].version {
			bf.AddString(fmt.Sprintf("%s:%s", gid, originator))
		}
	}
	ps.filter = bf
	ps.filterItems = totalEvents
	return bf
}

func (sm *SyncManager) ComputeDelta(ctx context.Context, peerID, groupID string) ([]string, error) {
	ps, ok := sm.GetPeer(peerID)
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", peerID)
	}

	peerVV := ps.GetVersion()
	localVV := sm.engine.GetVersionVector(groupID)
	if localVV == nil {
		return nil, ErrGroupNotFound
	}

	var missing []string
	for originator, localTS := range localVV {
		peerTS, seen := peerVV[originator]
		if !seen || localTS > peerTS {
			missing = append(missing, originator)
		}
	}
	return missing, nil
}

func (sm *SyncManager) MergeVersionVector(peerID, groupID string, peerVV map[string]int64) map[string]int64 {
	ps, ok := sm.GetPeer(peerID)
	if !ok {
		ps = sm.RegisterPeer(peerID)
	}

	for originator, ts := range peerVV {
		ps.UpdateVersion(originator, ts)
	}

	localVV := sm.engine.GetVersionVector(groupID)
	if localVV == nil {
		return peerVV
	}

	merged := make(map[string]int64)
	for k, v := range localVV {
		merged[k] = v
	}
	for k, v := range peerVV {
		if existing, ok := merged[k]; !ok || v > existing {
			merged[k] = v
		}
	}
	return merged
}

func (sm *SyncManager) PeerCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.peers)
}

func (sm *SyncManager) SyncAllPeers(ctx context.Context) map[string]error {
	peers := sm.KnownPeers()
	errs := make(map[string]error)
	for _, peerID := range peers {
		if err := sm.SyncPeer(ctx, peerID); err != nil {
			errs[peerID] = err
		}
	}
	return errs
}

func (sm *SyncManager) SyncPeer(ctx context.Context, peerID string) error {
	ps, ok := sm.GetPeer(peerID)
	if !ok {
		return fmt.Errorf("unknown peer: %s", peerID)
	}
	ps.SetState(SyncRequested)
	defer ps.SetState(SyncComplete)

	ps.SetState(SyncInProgress)
	for _, groupID := range sm.engine.GroupIDs() {
		missing, err := sm.ComputeDelta(ctx, peerID, groupID)
		if err != nil {
			continue
		}
		if len(missing) > 0 {
			ps.UpdateVersion(missing[0], 0)
		}
	}
	return nil
}
