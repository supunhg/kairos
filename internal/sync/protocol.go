package sync

import (
	"context"
	"encoding/json"

	v1 "github.com/supunhg/kairos/api/v1"
)

type SyncProtocol struct {
	manager *SyncManager
	engine  *Engine
}

type SyncRequest struct {
	NodeID   string           `json:"node_id"`
	Groups   []string         `json:"groups"`
	Filter   []byte           `json:"filter,omitempty"`
	Versions map[string]int64 `json:"versions,omitempty"`
}

type SyncResponse struct {
	NodeID   string          `json:"node_id"`
	Groups   []GroupSyncInfo `json:"groups"`
	Complete bool            `json:"complete"`
}

type GroupSyncInfo struct {
	GroupID    string           `json:"group_id"`
	GroupType  GroupType        `json:"group_type"`
	Versions   map[string]int64 `json:"versions"`
	EventCount int              `json:"event_count"`
}

func NewSyncProtocol(engine *Engine) *SyncProtocol {
	return &SyncProtocol{
		manager: NewSyncManager(engine),
		engine:  engine,
	}
}

func (sp *SyncProtocol) Manager() *SyncManager {
	return sp.manager
}

func (sp *SyncProtocol) HandleSyncRequest(ctx context.Context, req *SyncRequest) (*SyncResponse, error) {
	sp.manager.RegisterPeer(req.NodeID)

	resp := &SyncResponse{
		NodeID:   sp.engine.nodeID,
		Complete: true,
	}

	for _, groupID := range sp.engine.GroupIDs() {
		vv := sp.engine.GetVersionVector(groupID)
		if vv == nil {
			continue
		}

		if req.Versions != nil {
			sp.manager.MergeVersionVector(req.NodeID, groupID, req.Versions)
		}

		info := GroupSyncInfo{
			GroupID:    groupID,
			Versions:   vv,
			EventCount: len(vv),
		}

		group := sp.engine.groups[groupID]
		if group != nil {
			info.GroupType = group.Type
		}
		resp.Groups = append(resp.Groups, info)
	}

	return resp, nil
}

func (sp *SyncProtocol) HandleSyncResponse(ctx context.Context, resp *SyncResponse) error {
	sp.manager.RegisterPeer(resp.NodeID)

	for _, info := range resp.Groups {
		sp.manager.MergeVersionVector(resp.NodeID, info.GroupID, info.Versions)
	}
	return nil
}

func (sp *SyncProtocol) BuildSyncRequest(ctx context.Context) *SyncRequest {
	return &SyncRequest{
		NodeID: sp.engine.nodeID,
		Groups: sp.engine.GroupIDs(),
	}
}

func MarshalSyncRequest(req *SyncRequest) ([]byte, error) {
	return json.Marshal(req)
}

func UnmarshalSyncRequest(data []byte) (*SyncRequest, error) {
	var req SyncRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func MarshalSyncResponse(resp *SyncResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func UnmarshalSyncResponse(data []byte) (*SyncResponse, error) {
	var resp SyncResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func MarshalEvents(events []*v1.Event) ([]byte, error) {
	return json.Marshal(events)
}

func UnmarshalEvents(data []byte) ([]*v1.Event, error) {
	var events []*v1.Event
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}
	return events, nil
}
