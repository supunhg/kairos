// Package crdt provides conflict-free replicated data types.
package crdt

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

type Operation interface {
	Type() string
	Timestamp() int64
	NodeID() string
}

type ID struct {
	Timestamp int64
	NodeID    string
}

func (id ID) Compare(other ID) int {
	if id.Timestamp != other.Timestamp {
		if id.Timestamp < other.Timestamp {
			return -1
		}
		return 1
	}
	if id.NodeID < other.NodeID {
		return -1
	}
	if id.NodeID > other.NodeID {
		return 1
	}
	return 0
}

func (id ID) String() string {
	return strconv.FormatInt(id.Timestamp, 36) + "@" + id.NodeID
}

type LWWRegister struct {
	mu        sync.RWMutex
	value     any
	timestamp int64
	nodeID    string
}

func NewLWWRegister() *LWWRegister {
	return &LWWRegister{}
}

func (r *LWWRegister) Set(value any, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UnixNano()
	if r.timestamp < now || (r.timestamp == now && r.nodeID < nodeID) {
		r.value = value
		r.timestamp = now
		r.nodeID = nodeID
	}
}

func (r *LWWRegister) Get() any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value
}

func (r *LWWRegister) Merge(other *LWWRegister) {
	r.mu.Lock()
	defer r.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()
	if r.timestamp < other.timestamp || (r.timestamp == other.timestamp && r.nodeID < other.nodeID) {
		r.value = other.value
		r.timestamp = other.timestamp
		r.nodeID = other.nodeID
	}
}

func (r *LWWRegister) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.value = nil
	r.timestamp = 0
	r.nodeID = ""
}

type GCounter struct {
	mu     sync.RWMutex
	counts map[string]int64
}

func NewGCounter() *GCounter {
	return &GCounter{counts: make(map[string]int64)}
}

func (c *GCounter) Increment(nodeID string, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[nodeID] += delta
}

func (c *GCounter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total int64
	for _, v := range c.counts {
		total += v
	}
	return total
}

func (c *GCounter) Merge(other *GCounter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()
	for node, count := range other.counts {
		if c.counts[node] < count {
			c.counts[node] = count
		}
	}
}

type PNCounter struct {
	positive *GCounter
	negative *GCounter
}

func NewPNCounter() *PNCounter {
	return &PNCounter{
		positive: NewGCounter(),
		negative: NewGCounter(),
	}
}

func (c *PNCounter) Increment(nodeID string, delta int64) {
	if delta >= 0 {
		c.positive.Increment(nodeID, delta)
	} else {
		c.negative.Increment(nodeID, -delta)
	}
}

func (c *PNCounter) Value() int64 {
	return c.positive.Value() - c.negative.Value()
}

func (c *PNCounter) Merge(other *PNCounter) {
	c.positive.Merge(other.positive)
	c.negative.Merge(other.negative)
}

type Element struct {
	ID      ID
	Value   any
	Removed bool
}

type LWWMap struct {
	mu       sync.RWMutex
	elements map[string]*LWWRegister
}

func NewLWWMap() *LWWMap {
	return &LWWMap{
		elements: make(map[string]*LWWRegister),
	}
}

func (m *LWWMap) Set(key string, value any, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg, ok := m.elements[key]
	if !ok {
		reg = NewLWWRegister()
		m.elements[key] = reg
	}
	reg.Set(value, nodeID)
}

func (m *LWWMap) Delete(key string, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reg, ok := m.elements[key]; ok {
		reg.Set(nil, nodeID)
	}
}

func (m *LWWMap) Get(key string) any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if reg, ok := m.elements[key]; ok {
		return reg.Get()
	}
	return nil
}

func (m *LWWMap) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k, reg := range m.elements {
		if reg.Get() != nil {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (m *LWWMap) Merge(other *LWWMap) {
	m.mu.Lock()
	defer m.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for key, reg := range other.elements {
		existing, ok := m.elements[key]
		if !ok {
			newReg := NewLWWRegister()
			reg.mu.RLock()
			newReg.Set(reg.Get(), reg.nodeID)
			newReg.timestamp = reg.timestamp
			newReg.nodeID = reg.nodeID
			reg.mu.RUnlock()
			m.elements[key] = newReg
		} else {
			existing.Merge(reg)
		}
	}
}

func (m *LWWMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.elements)
}

type RGA struct {
	mu      sync.RWMutex
	nodes   []*rgaNode
	nodeMap map[string]bool
}

type rgaNode struct {
	id      ID
	value   rune
	removed bool
}

func NewRGA() *RGA {
	return &RGA{
		nodeMap: make(map[string]bool),
	}
}

func (r *RGA) Insert(pos int, text string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	runes := []rune(text)
	if len(runes) == 0 {
		return
	}

	insertIdx := r.resolvePos(pos)

	if insertIdx < 0 {
		insertIdx = len(r.nodes)
	}

	baseTS := time.Now().UnixNano()
	for i := 0; i < len(runes); i++ {
		id := ID{Timestamp: baseTS + int64(i), NodeID: nodeID}
		idStr := id.String()
		if r.nodeMap[idStr] {
			continue
		}

		newNode := &rgaNode{id: id, value: runes[i]}
		r.nodeMap[idStr] = true

		newNodes := make([]*rgaNode, 0, len(r.nodes)+1)
		newNodes = append(newNodes, r.nodes[:insertIdx]...)
		newNodes = append(newNodes, newNode)
		newNodes = append(newNodes, r.nodes[insertIdx:]...)
		r.nodes = newNodes
		insertIdx++
	}
}

func (r *RGA) Delete(pos int, length int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	for i, node := range r.nodes {
		if node.removed {
			continue
		}
		if count >= pos && count < pos+length {
			r.nodes[i].removed = true
		}
		count++
	}
}

func (r *RGA) Text() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var runes []rune
	for _, node := range r.nodes {
		if !node.removed {
			runes = append(runes, node.value)
		}
	}
	return string(runes)
}

func (r *RGA) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, node := range r.nodes {
		if !node.removed {
			count++
		}
	}
	return count
}

func (r *RGA) Merge(other *RGA) {
	r.mu.Lock()
	defer r.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for _, node := range other.nodes {
		idStr := node.id.String()
		if r.nodeMap[idStr] {
			continue
		}
		r.nodeMap[idStr] = true
		insertIdx := r.findInsertIdxByID(node.id)
		newNode := &rgaNode{id: node.id, value: node.value, removed: node.removed}
		newNodes := make([]*rgaNode, 0, len(r.nodes)+1)
		newNodes = append(newNodes, r.nodes[:insertIdx]...)
		newNodes = append(newNodes, newNode)
		newNodes = append(newNodes, r.nodes[insertIdx:]...)
		r.nodes = newNodes
	}
}

func (r *RGA) resolvePos(pos int) int {
	count := 0
	for i, node := range r.nodes {
		if count >= pos {
			return i
		}
		if !node.removed {
			count++
		}
	}
	return len(r.nodes)
}

func (r *RGA) findInsertIdxByID(id ID) int {
	for i, node := range r.nodes {
		if node.id.Compare(id) > 0 {
			return i
		}
	}
	return len(r.nodes)
}

func (r *RGA) Compact() {
	r.mu.Lock()
	defer r.mu.Unlock()

	alive := make([]*rgaNode, 0, len(r.nodes))
	for _, node := range r.nodes {
		if !node.removed {
			alive = append(alive, node)
		}
	}
	r.nodes = alive

	r.nodeMap = make(map[string]bool, len(alive))
	for _, node := range alive {
		r.nodeMap[node.id.String()] = true
	}
}
