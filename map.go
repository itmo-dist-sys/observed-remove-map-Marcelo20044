package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

const antiEntropyInterval = 20 * time.Millisecond

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// newerThan reports whether v is strictly newer than other.
func (v Version) newerThan(other Version) bool {
	if v.Counter != other.Counter {
		return v.Counter > other.Counter
	}
	return v.NodeID > other.NodeID
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode
	mu         sync.RWMutex
	state      MapState
	counter    uint64
	allNodeIDs []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		state:      make(MapState),
		allNodeIDs: allNodeIDs,
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}
	go n.antiEntropy()
	return nil
}

// antiEntropy periodically broadcasts the local state to all peers.
func (n *CRDTMapNode) antiEntropy() {
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()

	ctx := n.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !n.IsRunning() {
				return
			}
			snapshot := n.State()
			for _, peer := range n.allNodeIDs {
				if peer == n.ID() {
					continue
				}
				_ = n.Send(peer, snapshot)
			}
		}
	}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   Version{Counter: n.counter, NodeID: n.ID()},
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, remoteEntry := range remote {
		local, exists := n.state[k]
		if !exists || remoteEntry.Version.newerThan(local.Version) {
			n.state[k] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	snapshot := make(MapState, len(n.state))
	for k, v := range n.state {
		snapshot[k] = v
	}
	return snapshot
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make(map[string]string)
	for k, entry := range n.state {
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	remote, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}
	n.Merge(remote)
	return nil
}
