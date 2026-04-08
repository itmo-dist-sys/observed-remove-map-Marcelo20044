package node

import (
	"context"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
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
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	panic("Not implemented")
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	panic("Not implemented")
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	panic("Not implemented")
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	panic("Not implemented")
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	panic("Not implemented")
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	panic("Not implemented")
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	panic("Not implemented")
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	panic("Not implemented")
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	panic("Not implemented")
}
