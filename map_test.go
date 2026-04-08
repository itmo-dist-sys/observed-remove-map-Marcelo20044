package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nikitakosatka/hive/pkg/failure"
	"github.com/nikitakosatka/hive/pkg/hive"
	"github.com/nikitakosatka/hive/pkg/network"
	timemodel "github.com/nikitakosatka/hive/pkg/time"
)

func TestCRDTMap_Behavioral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, n *CRDTMapNode)
	}{
		{
			name: "put_followed_by_get",
			run: func(t *testing.T, n *CRDTMapNode) {
				n.Put("user:1", "alice")

				got, ok := n.Get("user:1")
				require.True(t, ok, "value must exist after Put")
				assert.Equal(t, "alice", got)
			},
		},
		{
			name: "delete_followed_by_get_returns_not_found",
			run: func(t *testing.T, n *CRDTMapNode) {
				n.Put("user:1", "alice")
				n.Delete("user:1")

				_, ok := n.Get("user:1")
				assert.False(t, ok, "deleted key must be reported as not found")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := NewCRDTMapNode("A", []string{"A"})
			tt.run(t, n)
		})
	}
}

func TestCRDTMap_CRDTInvariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "commutativity_of_merge",
			run: func(t *testing.T) {
				abA := NewCRDTMapNode("A", []string{"A", "B"})
				abB := NewCRDTMapNode("B", []string{"A", "B"})
				abA.Put("k1", "v1")
				abB.Put("k2", "v2")
				abA.Merge(abB.State())
				abB.Merge(abA.State())

				baA := NewCRDTMapNode("A", []string{"A", "B"})
				baB := NewCRDTMapNode("B", []string{"A", "B"})
				baB.Put("k2", "v2")
				baA.Put("k1", "v1")
				baA.Merge(baB.State())
				baB.Merge(baA.State())

				assert.Equal(t, abA.ToMap(), abB.ToMap(), "both replicas must converge in AB order")
				assert.Equal(t, baA.ToMap(), baB.ToMap(), "both replicas must converge in BA order")
				assert.Equal(t, abA.ToMap(), baA.ToMap(), "AB and BA merge orders must produce identical state")
			},
		},
		{
			name: "idempotency_of_repeated_merge",
			run: func(t *testing.T) {
				source := NewCRDTMapNode("A", []string{"A", "B"})
				target := NewCRDTMapNode("B", []string{"A", "B"})

				source.Put("k", "v")
				source.Delete("obsolete")
				state := source.State()

				target.Merge(state)
				once := target.ToMap()

				target.Merge(state)
				target.Merge(state)
				assert.Equal(t, once, target.ToMap(), "state must stay unchanged after repeated merge")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestCRDTMap_PartitionHealConvergence(t *testing.T) {
	t.Parallel()

	nodeIDs := []string{"A", "B", "C"}
	netCfg := network.NewReliableNetwork()
	timeCfg := timemodel.NewTime(
		timemodel.Synchronous,
		&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
		0.0,
	)
	failCfg := failure.NewCrashStop(0.0)
	cfg := hive.NewConfig(
		hive.WithNetwork(netCfg),
		hive.WithTime(timeCfg),
		hive.WithNodesFailures(failCfg),
	)

	sim := hive.NewSimulator(cfg)
	defer sim.Stop()

	nodes := make([]*CRDTMapNode, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		n := NewCRDTMapNode(id, nodeIDs)
		nodes = append(nodes, n)
		require.NoErrorf(t, sim.AddNode(n), "AddNode(%s)", id)
	}
	require.NoError(t, sim.Start(), "sim.Start")

	netCfg.DropProbability = 1.0
	nodes[0].Put("shared", "value-A")
	nodes[1].Put("shared", "value-B")

	time.Sleep(100 * time.Millisecond)

	netCfg.DropProbability = 0.0

	value, ok := waitUntilConvergedKey(nodes, "shared", 3*time.Second)
	require.True(t, ok, "all nodes must converge to the same visible value after healing")
	assert.Contains(t, []string{"value-A", "value-B"}, value, "unexpected converged value")

	expected := nodes[0].ToMap()
	for i := 1; i < len(nodes); i++ {
		assert.Equal(t, expected, nodes[i].ToMap(), "materialized map must match on all replicas")
	}
}

func waitUntilConvergedKey(nodes []*CRDTMapNode, key string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var value string
		converged := true

		for i, n := range nodes {
			v, ok := n.Get(key)
			if !ok {
				converged = false
				break
			}

			if i == 0 {
				value = v
				continue
			}
			if v != value {
				converged = false
				break
			}
		}

		if converged {
			return value, true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return "", false
}
