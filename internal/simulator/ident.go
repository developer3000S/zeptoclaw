package simulator

// Determinism is a requirement, not a nicety: a load report that cannot be
// reproduced is an anecdote. Everything random in the model flows either through
// identitySet (peer ids derived from the seed) or through rng below, and every
// stream is seeded from the run seed plus a domain constant, so no phase can
// shift another phase's draw.

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// rng is a named wrapper over math/rand's source so a call site reads as what
// it draws and the model never touches the global source.
type rng struct{ r *rand.Rand }

func newRNG(seed int64) *rng { return &rng{r: rand.New(rand.NewSource(seed))} }

func (x *rng) float() float64 { return x.r.Float64() }
func (x *rng) intn(n int) int { return x.r.Intn(n) }

// permShuffle is Fisher-Yates: it reorders the slice in place with a uniform
// permutation, which is what gossipsub does when it picks mesh peers, gossips
// IHAVE, or orders task candidates.
func (x *rng) permShuffle(a []int32) {
	for i := len(a) - 1; i > 0; i-- {
		j := x.r.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

// pickRound returns up to k of a candidate set, rotated by an offset derived from the
// caller and the beat. The router shuffles its peers before taking a prefix, so a plain
// sorted prefix would bias the lowest-indexed peers; drawing the shuffle from the run's
// shared stream would make the outcome depend on how many draws had already happened,
// which is a property of the harness. Rotation is deterministic per (caller, beat) and
// walks the whole set as the beat advances.
func pickRound(cands []int32, caller int32, beat int32, k int) []int32 {
	if len(cands) == 0 || k <= 0 {
		return nil
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a] < cands[b] })
	if k >= len(cands) {
		return cands
	}
	off := int((int64(caller)*7 + int64(beat)) % int64(len(cands)))
	out := make([]int32, 0, k)
	for j := 0; j < k; j++ {
		out = append(out, cands[(off+j)%len(cands)])
	}
	return out
}

func (x *rng) permShuffleItems(a []qitem) {
	for i := len(a) - 1; i > 0; i-- {
		j := x.r.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

// identitySet derives the mesh's peer identities from a seed. Ed25519 keys come
// from a deterministic reader, so peer ids — and with them the base58 strings
// that dominate a PeerState's wire size — are reproducible run to run.
func identitySet(n int, seed int64, listen []string) ([]peer.ID, [][]string, error) {
	ids := make([]peer.ID, n)
	addrs := make([][]string, n)
	src := rand.New(rand.NewSource(seed ^ 0x2f6b_2f6b_2f6b_2f6b))
	for i := 0; i < n; i++ {
		_, pub, err := crypto.GenerateEd25519Key(src)
		if err != nil {
			return nil, nil, fmt.Errorf("simulator: keygen: %w", err)
		}
		pid, err := peer.IDFromPublicKey(pub)
		if err != nil {
			return nil, nil, fmt.Errorf("simulator: peer id: %w", err)
		}
		ids[i] = pid
		addrs[i] = announceAddrs(pid, listen, i)
	}
	return ids, addrs, nil
}

// announceAddrs renders the /p2p-terminated multiaddrs a node publishes in the
// shape node.DiscoverableAddrs produces: one address per listen template with
// the node's own peer id appended. The addresses are synthetic because the model
// dials nothing, but their length is not: a PeerState carries them, and a
// multiaddr that is shorter than reality would understate every gossip frame.
func announceAddrs(pid peer.ID, listen []string, idx int) []string {
	host := fmt.Sprintf("10.%d.%d.%d", (idx>>8)&0xff, idx&0xff, (idx*37)%253+2)
	out := make([]string, 0, len(listen))
	for _, tmpl := range listen {
		a := tmpl
		if i := strings.Index(a, "0.0.0.0"); i >= 0 {
			a = a[:i] + host + a[i+len("0.0.0.0"):]
		}
		out = append(out, fmt.Sprintf("%s/p2p/%s", a, pid.String()))
	}
	if len(out) == 0 {
		out = []string{fmt.Sprintf("/ip4/%s/tcp/4001/p2p/%s", host, pid.String())}
	}
	return out
}
