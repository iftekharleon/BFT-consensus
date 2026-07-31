// Package config defines genesis state, chain parameters, and the initial
// validator set every node must agree on. See PROJECT_SPEC.md §3.13.
package config

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
	"github.com/ileon/bft-consensus/validator"
)

// GenesisValidator is a validator entry as it appears in the genesis JSON
// file: hex-encoded address and public key, plus voting power.
type GenesisValidator struct {
	Address     string `json:"address"`
	PubKey      string `json:"pub_key"`
	VotingPower int64  `json:"voting_power"`
}

// Genesis is the initial state every node must independently derive the
// identical view of: chain identity, the starting validator set, and the
// genesis timestamp.
type Genesis struct {
	ChainID    string             `json:"chain_id"`
	Validators []GenesisValidator `json:"validators"`
	Timestamp  int64              `json:"timestamp"`
}

// Config bundles a loaded Genesis with the node's local runtime parameters.
type Config struct {
	Genesis          Genesis
	ProposeTimeout   time.Duration
	PrevoteTimeout   time.Duration
	PrecommitTimeout time.Duration
	MaxMsgSize       int
	MempoolSize      int
	ListenAddr       string
	Peers            []string
}

// genesisProposer is the well-formed but all-zero address used as the
// Proposer field of the synthetic genesis block, since genesis has no
// actual proposer.
var genesisProposer = crypto.Address(hex.EncodeToString(make([]byte, 20)))

// LoadGenesis reads and parses a genesis JSON file from disk and validates
// it eagerly — a malformed genesis (duplicate validator address, zero or
// negative voting power) is rejected here with a clear error rather than
// surfacing later as a mysterious consensus failure.
func LoadGenesis(path string) (*Genesis, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading genesis file: %w", err)
	}

	var g Genesis
	if err := json.Unmarshal(data, &g); err != nil {
		return nil, fmt.Errorf("config: parsing genesis file: %w", err)
	}

	if _, err := g.ValidatorSet(); err != nil {
		return nil, fmt.Errorf("config: invalid genesis: %w", err)
	}

	return &g, nil
}

// ValidatorSet decodes the genesis validator list into a validator.Set.
// Each entry's address is re-derived from its decoded public key rather
// than trusted as written, and compared against the file's Address field
// — a mismatch means the genesis file is internally inconsistent (e.g.
// hand-edited incorrectly) and is rejected rather than silently accepted.
func (g *Genesis) ValidatorSet() (*validator.Set, error) {
	vals := make([]validator.Validator, len(g.Validators))
	for i, gv := range g.Validators {
		pubBytes, err := hex.DecodeString(gv.PubKey)
		if err != nil {
			return nil, fmt.Errorf("genesis: validator %d: invalid pubkey hex: %w", i, err)
		}
		pub, err := crypto.PubKeyFromBytes(pubBytes)
		if err != nil {
			return nil, fmt.Errorf("genesis: validator %d: %w", i, err)
		}

		derived := pub.Address()
		if gv.Address != "" && crypto.Address(gv.Address) != derived {
			return nil, fmt.Errorf("genesis: validator %d: address %q does not match address %q derived from pubkey",
				i, gv.Address, derived)
		}

		vals[i] = validator.Validator{
			Address:     derived,
			PubKey:      pub,
			VotingPower: gv.VotingPower,
		}
	}
	return validator.NewSet(vals)
}

// Block constructs the deterministic genesis block: height 0, a
// zero-length parent hash, no transactions, and the genesis timestamp
// taken verbatim from the Genesis struct. It never reads the wall clock —
// two nodes loading the same genesis.json at different real-world moments
// must compute the identical genesis hash, or they can never agree on
// anything that follows.
func (g *Genesis) Block() *ledger.Block {
	header := ledger.Header{
		Height:     0,
		Timestamp:  g.Timestamp,
		ParentHash: make([]byte, 32),
		Proposer:   genesisProposer,
	}
	return ledger.NewBlock(header, nil)
}
