package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ileon/bft-consensus/crypto"
)

func writeGenesisFile(t *testing.T, g Genesis) string {
	t.Helper()
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal genesis: %v", err)
	}
	path := filepath.Join(t.TempDir(), "genesis.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write genesis file: %v", err)
	}
	return path
}

func validGenesis(t *testing.T, n int) Genesis {
	t.Helper()
	vals := make([]GenesisValidator, n)
	for i := 0; i < n; i++ {
		_, pub, err := crypto.GenerateKeyPair()
		if err != nil {
			t.Fatalf("GenerateKeyPair: %v", err)
		}
		vals[i] = GenesisValidator{
			Address:     string(pub.Address()),
			PubKey:      hex.EncodeToString(pub),
			VotingPower: 1,
		}
	}
	return Genesis{ChainID: "test-chain", Validators: vals, Timestamp: 1700000000}
}

func TestLoadGenesisProducesMatchingValidatorSet(t *testing.T) {
	g := validGenesis(t, 4)
	path := writeGenesisFile(t, g)

	loaded, err := LoadGenesis(path)
	if err != nil {
		t.Fatalf("LoadGenesis: %v", err)
	}

	set, err := loaded.ValidatorSet()
	if err != nil {
		t.Fatalf("ValidatorSet: %v", err)
	}
	if set.Size() != 4 {
		t.Fatalf("expected 4 validators, got %d", set.Size())
	}
	for _, gv := range g.Validators {
		if _, ok := set.GetByAddress(crypto.Address(gv.Address)); !ok {
			t.Fatalf("expected validator %s in set", gv.Address)
		}
	}
}

func TestGenesisBlockHashDeterministicAcrossLoads(t *testing.T) {
	g := validGenesis(t, 4)
	path := writeGenesisFile(t, g)

	loaded1, err := LoadGenesis(path)
	if err != nil {
		t.Fatalf("LoadGenesis: %v", err)
	}
	loaded2, err := LoadGenesis(path)
	if err != nil {
		t.Fatalf("LoadGenesis: %v", err)
	}

	hash1 := loaded1.Block().Hash()
	hash2 := loaded2.Block().Hash()
	if !bytes.Equal(hash1, hash2) {
		t.Fatal("expected genesis block hash to be identical across separate loads")
	}
}

func TestLoadGenesisRejectsDuplicateValidator(t *testing.T) {
	g := validGenesis(t, 1)
	g.Validators = append(g.Validators, g.Validators[0])
	path := writeGenesisFile(t, g)

	if _, err := LoadGenesis(path); err == nil {
		t.Fatal("expected error for duplicate validator address")
	}
}

func TestLoadGenesisRejectsZeroPower(t *testing.T) {
	g := validGenesis(t, 1)
	g.Validators[0].VotingPower = 0
	path := writeGenesisFile(t, g)

	if _, err := LoadGenesis(path); err == nil {
		t.Fatal("expected error for zero voting power")
	}
}

func TestExampleGenesisFileLoads(t *testing.T) {
	path := filepath.Join("..", "genesis.example.json")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("genesis.example.json not present: %v", err)
	}
	g, err := LoadGenesis(path)
	if err != nil {
		t.Fatalf("LoadGenesis(genesis.example.json): %v", err)
	}
	if len(g.Validators) != 4 {
		t.Fatalf("expected 4 validators in example genesis, got %d", len(g.Validators))
	}
}
