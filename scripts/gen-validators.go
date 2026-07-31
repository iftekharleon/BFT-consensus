// Command gen-validators generates a demo validator set: N ed25519
// keypairs, a matching genesis.json, and one private-key file per
// validator so a local cluster can be launched immediately.
//
// The generated keys are demo/test keys only, meant to get a cloned repo
// to a running local cluster in under two minutes (see PROJECT_SPEC.md
// §3.13 point 4). They are deliberately checked in alongside
// genesis.example.json for that reason — never reuse them for anything
// beyond local experimentation.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ileon/bft-consensus/config"
	"github.com/ileon/bft-consensus/crypto"
)

func main() {
	n := flag.Int("n", 4, "number of validators to generate")
	genesisOut := flag.String("genesis-out", "genesis.example.json", "path to write the genesis file")
	keyDir := flag.String("key-dir", "example-keys", "directory to write private key files")
	chainID := flag.String("chain-id", "bft-consensus-example", "chain ID to embed in the genesis file")
	flag.Parse()

	if err := run(*n, *genesisOut, *keyDir, *chainID); err != nil {
		fmt.Fprintln(os.Stderr, "gen-validators:", err)
		os.Exit(1)
	}
}

func run(n int, genesisOut, keyDir, chainID string) error {
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		return fmt.Errorf("creating key directory: %w", err)
	}

	gen := config.Genesis{
		ChainID:    chainID,
		Timestamp:  time.Now().Unix(),
		Validators: make([]config.GenesisValidator, n),
	}

	for i := 0; i < n; i++ {
		priv, pub, err := crypto.GenerateKeyPair()
		if err != nil {
			return fmt.Errorf("generating keypair %d: %w", i, err)
		}

		gen.Validators[i] = config.GenesisValidator{
			Address:     string(pub.Address()),
			PubKey:      hex.EncodeToString(pub),
			VotingPower: 1,
		}

		keyPath := filepath.Join(keyDir, fmt.Sprintf("val%d.key", i))
		if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(priv)), 0o600); err != nil {
			return fmt.Errorf("writing key file %d: %w", i, err)
		}
	}

	data, err := json.MarshalIndent(gen, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling genesis: %w", err)
	}
	data = append(data, '\n')

	if err := os.WriteFile(genesisOut, data, 0o644); err != nil {
		return fmt.Errorf("writing genesis file: %w", err)
	}

	fmt.Printf("wrote %s with %d validators, keys in %s/\n", genesisOut, n, keyDir)
	return nil
}
