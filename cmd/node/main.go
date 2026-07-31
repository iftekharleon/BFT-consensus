// Command node is the bft-consensus validator binary: it wires config,
// crypto, the validator set, the ledger, the mempool, consensus, and p2p
// together and runs the message pump until interrupted. See
// PROJECT_SPEC.md §3.14.
//
// Peer addressing convention: --peers is a comma-separated list of
// "host:port" entries, one per *other* validator in genesis.json, in the
// same order they appear in genesis.Validators (skipping this node's own
// entry). Genesis carries validator identity, not network location —
// this project doesn't implement peer discovery, so the two lists must
// be given in matching order.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ileon/bft-consensus/config"
	"github.com/ileon/bft-consensus/consensus"
	"github.com/ileon/bft-consensus/ledger"
	"github.com/ileon/bft-consensus/mempool"
	"github.com/ileon/bft-consensus/p2p"
)

func main() {
	genesisPath := flag.String("genesis", "genesis.example.json", "path to genesis JSON file")
	keyPath := flag.String("key", "", "path to hex-encoded ed25519 private key file (required)")
	listenAddr := flag.String("listen", "127.0.0.1:26656", "address to listen for peer connections on")
	peersFlag := flag.String("peers", "", "comma-separated host:port list of other validators, in genesis order excluding self")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	setLogLevel(*logLevel)

	if *keyPath == "" {
		slog.Error("--key is required")
		os.Exit(1)
	}
	if err := run(*genesisPath, *keyPath, *listenAddr, *peersFlag); err != nil {
		slog.Error("node exited with error", "err", err)
		os.Exit(1)
	}
}

func run(genesisPath, keyPath, listenAddr, peersFlag string) error {
	genesis, err := config.LoadGenesis(genesisPath)
	if err != nil {
		return fmt.Errorf("loading genesis: %w", err)
	}

	priv, err := loadPrivKey(keyPath)
	if err != nil {
		return fmt.Errorf("loading key: %w", err)
	}
	self := priv.Public().Address()

	valSet, err := genesis.ValidatorSet()
	if err != nil {
		return fmt.Errorf("building validator set: %w", err)
	}
	if _, ok := valSet.GetByAddress(self); !ok {
		return fmt.Errorf("this node's key (address %s) is not in the genesis validator set", self)
	}

	store := ledger.NewStore(genesis.Block())
	pool := mempool.NewPool(10_000)

	net, err := p2p.NewNetwork(listenAddr, self)
	if err != nil {
		return fmt.Errorf("creating network: %w", err)
	}
	if err := net.Start(); err != nil {
		return fmt.Errorf("starting network: %w", err)
	}
	defer net.Stop()
	dialPeers(net, genesis, self, peersFlag)

	st := consensus.NewState(consensus.Config{
		ValSet: valSet, PrivKey: priv, Self: self, Store: store, Mempool: pool,
		ChainID:          genesis.ChainID,
		ProposeTimeout:   3 * time.Second,
		PrevoteTimeout:   1 * time.Second,
		PrecommitTimeout: 1 * time.Second,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go st.Start()
	defer st.Stop()

	pump(ctx, net, st, self)
	slog.Info("shutdown complete")
	return nil
}
