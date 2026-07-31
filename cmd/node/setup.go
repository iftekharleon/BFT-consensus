package main

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ileon/bft-consensus/config"
	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/p2p"
)

func loadPrivKey(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	keyBytes, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("decoding hex key: %w", err)
	}
	return crypto.PrivKey(keyBytes), nil
}

// initialDialTimeout bounds how long dialPeers waits for the first
// connection attempt to every configured peer before letting consensus
// start anyway. Starting the FSM before peer connections are up is a
// real startup race: an early round's proposal can be sent before a
// slow-to-connect peer is attached, and once that peer falls behind
// there is no block-sync mechanism to catch it up again (out of scope —
// see docs/design.md §7) — its only recovery is a later round actually
// reaching it. Waiting here doesn't eliminate the race (a peer can
// still take longer than this to connect, or drop and reconnect later),
// but it closes the most common and entirely avoidable case: every
// process starting at once and racing to dial each other.
const initialDialTimeout = 5 * time.Second

// dialPeers dials each configured peer, blocking up to
// initialDialTimeout for the first attempt at each to succeed before
// returning; any that don't connect in time keep retrying indefinitely
// in the background (a peer that's still down, or restarts later,
// should not prevent this node from running).
func dialPeers(net *p2p.Network, genesis *config.Genesis, self crypto.Address, peersFlag string) {
	if peersFlag == "" {
		return
	}
	addrs := strings.Split(peersFlag, ",")

	var others []string
	for _, v := range genesis.Validators {
		if v.Address != string(self) {
			others = append(others, v.Address)
		}
	}
	if len(addrs) != len(others) {
		slog.Error("--peers count does not match genesis validator count (excluding self)",
			"peers", len(addrs), "expected", len(others))
		return
	}

	var wg sync.WaitGroup
	for i, addr := range addrs {
		addr := strings.TrimSpace(addr)
		peerAddr := crypto.Address(others[i])
		connected := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-connected:
			case <-time.After(initialDialTimeout):
				slog.Warn("initial connect timed out, continuing to retry in background", "addr", addr, "peer", peerAddr)
			}
		}()

		go func() {
			for {
				if err := net.Dial(addr, peerAddr); err != nil {
					slog.Warn("dial failed, retrying", "addr", addr, "peer", peerAddr, "err", err)
					time.Sleep(time.Second)
					continue
				}
				close(connected)
				return
			}
		}()
	}
	wg.Wait()
}

func setLogLevel(level string) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetLogLoggerLevel(lvl)
}
