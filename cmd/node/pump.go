package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"

	"github.com/ileon/bft-consensus/consensus"
	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/p2p"
)

// pump is the node's entire main loop: route inbound network envelopes
// into the FSM, feed fired timeouts back into it, broadcast whatever it
// wants to say, and log commits — until the context is cancelled.
func pump(ctx context.Context, net *p2p.Network, st *consensus.State, self crypto.Address) {
	for {
		select {
		case env := <-net.Receive():
			routeEnvelope(st, env)
		case t := <-st.TimeoutChan():
			st.HandleTimeout(t)
		case msg := <-st.MsgOut():
			broadcastMessage(net, self, msg)
		case blk := <-st.CommitOut():
			slog.Info("committed", "height", blk.Header.Height, "hash", hex.EncodeToString(blk.Hash()))
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		}
	}
}

func routeEnvelope(st *consensus.State, env *p2p.Envelope) {
	switch env.Type {
	case p2p.MsgTypeProposal:
		var p consensus.Proposal
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			slog.Warn("dropping malformed proposal envelope", "from", env.From, "err", err)
			return
		}
		if err := st.HandleProposal(&p); err != nil {
			slog.Debug("HandleProposal", "from", env.From, "err", err)
		}
	case p2p.MsgTypePrevote, p2p.MsgTypePrecommit:
		var v consensus.Vote
		if err := json.Unmarshal(env.Payload, &v); err != nil {
			slog.Warn("dropping malformed vote envelope", "from", env.From, "err", err)
			return
		}
		if err := st.HandleVote(&v); err != nil {
			slog.Debug("HandleVote", "from", env.From, "err", err)
		}
	}
}

func broadcastMessage(net *p2p.Network, self crypto.Address, msg consensus.Message) {
	var msgType p2p.MsgType
	var payload []byte
	var err error

	switch {
	case msg.Proposal != nil:
		msgType = p2p.MsgTypeProposal
		payload, err = json.Marshal(msg.Proposal)
	case msg.Vote != nil:
		if msg.Vote.Type == consensus.VoteTypePrecommit {
			msgType = p2p.MsgTypePrecommit
		} else {
			msgType = p2p.MsgTypePrevote
		}
		payload, err = json.Marshal(msg.Vote)
	default:
		return
	}
	if err != nil {
		slog.Warn("failed to encode outbound message", "err", err)
		return
	}
	net.Broadcast(&p2p.Envelope{Type: msgType, From: self, Payload: payload})
}
