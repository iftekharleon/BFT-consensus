# Design: bft-consensus

## 1. Problem Statement

Distributed systems that tolerate faults generally assume one of two fault models.

**Crash faults** (the Raft/Paxos model): a node either behaves correctly or stops
responding entirely. It never sends a message it shouldn't, never sends two
different messages claiming to be the same thing, and never lies about what it
has seen. Under this assumption, `N = 2f + 1` nodes tolerate `f` failures, and the
protocol only has to reason about *silence*, not *deception*.

That assumption is false for a blockchain. A validator in a permissionless or
semi-trusted network is an economic actor, not a well-behaved server. It can be
compromised, bribed, buggy, or simply malicious, and it can do far more than go
silent: it can **equivocate** (send block A to half the network and block B to
the other half, hoping to split the vote and get both committed on different
sides of a fork), send conflicting votes, censor specific transactions, replay
stale signed messages, or selectively delay messages to specific peers. None of
that is "crashing" — it's an adversary with a valid private key operating
inside the protocol's own message format.

Raft's safety proof does not survive contact with a single such node, because
Raft's leader election and log replication logic implicitly trusts every signed
message it receives at face value. A **Byzantine Fault Tolerant (BFT)** protocol
makes no such assumption: it is designed so that safety holds even when up to
`f` validators are arbitrarily malicious, coordinated, and running attacker-
controlled code — not just crashed.

This project implements a Tendermint/IBFT-style, leader-based, partially
synchronous BFT consensus engine, and — critically — a fault-injection
simulation harness that actually drives the protocol through equivocation,
double-voting, censorship, network partition, and leader silence, and proves
(via automated assertions, not manual inspection) that safety holds throughout
and that liveness recovers once conditions allow it. The engine is the
mechanism; the simulation harness is the proof that the mechanism does what it
claims.

## 2. System Model

- **Validator set.** `N` validators total, each with a public/private ed25519
  keypair and a voting power. The set is fixed for the lifetime of the chain
  (no validator set changes — see §7 Known Limitations).
- **Fault bound.** Up to `f` validators may be Byzantine — arbitrary behavior,
  including full collusion with each other. Safety requires `N >= 3f + 1`. This
  project targets `N = 4, f = 1` as the primary test configuration, since 4 is
  the smallest validator count for which `f >= 1` is possible at all, and is
  therefore the smallest set that can actually exercise Byzantine behavior.
- **Quorum threshold.** `2f + 1` = `ceil(2N/3)` voting power. This is the
  smallest majority such that any two quorums, drawn from `N = 3f + 1`
  validators, are guaranteed to intersect in at least one honest validator (see
  §4, Safety Argument, once locking is introduced in §3).
- **Network model: partial synchrony.** The network is not synchronous — there
  is no fixed, known upper bound on message delay that the protocol can rely on
  for correctness. Instead, the model assumes a **Global Stabilization Time
  (GST)**: before GST, the network may delay, reorder, or drop messages
  arbitrarily (bounded only by "messages are not lost forever" for honest-to-
  honest links); after GST, message delay becomes bounded, though the bound and
  even the time GST occurs are unknown to the protocol in advance.
- **What this buys, and what it costs.** Because the protocol never assumes a
  known message-delay bound, it cannot use fixed timeouts to safely conclude
  "that validator is definitely not going to respond." It can only use
  timeouts to make *progress* (move to the next round), never to make a
  *safety* decision (commit a block). This is precisely why the protocol
  splits into two independent properties — safety, which must hold even during
  the asynchronous period before GST, and liveness, which is only guaranteed
  to resume after GST. §1.2 of the project spec and §3 below develop this
  distinction in full; the short version is: **safety is unconditional,
  liveness is conditional on eventual synchrony.**
- **Cryptographic assumptions.** ed25519 signatures are unforgeable without the
  private key, and every consensus message (`Proposal`, `Vote`) is signed and
  independently verified by every recipient. A validator's only unforgeable
  "speech act" is a signed message; the harness's equivocation detection
  (§4, §6) depends entirely on this — two conflicting signed statements from
  the same key are undeniable proof of Byzantine behavior, not something that
  can be explained away as a bug or a race.
- **What the model explicitly does not assume:** synchronized clocks (vote
  timestamps are informational only — see `consensus/vote.go` and §7), a
  trusted third party, or that the network topology is anything more than
  "messages between honest nodes eventually arrive." A fully-connected TCP mesh
  is used for simplicity at the scale this project targets (`N <= 10`); this is
  a deployment simplification, not a protocol requirement — see §7.

## 3. Protocol Description

*(To be completed in M2, alongside `consensus/types.go`, `vote.go`, and
`quorum.go` — the round state machine, the three per-round steps, round
change, and the locking rule, with the state diagram from the project spec
included and referenced against the actual Go types once they exist.)*

## 4. Safety Argument

*(To be completed in M9. Quorum-intersection proof in the author's own words.)*

## 5. Liveness Argument

*(To be completed in M9.)*

## 6. Attack Analysis

*(To be completed in M9, alongside `sim/faults.go` — one subsection per fault
type: the attack, the defense, and the test that proves it.)*

## 7. Known Limitations

*(To be completed in M9. Deliberately out of scope: persistent storage,
validator set changes, light clients / state sync, a smart contract VM, BLS
signature aggregation, a gossip/DHT layer, a REST API or block explorer. Each
is a real production feature and a real distraction from what this project is
actually proving.)*

## 8. References

*(To be completed in M9: Tendermint consensus paper, PBFT — Castro & Liskov,
HotStuff.)*
