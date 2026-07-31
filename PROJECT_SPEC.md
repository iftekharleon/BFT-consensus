# BFT Consensus Engine — Full Project Specification

**Project:** `bft-consensus`
**Language:** Go (1.22+)
**Protocol family:** Tendermint / IBFT-style leader-based Byzantine Fault Tolerant consensus
**Goal:** A working, tested, partially-networked BFT consensus engine that provably survives Byzantine validators, with a fault-injection simulation harness as the centerpiece.

---

## 0. Why This Project

Most "blockchain" portfolio repos stop at a Merkle tree or a Raft implementation. Raft is **crash fault tolerant** — it assumes nodes fail by stopping, never by lying. Real blockchains face **Byzantine** faults: validators that equivocate, send conflicting votes, censor, or collude. Implementing (and testing against) Byzantine faults is the differentiator here.

The three things a reviewer will check, in order:

1. `docs/design.md` — do you understand safety vs. liveness?
2. `sim/scenarios_test.go` — did you actually prove it works under attack?
3. `consensus/state.go` — is the state machine clean or spaghetti?

Everything else is scaffolding. Build with that priority in mind.

---

## 1. Protocol Overview

### 1.1 The Model

- `N` validators total.
- Up to `f` may be Byzantine (arbitrary behavior: lie, equivocate, go silent, collude).
- Safety requires `N >= 3f + 1`. So with `N = 4`, `f = 1`.
- **Quorum threshold** = `2f + 1` = `ceil(2N/3)` voting power.
- Partially synchronous network: messages eventually arrive, but delay is unbounded before GST (Global Stabilization Time).

### 1.2 The Two Properties

| Property | Meaning | How you break it | How you prove it |
|---|---|---|---|
| **Safety** | No two honest validators commit different blocks at the same height. "Nothing bad happens." | Equivocating proposer sends block A to half the set, block B to the other half | Sim scenario: run equivocation, assert all committed blocks at height H are identical |
| **Liveness** | The chain eventually makes progress. "Something good eventually happens." | Silent leader; leader that proposes nothing | Sim scenario: kill leader, assert height advances within N rounds |

Safety is **never** sacrificed. Liveness is sacrificed temporarily (rounds time out and advance) until enough honest nodes are connected.

### 1.3 The Round State Machine

Each **height** (block number) may take multiple **rounds** to commit. A round has three steps:

```
                    ┌──────────────────────────────────────┐
                    │  NewHeight(H)                        │
                    └───────────────┬──────────────────────┘
                                    ▼
                    ┌──────────────────────────────────────┐
      ┌────────────▶│  PROPOSE (round R)                   │
      │             │  proposer = validators[(H+R) % N]    │
      │             │  proposer broadcasts Proposal(block) │
      │             └───────────────┬──────────────────────┘
      │                             ▼
      │             ┌──────────────────────────────────────┐
      │             │  PREVOTE                             │
      │             │  valid proposal? → prevote(blockHash) │
      │             │  invalid/timeout? → prevote(NIL)      │
      │             └───────────────┬──────────────────────┘
      │                             ▼
      │             ┌──────────────────────────────────────┐
      │             │  PRECOMMIT                           │
      │             │  saw 2f+1 prevotes for X?             │
      │             │    → lock on X, precommit(X)          │
      │             │  else → precommit(NIL)                │
      │             └───────────────┬──────────────────────┘
      │                             ▼
      │                   ┌─────────────────┐
      │                   │ 2f+1 precommits │
      │            no ────│   for same X?   │──── yes
      │            │      └─────────────────┘       │
      │            ▼                                ▼
      │   ┌─────────────────┐            ┌─────────────────────┐
      └───│ ROUND CHANGE    │            │  COMMIT block X     │
          │ R = R + 1       │            │  H = H + 1, R = 0   │
          │ timeout *= 1.5  │            └─────────────────────┘
          └─────────────────┘                      │
                    ▲                              │
                    └──────────────────────────────┘
```

### 1.4 Locking — The Safety Mechanism (read this twice)

This is the part everyone gets wrong, and the part a good reviewer probes.

When a validator sees `2f+1` prevotes for block `X` in round `R`, it **locks** on `X`:

```go
lockedBlock = X
lockedRound = R
```

Once locked, in all future rounds, the validator will **only prevote for `X`** — unless it sees a valid Proof-of-Lock-Change (a `2f+1` prevote set for a different block from a round **later** than `lockedRound`).

**Why this matters:** if `2f+1` nodes prevoted for `X` in round 5, then at least `f+1` honest nodes locked on `X`. Any future quorum of `2f+1` must intersect that `f+1` set by at least one honest node — who will refuse to prevote for anything else. So no conflicting block can ever gather `2f+1`. That intersection argument is the entire safety proof. Put it in `docs/design.md` in your own words.

**Unlocking** happens only on seeing a `2f+1` prevote set (a "polka") for a different block at a round `> lockedRound`. This preserves liveness — otherwise a node locked on a block nobody else has could stall forever.

---

## 2. Repository Layout

```
bft-consensus/
├── cmd/
│   └── node/
│       └── main.go              # binary entrypoint, flag parsing, node wiring
├── consensus/
│   ├── types.go                 # Height, Round, Step, Proposal, core enums
│   ├── vote.go                  # Vote struct, signing, verification
│   ├── quorum.go                # vote aggregation, 2f+1 detection, quorum certs
│   ├── state.go                 # THE FSM — main state machine
│   ├── state_test.go
│   ├── timeout.go               # per-step timeout scheduling
│   └── timeout_test.go
├── crypto/
│   ├── keys.go                  # ed25519 keygen / sign / verify / address derivation
│   └── keys_test.go
├── validator/
│   ├── set.go                   # validator set, voting power, proposer selection
│   └── set_test.go
├── ledger/
│   ├── block.go                 # Block struct, hashing, parent linkage, validation
│   ├── block_test.go
│   ├── store.go                 # append-only committed block store
│   └── store_test.go
├── mempool/
│   └── pool.go                  # pending transaction queue
├── p2p/
│   ├── message.go               # wire envelope, encode/decode
│   ├── network.go               # peer management, broadcast, gossip
│   └── network_test.go
├── config/
│   └── genesis.go               # genesis state, chain params, initial validator set
├── sim/
│   ├── harness.go               # in-process N-node cluster runner
│   ├── faults.go                # Byzantine behavior injectors
│   └── scenarios_test.go        # the scenario test suite — the money file
├── docs/
│   ├── design.md                # protocol spec + safety/liveness argument
│   └── diagrams/                # state machine + message flow diagrams
├── .github/workflows/ci.yml     # go vet, go test -race, golangci-lint
├── .gitignore
├── go.mod
├── LICENSE                      # MIT or Apache-2.0
└── README.md
```

**Why multi-package matters:** it forces you to define clean interfaces between layers. A reviewer skimming your repo sees package boundaries and immediately judges whether you think in systems or scripts. A single 2000-line `main.go` is an instant reject signal regardless of correctness.

---

## 3. Component Specifications

Each component below has: **what it does**, **the API surface**, **how to build it**, and **acceptance criteria**.

---

### 3.1 `crypto/keys.go` — Cryptographic Identity

**What it does:** Provides validator identity. Every consensus message is signed; every signature is verified. This is the trust anchor for the whole system.

**API surface:**

```go
type PrivKey ed25519.PrivateKey
type PubKey  ed25519.PublicKey
type Address string   // hex-encoded, truncated hash of pubkey

func GenerateKeyPair() (PrivKey, PubKey, error)
func (p PrivKey) Sign(msg []byte) []byte
func (p PubKey) Verify(msg, sig []byte) bool
func (p PubKey) Address() Address
func PubKeyFromBytes(b []byte) (PubKey, error)
```

**How to build it:**

1. Use stdlib `crypto/ed25519` only. Do not add a crypto dependency. Do not implement primitives yourself — a reviewer with a security background (like the one who will read this) treats hand-rolled crypto as a red flag, not a flex.
2. `Address()` = `hex(sha256(pubkey)[:20])`. Twenty bytes matches Ethereum convention and keeps logs readable.
3. Make `Sign` deterministic — ed25519 already is. No nonce handling needed. That's why ed25519 over ECDSA here; mention that reasoning in `design.md`.
4. Return errors rather than panicking on malformed key bytes. Panics in a networked component are a DoS vector, and you know that better than most.

**Acceptance:**
- Sign → Verify round-trips true.
- Tampered message → Verify false.
- Wrong pubkey → Verify false.
- Malformed key bytes → error, no panic.
- `go test -race ./crypto/...` clean.

---

### 3.2 `validator/set.go` — Validator Set & Proposer Selection

**What it does:** Holds the ordered set of validators with voting power, computes quorum thresholds, and deterministically selects the proposer for any (height, round).

**API surface:**

```go
type Validator struct {
    Address     crypto.Address
    PubKey      crypto.PubKey
    VotingPower int64
}

type Set struct {
    validators []Validator   // sorted by Address for determinism
    totalPower int64
    index      map[crypto.Address]*Validator
}

func NewSet(vals []Validator) (*Set, error)
func (s *Set) Size() int
func (s *Set) TotalPower() int64
func (s *Set) QuorumThreshold() int64        // ceil(2*total/3), the 2f+1 line
func (s *Set) GetByAddress(a crypto.Address) (*Validator, bool)
func (s *Set) Proposer(height uint64, round uint32) Validator
func (s *Set) HasQuorum(power int64) bool
```

**How to build it:**

1. **Sort validators by address on construction.** Every node must derive the identical ordering from genesis, or proposer selection diverges and the chain forks instantly. This is a classic real-world consensus bug — determinism of set ordering.
2. `QuorumThreshold()` = `(2*totalPower)/3 + 1`. Verify by hand: `total=4, f=1` → `(8/3)+1 = 2+1 = 3` = `2f+1`. Correct. Write that arithmetic check as a comment; it prevents a future off-by-one.
3. `Proposer(h, r)` = round-robin: `validators[(h + uint64(r)) % len(validators)]`. Simple, deterministic, and rotates the leader on every round change so a faulty leader can't stall forever. (Production chains use weighted round-robin; note the simplification in design.md and explain the tradeoff — that shows you know what you skipped and why.)
4. Reject on construction: empty set, duplicate addresses, non-positive voting power. Each of those is a live vulnerability class if unchecked.

**Acceptance:**
- Same input set → same ordering, regardless of input order.
- Quorum threshold correct for N=1,4,7,10,100 (table-driven test).
- Proposer rotates across rounds and is identical across independently constructed sets.
- Duplicate address in constructor → error.

---

### 3.3 `consensus/types.go` — Core Types

**What it does:** Defines the vocabulary of the protocol. Every other file in `consensus/` depends on this.

**API surface:**

```go
type Step uint8

const (
    StepNewHeight Step = iota
    StepPropose
    StepPrevote
    StepPrecommit
    StepCommit
)

type Proposal struct {
    Height    uint64
    Round     uint32
    BlockHash []byte
    Block     *ledger.Block
    POLRound  int32          // proof-of-lock round; -1 if none
    Proposer  crypto.Address
    Signature []byte
}

func (p *Proposal) SignBytes() []byte   // canonical serialization for signing
func (p *Proposal) Verify(pub crypto.PubKey) bool
```

**How to build it:**

1. `SignBytes()` must be **canonical** — the same logical proposal must always produce identical bytes on every node. Use explicit big-endian encoding of each field in a fixed order. Do **not** use `encoding/json` for sign bytes: map ordering and whitespace are not guaranteed stable, and that's a signature-malleability bug waiting to happen.
2. Exclude `Signature` itself from `SignBytes()`. Obvious, but it's the single most common mistake in this layer.
3. Include `Height` and `Round` in the signed payload. Omitting them enables cross-round/cross-height replay — the same signature valid in a context it wasn't meant for. You've filed this class of bug before; here you're on the defending side.
4. `POLRound = -1` means "no proof of lock." Use `int32`, not `uint32`, so `-1` is representable without a sentinel hack.

**Acceptance:**
- `SignBytes()` is byte-identical for two independently constructed but logically equal proposals.
- Changing any field changes the sign bytes.
- Signature over a proposal for height H does not verify against the same proposal at height H+1.

---

### 3.4 `consensus/vote.go` — Votes

**What it does:** Represents prevotes and precommits, the atoms of the consensus protocol.

**API surface:**

```go
type VoteType uint8

const (
    VoteTypePrevote VoteType = iota + 1
    VoteTypePrecommit
)

type Vote struct {
    Type      VoteType
    Height    uint64
    Round     uint32
    BlockHash []byte          // nil/empty == NIL vote
    Voter     crypto.Address
    Timestamp int64
    Signature []byte
}

func (v *Vote) SignBytes() []byte
func (v *Vote) Verify(pub crypto.PubKey) bool
func (v *Vote) IsNil() bool
```

**How to build it:**

1. `VoteType` starts at 1, not 0. A zero value should never be a valid vote type — it makes an uninitialized struct fail loudly instead of silently counting as a prevote.
2. **NIL votes are first-class.** A validator that sees no valid proposal must actively vote NIL rather than stay silent. NIL votes count toward the `2f+1` needed to conclude a round and move on. Silence stalls; NIL progresses. This distinction is what makes the protocol live under a faulty leader.
3. `SignBytes()` must include `Type`. Without it, a prevote signature is replayable as a precommit — an instant safety break. Treat this as a required test case, not an afterthought.
4. Timestamp is informational only. Never make consensus decisions based on it — clock skew is not a safe input to a Byzantine-tolerant protocol. Note that explicitly in design.md.

**Acceptance:**
- Prevote signature does not verify as a precommit (test this explicitly).
- NIL vote round-trips through sign/verify.
- Same vote at different rounds produces different sign bytes.

---

### 3.5 `consensus/quorum.go` — Vote Aggregation

**What it does:** Collects votes, tracks power per block hash, detects when a `2f+1` quorum has formed, and — critically — detects equivocation.

**API surface:**

```go
type VoteSet struct {
    height     uint64
    round      uint32
    voteType   VoteType
    valSet     *validator.Set
    votes      map[crypto.Address]*Vote        // one vote per validator
    powerByHash map[string]int64               // hash → accumulated power
    maj23      []byte                          // hash that reached 2f+1, nil if none
}

func NewVoteSet(h uint64, r uint32, t VoteType, vs *validator.Set) *VoteSet
func (vs *VoteSet) AddVote(v *Vote) (added bool, err error)
func (vs *VoteSet) HasTwoThirdsMajority() ([]byte, bool)
func (vs *VoteSet) HasTwoThirdsAny() bool      // 2f+1 total votes, any hash
func (vs *VoteSet) TotalPower() int64

type Evidence struct {
    VoteA, VoteB *Vote     // two conflicting signed votes from same validator
}
var ErrEquivocation = errors.New("validator sent conflicting votes")
```

**How to build it:**

1. `AddVote` validation order matters. Check in this sequence, cheapest and most-rejecting first:
   - height/round/type match this VoteSet → else reject
   - voter is in the validator set → else reject
   - signature valid → else reject
   - not already voted → else check for equivocation

2. **Equivocation detection is a headline feature.** If a validator already has a vote in this set and submits a *different* one for the same (height, round, type), that is cryptographic proof of Byzantine behavior. Return `ErrEquivocation` wrapped with both signed votes as `Evidence`. Two signatures from one key over conflicting statements is undeniable — it can't be forged or explained away. Surfacing this cleanly is the thing that will most obviously read as "written by someone who thinks about attacks."

3. `HasTwoThirdsMajority()` returns the block hash that crossed the threshold. Cache it in `maj23` once found — recomputing on every vote is O(n) churn in a hot path.

4. `HasTwoThirdsAny()` is distinct and necessary: `2f+1` votes total, even if split across different hashes (or NIL). It means "the round has spoken, no single block won" → advance to the next round. Conflating this with `HasTwoThirdsMajority` is a liveness bug.

5. Track power, not vote count. They're equal only when all voting power is 1. Writing it in terms of power from day one means weighted validators work later with no refactor.

**Acceptance:**
- 3 of 4 equal-power validators voting the same hash → majority detected at exactly the third vote, not the second.
- Duplicate identical vote → `added=false`, no error, power unchanged.
- Conflicting vote from same validator → `ErrEquivocation` with both votes retrievable.
- Vote from a non-member → rejected.
- Split votes 2/2 → `HasTwoThirdsAny()` false at N=4 (2 < 3); 3 votes split 2/1 → true, majority false.

---

### 3.6 `consensus/state.go` — The Finite State Machine (core)

**What it does:** Drives the entire protocol. Consumes proposals, votes, and timeouts; emits messages to broadcast and blocks to commit. This is 40% of the project's value and where all the hard bugs live.

**API surface:**

```go
type State struct {
    height       uint64
    round        uint32
    step         Step

    valSet       *validator.Set
    privKey      crypto.PrivKey
    self         crypto.Address

    proposal     *Proposal
    lockedBlock  *ledger.Block
    lockedRound  int32          // -1 = not locked
    validBlock   *ledger.Block
    validRound   int32

    prevotes     map[uint32]*VoteSet   // round → set
    precommits   map[uint32]*VoteSet

    store        *ledger.Store
    mempool      *mempool.Pool
    timeouts     *TimeoutManager

    msgOut       chan Message          // outbound to p2p
    commitOut    chan *ledger.Block    // committed blocks
    mu           sync.Mutex
}

func NewState(cfg Config) *State
func (s *State) Start()
func (s *State) Stop()
func (s *State) HandleProposal(p *Proposal) error
func (s *State) HandleVote(v *Vote) error
func (s *State) HandleTimeout(t Timeout)
func (s *State) Height() uint64
func (s *State) Round() uint32
```

**Internal transitions to implement, in build order:**

```go
func (s *State) enterNewHeight(h uint64)
func (s *State) enterPropose(round uint32)
func (s *State) enterPrevote(round uint32)
func (s *State) enterPrecommit(round uint32)
func (s *State) enterCommit(round uint32)
func (s *State) enterNewRound(round uint32)
```

**How to build it — in this exact order:**

**Step 1 — Skeleton with a single validator (N=1).**
Wire `enterNewHeight → enterPropose → enterPrevote → enterPrecommit → enterCommit → enterNewHeight`. With N=1 you are your own quorum, so it should chain blocks immediately. Get this loop closing before adding anything else. If it doesn't commit height 1, 2, 3 in sequence, stop and fix — nothing downstream works otherwise.

**Step 2 — `enterPropose`.**
```
if s.self != valSet.Proposer(height, round):
    schedule ProposeTimeout; return       // not our turn, just wait
if lockedBlock != nil:
    propose lockedBlock, POLRound = lockedRound    // MUST re-propose the lock
else:
    block = buildBlock(mempool.Reap(), store.Head())
    propose block, POLRound = -1
sign and emit proposal
```
The locked-block re-proposal branch is the one people forget. Skipping it breaks safety under round change. Write the test for it before you write the code.

**Step 3 — `enterPrevote`.**
```
if proposal == nil or proposal invalid:
    prevote NIL
else if lockedBlock != nil and lockedBlock.Hash != proposal.BlockHash:
    if proposal has valid POL at round > lockedRound:
        unlock; prevote proposal.BlockHash
    else:
        prevote NIL                       // stay loyal to the lock
else:
    prevote proposal.BlockHash
```

**Step 4 — `enterPrecommit`.**
```
hash, ok := prevotes[round].HasTwoThirdsMajority()
if !ok:
    precommit NIL                          // no polka, nothing to commit to
else if hash is NIL:
    unlock (lockedBlock = nil, lockedRound = -1)
    precommit NIL
else:
    lockedBlock = block(hash); lockedRound = round
    validBlock  = block(hash); validRound  = round
    precommit hash
```

**Step 5 — `enterCommit`.**
```
hash, ok := precommits[round].HasTwoThirdsMajority()
if !ok or hash is NIL: return              // should not reach here
block = fetch block by hash
store.Append(block)
mempool.Remove(block.Txs)
emit on commitOut
reset: lockedBlock=nil, lockedRound=-1, proposal=nil, round=0
enterNewHeight(height + 1)
```

**Step 6 — `enterNewRound` (round change).**
```
round++
step = StepPropose
timeout duration *= 1.5 (capped)
clear proposal (but NOT lockedBlock — the lock survives round changes)
enterPropose(round)
```

**Non-negotiable rules while building:**

1. **Never move backwards.** Reject any message for `(height, round) < current`. Accept and buffer messages for future rounds — nodes run at different speeds, and dropping ahead-of-time messages costs you liveness.
2. **One lock, held on every public entrypoint.** `HandleProposal`, `HandleVote`, `HandleTimeout` all run concurrently from network and timer goroutines. Take `s.mu` at the top of each, and never call an internal `enterX` without already holding it. Test with `-race` from day one, not at the end.
3. **The lock survives round change and only clears on commit.** Clearing `lockedBlock` in `enterNewRound` is the single most common way people break safety in this implementation. Write a regression test the moment you write the line.
4. **Timeouts always fire an event, never mutate state directly.** They enqueue a `Timeout` handled by the same serialized path as messages. A timer goroutine touching state fields directly is a data race and a correctness bug at once.
5. **Log every transition** as structured output: `height, round, step, action, blockHash[:8]`. When a 4-node sim deadlocks at 3am, these logs are the only way you'll find it. Build the logging in from the start; retrofitting it during a debugging session is miserable.

**Acceptance:**
- N=1 commits a chain of 10 blocks unaided.
- N=4 all honest: commits at round 0 every height.
- N=4 with silent leader: commits at round 1, height still advances.
- Locked node refuses to prevote a conflicting proposal without a valid POL.
- `go test -race ./consensus/...` clean.

---

### 3.7 `consensus/timeout.go` — Timeout Management

**What it does:** Schedules per-step deadlines and delivers expiry events into the FSM. This is the entire liveness mechanism — without timeouts, one silent leader halts the chain forever.

**API surface:**

```go
type Timeout struct {
    Height   uint64
    Round    uint32
    Step     Step
    Duration time.Duration
}

type TimeoutManager struct {
    ticker  *time.Timer
    outCh   chan Timeout
    current Timeout
    mu      sync.Mutex
}

func NewTimeoutManager() *TimeoutManager
func (tm *TimeoutManager) Schedule(t Timeout)
func (tm *TimeoutManager) Chan() <-chan Timeout
func (tm *TimeoutManager) Stop()
```

**How to build it:**

1. Base durations: propose 3s, prevote 1s, precommit 1s. Tune later; make them config fields, not constants buried in code.
2. Backoff: `duration = base + round * delta` (linear) or `base * 1.5^round` (exponential, capped). Growth is mandatory — under network partition, fixed timeouts cause endless round churn where nobody ever has time to converge. Cap it so recovery isn't glacial.
3. **Guard against stale fires.** A timer scheduled for (H=5, R=2, Prevote) may fire after the node already reached (H=5, R=3). The FSM must compare the timeout's (height, round, step) to current state and discard mismatches. Without this guard you get spurious round changes that are agonizing to debug.
4. One active timer at a time. `Schedule` stops the previous one before starting the new one. Leaked timers cause exactly the stale-fire problem above.

**Acceptance:**
- Scheduling a new timeout cancels the previous one.
- Fired timeout carries the exact (H, R, Step) it was scheduled with.
- Backoff increases monotonically with round and respects the cap.
- No goroutine leak after `Stop()` — verify with `goleak` or a simple runtime count.

---

### 3.8 `ledger/block.go` — Block Structure

**What it does:** Defines the unit of commitment and the hash-chain linking blocks into a chain.

**API surface:**

```go
type Header struct {
    Height     uint64
    Timestamp  int64
    ParentHash []byte
    TxRoot     []byte
    Proposer   crypto.Address
}

type Block struct {
    Header Header
    Txs    [][]byte
    hash   []byte     // cached
}

func NewBlock(h Header, txs [][]byte) *Block
func (b *Block) Hash() []byte
func (b *Block) ValidateBasic() error
func (b *Block) ValidateAgainstParent(parent *Block) error
```

**How to build it:**

1. `Hash()` = `sha256(canonical_encode(Header))`. Header only — transactions are bound in via `TxRoot`. Cache the result; it's computed on nearly every message path.
2. `TxRoot` = simple Merkle root over transactions. Empty block → 32 zero bytes, not nil. Nil vs. empty inconsistency across nodes produces different hashes for logically identical blocks, which forks the chain.
3. `ValidateBasic()` — self-contained checks: height > 0, parent hash length correct, proposer address well-formed, tx count under limit.
4. `ValidateAgainstParent()` — contextual checks: `height == parent.height + 1`, `parentHash == parent.Hash()`, `timestamp > parent.timestamp`. Separating stateless from stateful validation is standard client architecture and reads as such.

**Acceptance:**
- Two blocks with identical headers hash identically; any field change alters the hash.
- Empty and nil tx slices produce the same `TxRoot`.
- Height gap or wrong parent hash → validation error.

---

### 3.9 `ledger/store.go` — Block Store

**What it does:** Append-only storage of committed blocks. The canonical chain.

**API surface:**

```go
type Store struct {
    blocks   []*Block
    byHash   map[string]*Block
    mu       sync.RWMutex
}

func NewStore(genesis *Block) *Store
func (s *Store) Append(b *Block) error
func (s *Store) Head() *Block
func (s *Store) Height() uint64
func (s *Store) ByHeight(h uint64) (*Block, bool)
func (s *Store) ByHash(h []byte) (*Block, bool)
```

**How to build it:**

1. In-memory is fine and correct for this project's scope. Keep the interface narrow enough that a BadgerDB or Pebble backend could slot in later, and say so in the README — showing you know where the seam is beats over-engineering it.
2. `Append` **must** reject a block whose height isn't exactly `Head().Height + 1`. A gap means the FSM has a bug; failing loudly here catches it immediately instead of letting the chain silently diverge.
3. `Append` must reject a second block at an existing height. That's a fork attempt, and the store is the last line of defense against it. Return a distinct sentinel error (`ErrConflictingBlock`) so the sim harness can assert on it directly.
4. `RWMutex`, not `Mutex` — reads massively outnumber writes here.

**Acceptance:**
- Sequential appends succeed; a gap errors.
- Appending a different block at an existing height returns `ErrConflictingBlock`.
- Concurrent readers during a write are race-clean under `-race`.

---

### 3.10 `mempool/pool.go` — Transaction Pool

**What it does:** Buffers pending transactions and supplies them to block proposals. Deliberately minimal — this is not the interesting part of the project.

**API surface:**

```go
type Pool struct {
    txs    [][]byte
    seen   map[string]struct{}
    maxTxs int
    mu     sync.Mutex
}

func NewPool(maxTxs int) *Pool
func (p *Pool) Add(tx []byte) error
func (p *Pool) Reap(max int) [][]byte     // peek, does NOT remove
func (p *Pool) Remove(txs [][]byte)       // called on commit
func (p *Pool) Size() int
```

**How to build it:**

1. `Reap` **peeks without removing.** If the round fails and no block commits, those transactions must still be available for the next proposal. Removing on reap silently drops transactions on every round change — a subtle and very real bug class.
2. Deduplicate by `sha256(tx)` in the `seen` map. Reject duplicates on `Add`.
3. Cap the pool size and reject when full. An unbounded mempool is a trivial memory-exhaustion DoS, and leaving it unbounded in a repo you're showing to security-aware reviewers would be a poor look.
4. Resist scope creep: no gas pricing, no nonce ordering, no eviction policy. One paragraph in the README about what a production mempool adds is worth more than a half-built version of it.

**Acceptance:**
- Reap then fail-round then reap again returns the same transactions.
- Duplicate add rejected.
- Add beyond `maxTxs` errors.

---

### 3.11 `p2p/message.go` — Wire Protocol

**What it does:** Wraps consensus messages in a typed envelope for transmission, and decodes them safely on receipt.

**API surface:**

```go
type MsgType uint8

const (
    MsgTypeProposal MsgType = iota + 1
    MsgTypePrevote
    MsgTypePrecommit
)

type Envelope struct {
    Type    MsgType
    From    crypto.Address
    Payload []byte
}

func Encode(e *Envelope) ([]byte, error)
func Decode(b []byte) (*Envelope, error)
```

**How to build it:**

1. Length-prefixed frames over the wire: 4-byte big-endian length, then payload. TCP is a stream, not a message boundary — without framing you will get partial reads and corrupted decodes, and it will look like a consensus bug for hours.
2. **Enforce a maximum message size before allocating.** Read the length prefix, check it against `maxMsgSize`, and reject oversized frames without allocating the buffer. A length-prefix parser that allocates first is a one-packet remote OOM. This is exactly the kind of thing you'd flag in someone else's client.
3. `Decode` must never panic on malformed input. Fuzz it: `go test -fuzz=FuzzDecode` for a few minutes will find the bad paths for you, and having a fuzz target in the repo is a strong signal on its own.
4. JSON or gob is acceptable for the envelope during development. Protobuf is nicer but adds toolchain friction — if you use it, commit the generated code so the repo builds with a plain `go build`.

**Acceptance:**
- Encode → Decode round-trips all three message types.
- Truncated frame → error, no panic.
- Oversized length prefix → rejected before allocation.
- Fuzz target runs clean for 60 seconds.

---

### 3.12 `p2p/network.go` — Peer Networking

**What it does:** Maintains connections to peer validators and broadcasts consensus messages. Intentionally simple: a fully-connected mesh of TCP links, not a DHT.

**API surface:**

```go
type Network struct {
    self     crypto.Address
    listener net.Listener
    peers    map[crypto.Address]*Peer
    inCh     chan *Envelope
    mu       sync.RWMutex
}

func NewNetwork(addr string, self crypto.Address) (*Network, error)
func (n *Network) Start() error
func (n *Network) Dial(addr string, peer crypto.Address) error
func (n *Network) Broadcast(e *Envelope)
func (n *Network) Send(to crypto.Address, e *Envelope)
func (n *Network) Receive() <-chan *Envelope
func (n *Network) Stop()
```

**How to build it:**

1. Fully-connected mesh. At N ≤ 10 validators this is correct and simple; gossip only pays off at larger scale. State the tradeoff in design.md rather than building a DHT you don't need.
2. One reader goroutine and one writer goroutine per peer, with a buffered outbound channel. If the outbound buffer fills, drop the message rather than blocking — a slow or malicious peer must never be able to stall the consensus loop. That backpressure decision is worth a comment; it's a real availability consideration.
3. Non-blocking `Broadcast`: iterate peers, non-blocking send into each peer's channel, drop on full. Consensus tolerates message loss by design; it does not tolerate a blocked main loop.
4. Reconnect with exponential backoff on peer disconnect. Without it, a single transient network blip permanently removes a validator from the set.
5. **The interface matters more than the implementation.** Define `Transport` as an interface and make the TCP version one implementation. The sim harness then supplies an in-memory transport with no real sockets — which makes fault injection trivial and tests fast.

```go
type Transport interface {
    Broadcast(e *Envelope)
    Send(to crypto.Address, e *Envelope)
    Receive() <-chan *Envelope
}
```

**Acceptance:**
- Two nodes connect and exchange messages.
- Killing a peer doesn't crash the survivor.
- Reconnect succeeds after a peer restarts.
- A slow peer does not block broadcast to others (test with a deliberately stalled reader).

---

### 3.13 `config/genesis.go` — Genesis & Configuration

**What it does:** Defines the initial state every node must agree on: the validator set, chain ID, timeout parameters, and genesis block.

**API surface:**

```go
type Genesis struct {
    ChainID    string
    Validators []GenesisValidator
    Timestamp  int64
}

type GenesisValidator struct {
    Address     string   // hex
    PubKey      string   // hex
    VotingPower int64
}

type Config struct {
    Genesis        Genesis
    ProposeTimeout time.Duration
    PrevoteTimeout time.Duration
    PrecommitTimeout time.Duration
    MaxMsgSize     int
    MempoolSize    int
    ListenAddr     string
    Peers          []string
}

func LoadGenesis(path string) (*Genesis, error)
func (g *Genesis) ValidatorSet() (*validator.Set, error)
func (g *Genesis) Block() *ledger.Block
```

**How to build it:**

1. JSON file on disk. Genesis is human-authored configuration, not a hot path — readability wins over efficiency.
2. `ChainID` is included in vote sign bytes. Without it, a signature from a testnet validator is replayable on mainnet. This is a well-known real-world class of bug and worth a line in design.md.
3. `Genesis.Block()` must be **deterministic** — derived purely from genesis fields with a fixed timestamp, never `time.Now()`. Nodes starting at different moments must compute the identical genesis hash or they will never agree on anything.
4. Ship a `genesis.example.json` with 4 validators and a small `scripts/gen-validators.go` helper that produces keys and the matching genesis. Anyone cloning the repo should reach a running 4-node cluster in under two minutes. Onboarding friction is what stops reviewers from actually running your code.

**Acceptance:**
- Genesis loads, produces a validator set matching the file.
- Genesis block hash is identical across repeated loads and separate processes.
- Malformed genesis (duplicate validator, zero power) → clear error.

---

### 3.14 `cmd/node/main.go` — Node Entrypoint

**What it does:** Wires every package into a running node and handles the process lifecycle.

**How to build it:**

1. Flags: `--genesis`, `--key`, `--listen`, `--peers`, `--log-level`.
2. Startup order: load config → load key → build validator set → open store with genesis → start network → dial peers → start FSM.
3. The message pump is the whole main loop:
```go
for {
    select {
    case env := <-net.Receive():
        route to state.HandleProposal / HandleVote
    case t := <-timeouts.Chan():
        state.HandleTimeout(t)
    case msg := <-state.MsgOut():
        net.Broadcast(msg)
    case blk := <-state.CommitOut():
        log.Info("committed", "height", blk.Header.Height, ...)
    case <-ctx.Done():
        graceful shutdown
    }
}
```
4. Handle `SIGINT`/`SIGTERM` with context cancellation and a clean shutdown of network and FSM. Abrupt exit on Ctrl-C is a small thing that reviewers consistently notice.
5. Keep it thin. If `main.go` exceeds ~150 lines, logic has leaked out of the packages where it belongs.

**Acceptance:**
- Four nodes launched from four terminals reach consensus and commit blocks.
- Ctrl-C shuts down cleanly with no panic and no goroutine leak.

---

### 3.15 `sim/harness.go` — Simulation Harness

**What it does:** Spins up N nodes in a single process with an in-memory transport, so scenarios run deterministically in milliseconds instead of requiring real sockets and sleeps.

**API surface:**

```go
type Cluster struct {
    nodes     []*SimNode
    transport *MemTransport
    t         *testing.T
}

func NewCluster(t *testing.T, n int) *Cluster
func (c *Cluster) Start()
func (c *Cluster) Stop()
func (c *Cluster) Node(i int) *SimNode
func (c *Cluster) MakeByzantine(i int, behavior Behavior)
func (c *Cluster) Partition(groupA, groupB []int)
func (c *Cluster) HealPartition()
func (c *Cluster) WaitForHeight(h uint64, timeout time.Duration) error
func (c *Cluster) AssertNoFork()          // the critical safety assertion
func (c *Cluster) SubmitTx(tx []byte)
```

**How to build it:**

1. `MemTransport` routes envelopes between in-process nodes via channels. It owns a partition table and a per-node behavior hook — that's the injection point for every fault scenario.
2. `AssertNoFork()` is the most important function in the repo. For every height every node has committed, assert all committed block hashes are identical. Call it at the end of **every** scenario without exception. This is the automated safety proof, and it's what turns "I wrote consensus" into "I verified consensus."
3. `WaitForHeight` polls with a deadline and fails the test on timeout — that's the liveness assertion. Never use a bare `time.Sleep` to wait for progress; it's flaky and it reads as inexperience.
4. Make it deterministic where possible: seeded randomness, controllable message ordering. A flaky consensus test suite is worse than none, because you'll learn to ignore it.
5. Expose per-node logs tagged with node index. When a scenario fails you'll need to reconstruct the interleaving across four nodes.

**Acceptance:**
- 4-node all-honest cluster reaches height 10 in under a second.
- Partition and heal both take effect immediately.
- `AssertNoFork` fails loudly when handed a deliberately forked state (test your test).

---

### 3.16 `sim/faults.go` — Byzantine Fault Injectors

**What it does:** Implements the attacks. This file is the reason the project is worth building, and it's the file a reviewer with a security background will open first.

**API surface:**

```go
type Behavior interface {
    OnPropose(p *Proposal) []*Proposal     // may duplicate, alter, or drop
    OnVote(v *Vote) []*Vote
    ShouldSend(to crypto.Address, msg any) bool
}

type SilentNode struct{}          // sends nothing
type EquivocatingProposer struct{} // different block to each half of the set
type DoubleVoter struct{}          // conflicting prevotes to different peers
type LaggingNode struct{ Delay time.Duration }
type InvalidProposer struct{}      // malformed / invalid blocks
type CensoringNode struct{}        // drops all txs from proposals
type ReplayNode struct{}           // replays old-height votes
```

**How to build each fault:**

| Fault | Implementation | What it must prove |
|---|---|---|
| **SilentNode** | `ShouldSend` always false | Chain still commits with f silent nodes; liveness via round change |
| **EquivocatingProposer** | `OnPropose` returns two proposals with different blocks, `ShouldSend` routes A to first half, B to second half | **Safety holds** — no fork, `AssertNoFork` passes. The single most important scenario |
| **DoubleVoter** | `OnVote` returns two conflicting prevotes, split by peer | Equivocation detected, `Evidence` produced, no fork |
| **LaggingNode** | Delay all outbound messages | Chain tolerates slow peers; may need extra rounds |
| **InvalidProposer** | Proposes block with bad parent hash / bad height | Honest nodes prevote NIL, round advances, chain recovers |
| **CensoringNode** | Empty tx list in proposals | Liveness of *transactions* degrades but chain progresses — a useful nuance to demonstrate |
| **ReplayNode** | Re-send committed-height votes | Stale messages rejected, no state corruption |

**Rules:**

1. Byzantine behavior belongs entirely in the transport/behavior layer — **never** add `if s.isByzantine` branches inside `consensus/state.go`. Faults must be injectable against the unmodified honest FSM, or the tests prove nothing about the real code path.
2. Each fault must be composable: partition + equivocation simultaneously is a strictly harder and more realistic scenario than either alone.
3. Document each attack's mechanism in a comment. This file doubles as a teaching artifact, and it's the clearest place to demonstrate that you think like an attacker.

**Acceptance:**
- Every behavior type has at least one passing scenario.
- Equivocating proposer with N=4, f=1: no fork, chain still commits.
- All faults injectable without touching consensus package source.

---

### 3.17 `sim/scenarios_test.go` — Scenario Test Suite

**What it does:** The proof. Every claim in your README is backed by a test here.

**Required scenarios:**

```go
func TestHappyPath_4Nodes_10Blocks(t *testing.T)
func TestSilentLeader_RoundChangeRecovery(t *testing.T)
func TestEquivocatingProposer_NoFork(t *testing.T)     // headline test
func TestDoubleVoter_EvidenceDetected(t *testing.T)
func TestNetworkPartition_HaltsThenRecovers(t *testing.T)
func TestMinorityPartition_NoProgress_NoFork(t *testing.T)
func TestInvalidProposal_NilPrevoteRecovery(t *testing.T)
func TestLaggingValidator_StillCommits(t *testing.T)
func TestByzantineExceedsThreshold_SafetyMayBreak(t *testing.T)  // f+1 faulty — document honestly
func TestNodeRestart_RejoinsAndCatchesUp(t *testing.T)
func TestConcurrentTxSubmission_NoDuplicates(t *testing.T)
```

**How to build them:**

1. Every test ends with `c.AssertNoFork()`. No exceptions.
2. `TestByzantineExceedsThreshold` is deliberately included: with `f+1` Byzantine nodes out of 4, safety **can** break — that's the mathematical bound, not a bug. Demonstrating the boundary and documenting it honestly is a stronger signal than pretending the protocol is unconditionally safe. Assert the failure mode rather than asserting success.
3. `TestMinorityPartition_NoProgress_NoFork` captures the essential CAP tradeoff: the minority side must **halt**, not fork. Choosing consistency over availability is the defining property of a BFT chain, and this test is where you show you understand that.
4. Table-driven where the shape repeats. Distinct functions where the scenario logic differs — readability beats cleverness in a file reviewers will actually read.
5. Target the full suite under 30 seconds so CI stays useful.

**Acceptance:**
- All scenarios pass.
- `go test -race ./...` clean.
- Suite completes in under 30s.
- CI runs it on every push.

---

### 3.18 `docs/design.md` — Design Document

**What it does:** This is the first file a reviewer opens and often the only one they read fully. Weight your effort accordingly.

**Required sections:**

1. **Problem statement** — what BFT consensus is and why crash-fault-tolerant protocols like Raft are insufficient for adversarial settings.
2. **System model** — N, f, the 3f+1 bound, partial synchrony assumptions, and what you assume about the network.
3. **Protocol description** — the three steps, round change, and locking, with the state diagram.
4. **Safety argument** — the quorum-intersection proof in your own words. Two `2f+1` quorums out of `3f+1` validators must share at least one honest member; a locked honest validator refuses to prevote conflicting blocks; therefore no two conflicting blocks can both gather `2f+1` precommits. Write this yourself rather than paraphrasing a paper — it's the section that most clearly separates understanding from copying.
5. **Liveness argument** — why round-robin proposer rotation plus timeout backoff guarantees eventual progress after GST.
6. **Attack analysis** — one subsection per fault in `faults.go`: the attack, the defense, and the test that proves it. This section is your differentiator; nobody else's consensus repo has it.
7. **Known limitations** — no persistence, no validator set changes, no light client, no BLS aggregation, fully-connected mesh only. Listing what you deliberately scoped out reads as engineering judgment. Omitting it reads as not knowing.
8. **References** — Tendermint paper, PBFT (Castro & Liskov), HotStuff.

**How to build it:** Write sections 1–3 before writing `state.go`. Writing the spec first is how you avoid discovering in week 3 that your locking rule was wrong. Sections 6–7 get written as you build and find things.

---

### 3.19 `README.md` — The Front Door

Structure, in order:

1. One-sentence description and a badge row (CI status, Go version, license).
2. An asciinema recording or GIF of a 4-node cluster committing blocks. **Do this.** A reviewer with 90 seconds will watch a GIF and will not clone your repo.
3. Feature list with the scenario test that backs each claim.
4. Quickstart: clone → `make run-cluster` → blocks committing. Under two minutes, or people bounce.
5. Architecture diagram and package table.
6. A short section on the Byzantine scenarios covered, with the command to run them.
7. Link to `docs/design.md`.
8. Limitations and a roadmap.

Keep it under 200 lines. A README longer than the design doc inverts the signal.

---

## 4. Build Order & Milestones

| Milestone | Deliverable | Components | Definition of done |
|---|---|---|---|
| **M1** | Foundation | crypto, validator, config, ledger/block | All unit tests green; genesis loads; keys sign/verify |
| **M2** | Types & votes | consensus/types, vote, quorum | Quorum detection correct at N=4,7,10; equivocation detected |
| **M3** | FSM single node | consensus/state (N=1 path), ledger/store, mempool | N=1 node commits 10 blocks unaided |
| **M4** | FSM multi-node | full state.go, timeout.go | 4-node in-memory cluster commits at round 0 |
| **M5** | Liveness | round change, timeout backoff | Silent leader → commits at round 1 |
| **M6** | Networking | p2p/message, p2p/network, cmd/node | 4 real processes over TCP reach consensus |
| **M7** | Byzantine sim | sim/harness, sim/faults | Equivocation scenario passes, no fork |
| **M8** | Full scenarios | sim/scenarios_test | All 11 scenarios green, `-race` clean |
| **M9** | Docs & polish | design.md, README, CI, GIF | Clone-to-running under 2 minutes |

**Milestone M7 is the project.** Everything before it is table stakes that hundreds of repos have. If time runs short, cut networking polish and mempool features before cutting fault scenarios.

---

## 5. Failure Modes to Expect

These will happen. Recognizing them fast saves days.

| Symptom | Likely cause |
|---|---|
| Chain halts at height 1, round grows forever | Not broadcasting your own vote to yourself; you never reach 2f+1 |
| Two nodes commit different blocks | Lock cleared during round change, or locked block not re-proposed |
| Nodes stuck one round apart permanently | Rejecting future-round messages instead of buffering them |
| Random spurious round changes | Stale timeout fires not being filtered by (H, R, Step) |
| `-race` failures under load | Timer goroutine mutating FSM state directly instead of enqueueing an event |
| Works at N=1, deadlocks at N=4 | Quorum threshold off by one — recheck `(2*total)/3 + 1` |
| Non-deterministic block hashes across nodes | JSON used for canonical sign/hash bytes, or `time.Now()` in the genesis block |
| Transactions vanish after a failed round | `Reap` removing from the mempool instead of peeking |

---

## 6. Scope Discipline

**Do not build**, no matter how tempting:

- Persistent storage / database backend
- Validator set changes (this is genuinely hard and a project of its own)
- Light clients or state sync
- A smart contract VM
- BLS signature aggregation
- A gossip/DHT layer
- A REST API or block explorer

Each is a real feature of production chains, and each is a way to spend three weeks not finishing the thing that actually differentiates you. List them in the README under "Out of scope, and why" — that section demonstrates judgment more effectively than a half-finished implementation of any of them.

**Do polish:**

- CI green on every push
- `go vet` and `golangci-lint` clean
- Test coverage above 70% on `consensus/` and `validator/`
- A `Makefile` with `run-cluster`, `test`, `test-race`, `lint`
- Meaningful commit messages showing incremental, thoughtful progress

Commit history is itself a portfolio artifact. Fifty commits reading "wip", "fix", "asdf" undercut otherwise strong code. Commits like `consensus: preserve lock across round change (fixes safety violation in TestEquivocatingProposer)` are worth as much as the code they carry.

---

## 7. Day 1 Checklist

- [ ] `git init`, `go mod init github.com/<user>/bft-consensus`
- [ ] Create all package directories
- [ ] `.gitignore`, `LICENSE`, README stub
- [ ] Implement `crypto/keys.go` — keygen, sign, verify, address
- [ ] Implement `crypto/keys_test.go` — 4 tests including negative cases
- [ ] `go test -race ./crypto/...` green
- [ ] Push to GitHub, public
- [ ] Start `docs/design.md` sections 1–2 (system model) — do this before writing consensus code

---

*End of specification.*
