# Design: failed-seal recovery (bead `shepherd-runtime-go-mtf`)

Status: **runtime implemented** (bead `shepherd-runtime-go-mtf`); yaah
integration still pending (`yaah-9nqf`). Cross-repo tracker: `yaah-9nqf`.

Decisions resolved (see §10): detached finalization by default; the artifact
guarantees **both** workspace state and conversation; worktree lane keeps the
physical world, container lane seeds from captured state.

## 1. Problem

A retained dispatch seals its work after the body runs. Today the seal is a
single all-or-nothing function (`settlement.go:359-518`): capture, diff, blob
writes, then one sealed fact. Any failure after the body — an expired attempt
deadline, a canceled context, a blob-store or append error — returns `nil` and
deletes the blobs it wrote. Nothing durable names the attempt's work.

The host then has no handle: `ScopeManager.Seal` registers an output only on
success (`scope_manager.go:342-353`), yaah's proposal registry is populated
only after a non-nil output (`retained.go:1201-1222`), and `RecoverScopes`
rebuilds outputs only from `goatherd.run_output.sealed.v1` facts
(`recovery.go:593-680`). The conversation is persisted *only* as the seal's
snapshot (`retained_proposal_meta.go:3-7`), so it dies too. yaah's error path
then calls `parent.Discard(scope)` (`retained.go:1178-1192`), which destroys
the worktree (`scope.go:544-549`, `:683-696`). Net result: ~10 minutes of work
and its conversation are unrecoverable.

The trigger in the observed incident is the 600 s attempt deadline
(`internal/dispatch/api.go:262`), which flows unchanged into the seal at
`retained.go:1155`. But the deadline is only one way to fail at this boundary;
the design must cover the whole class.

v0.25.0 added `SealOptions.SealContext` — a live finalization context — but it
is opt-in and **no production caller sets it** (only `settlement.go`, its test,
and a parity doc reference it). That is a symptom of the real problem:
finalization is governed by the same context as the attempt that just expired.
This design makes finalization a distinct, durable phase rather than a flag.

## 2. Principles

1. **Handle before payload.** The durable recovery handle is written before
   any step that can fail; everything afterwards is best-effort refinement.
   Failure never removes the handle.
2. **Finalization is a separate phase with its own bounded budget.** The
   attempt deadline governs the body. Once finalization begins, durable work
   runs under a context detached from the attempt's cancellation, with a short
   independent deadline. This is structural (the default), not opt-in.
3. **The conversation is durable before finalization, not inside it.** The
   transcript is written by its own idempotent API at the body/seal boundary
   under a detached budget. No seal step can erase it, and every surfaced
   recovery handle carries a digest-verified transcript.
4. **Append-only correction.** A failed seal is a fact, not an update. A later
   successful seal cites the same artifact id; the trace keeps both.
5. **Never silently restore.** Recovery re-adopts a worktree only through the
   host-supplied `Resolver` (`recovery.go:26-42`), and marks an artifact stale
   when its recorded state digest no longer matches the on-disk state — the
   same rule checkpoints use (`recovery.go:546`).
6. **Host owns lifecycle policy.** The runtime retains the artifact and leaves
   the scope alone; yaah decides when to discard a failed-seal handle. The
   runtime never auto-discards.
7. **Capability-preserving seed.** The artifact carries enough to create a new
   isolated child (`ForkFromFailedSeal`, mirroring `ForkFromOutput` at
   `scope_manager.go:132-156`): workspace, baseline, diff, changes, transcript,
   metadata, lineage.

## 3. New durable records

Five schema refs, all append-only on the sealing scope's owner path:

| Schema ref | Role | When |
|---|---|---|
| `goatherd.attempt.transcript.v1` | The **conversation**, written by `PersistAttemptTranscript`. Idempotent on `attempt_id`. | At the body/seal boundary, **before** finalization. |
| `goatherd.seal.artifact.v1` | **Tier 1 — the handle.** Inline, bounded, no blob references. Cites `transcript_id`/digest. | First step of finalization, before capture/diff/blob work. |
| `goatherd.seal.values.v1` | **Tier 2 — the payload.** Diff and metadata: inline if ≤ `sealInlineLimit`, otherwise named-blob digests. Cites `artifact_id`. | After each value is durably written. |
| `goatherd.seal.failed.v1` | Terminal failure event. Cites `artifact_id`, names `stage` and `reason`, and lists which Tier-2 values landed. | On any failure in finalization. |
| `goatherd.seal.artifact.discarded.v1` | Host's explicit terminal for work it will not recover. | `DiscardFailedSeal`. |

The existing `goatherd.run_output.sealed.v1` fact gains one field,
`artifact_id`, as a causal parent. It is the success terminal for the same
artifact.

**Transcript fact (`goatherd.attempt.transcript.v1`).** Written by
`mgr.PersistAttemptTranscript(ctx, attemptID, scopeID, data)`:
- payload: `attempt_id`, `scope`, `size`, `sha256`, and either inline bytes
  (≤ `sealInlineLimit`) or `blob_kind`/`blob_key` for the named-blob store;
- `AppendIntentID` is derived from `attempt_id`, so a retry returns the prior
  receipt instead of writing a second copy (kernel idempotent intent replay);
- the write runs under `finalizationContext(ctx, TranscriptBudget)` — detached
  from the attempt deadline, with bounded retries — because this is the
  durability guarantee, not a best-effort sidecar;
- if this write fails after retries, **no seal is attempted**: a
  `goatherd.seal.failed.v1` record with stage `persist_transcript` is written
  when the store is reachable, and the host receives a distinct
  `SealError{Stage: "persist_transcript"}` rather than a partial handle.

**Tier 1 artifact.** Inline only, so it cannot fail for size reasons:
`artifact_id` (owner + nextCheckpointSeq), `attempt_id`, `transcript_id`,
`transcript_digest`, `scope`, `owner`, `fork_point`, `created_at_unixnano`,
full `state` and `baseline` `WorkspaceState` maps (backend, revision, `data`,
digest — as the sealed fact already does at `settlement.go:429-452`), `changes`
(path/kind), `derived_from`, `supersedes`, and finalization metadata (mode,
timeout, deadline-exceeded flag).

**Tier 2 values.** Diff and opaque metadata are written in one or more
`goatherd.seal.values.v1` records citing `artifact_id`, so a partial diff write
does not lose an already-persisted metadata value. The conversation is *not*
written here — it already has its own fact. Missing values are reported by
name; recovery never guesses them (`recoverSealValues`, `recovery.go:811-854`).

**Pairing rules for recovery**, keyed by `artifact_id`, with the transcript
fact resolved by `transcript_id` and verified against `transcript_digest`:

- sealed fact with matching `artifact_id` → recovered and settled (`resolved`);
- `seal.failed.v1` → failed handle, surfaced;
- `seal.artifact.discarded.v1` → not surfaced;
- neither → **interrupted** (crash between prepare and terminal record),
  surfaced as a failed handle with reason `interrupted`;
- transcript missing, corrupt, or digest mismatch → handle is marked
  `TranscriptInvalid` and `ForkFromFailedSeal` refuses it. Handles are only
  surfaced as recoverable when workspace **and** transcript are both intact,
  which is what "both guaranteed" means as a testable contract.

## 4. Revised `Seal` flow

```
host (body just ended)
  ├─ attemptID = envelope id
  └─ mgr.PersistAttemptTranscript(ctx, attemptID, scopeID, transcript)
        detached budget + bounded retry; idempotent → transcript_id
        failure → SealError{Stage:"persist_transcript"}; NO seal, NO handle

Seal(ctx, opts)                        // opts.TranscriptID set by host
  │
  ├─ finalizeCtx = finalize(ctx, opts.Finalization)
  │     default: WithoutCancel(ctx) + FinalizationTimeout (30s)
  │     override: host supplies a live Context and/or Mode=Inherit
  ├─ PREFLIGHT: finalizeCtx.Err() → typed error, no work attempted
  ├─ write Tier-1 artifact fact (cites transcript_id)   ← handle exists
  ├─ capture / annotate delta base / diff (finalizeCtx)
  │     failure → failed record, return SealError{ArtifactID,...}
  ├─ write diff + metadata values (finalizeCtx)
  │     each success → append values record citing artifact_id
  │     failure → failed record naming stage + values present
  ├─ append run_output.sealed.v1 with artifact_id (finalizeCtx)
  │     failure → failed record, NO sealed fact
  └─ return RetainedOutput
```

Changes to the existing implementation:

- `cleanup()` (`settlement.go:469-476`) currently deletes every written blob on
  failure. Artifact and transcript blobs are **excluded**. They are deleted
  only by `DiscardFailedSeal` (or host retention) under a bounded detached
  context. Keep warning on failed deletion; never let cleanup erase the
  recovery handle or the conversation.
- Transcript bytes are not duplicated into the sealed fact's `snapshot` when
  `opts.TranscriptID` is set; the sealed payload records `transcript_id` and
  its digest instead. `RetainedOutput.Snapshot()` on recovery is populated
  from the transcript fact, preserving the existing accessor contract.
  Callers that do not use the new API keep the old inline snapshot path.
- `SealContext` (v0.25.0) becomes a compatibility alias for
  `Finalization.Context`; deprecate it, do not remove it in the same release.
- Typed errors so callers can branch without string matching:
  ```go
  var ErrSealFinalization      = errors.New("goatherd: seal finalization failed")
  var ErrSealDeadlineBeforePersist = errors.New("goatherd: finalization deadline expired before persist")
  var ErrSealTranscriptInvalid = errors.New("goatherd: failed-seal transcript missing or corrupt")

  type SealError struct {
      Stage      string // "persist_transcript", "artifact", "capture", "diff",
                        // "persist_diff", "persist_metadata", "sealed_append"
      ArtifactID string // empty only if Tier 1 was never written
      Err        error
  }
  func (e *SealError) Unwrap() error { return e.Err }
  ```
  A pre-flight `finalizeCtx.Err()` check before every durable write produces
  `ErrSealDeadlineBeforePersist` wrapped in `SealError` — the bead's item 4 —
  so no `BeginTx` is attempted on a dead context and the error names the
  artifact the host can recover from. (A kernel-level `ctx.Err()` check in
  `PutBlob`/`Append` is a separate, optional follow-up bead.)

**Finalization semantics are a deliberate behavior change.** Cancellation of
`ctx` stops the body and gates entry into finalization, but once finalization
starts it completes within its own budget even if the attempt deadline fires
mid-seal. `FinalizationMode=Inherit` restores strict cancellation for hosts
that need a hard abort. Rationale: losing finished work is worse than a short
extra write window, and the current default is what caused the incident.

## 5. Public API additions

```go
// Written at the body/seal boundary; idempotent on attemptID, detached budget.
func (m *ScopeManager) PersistAttemptTranscript(
    ctx context.Context, attemptID, scopeID string, data []byte,
) (transcriptID string, err error)

// Handle rebuilt by RecoverScopes (or returned on a failed Seal).
type FailedSeal struct {
    ID, ArtifactID, ScopeID, OwnerID, AttemptID string
    Workspace, Baseline trace.WorkspaceState
    Changes   []ProposedChange
    Diff      string
    Metadata  []byte
    Transcript        []byte   // digest-verified; always present on a surfaced handle
    TranscriptID      string
    TranscriptInvalid bool     // set when verification fails; refuses Fork
    Values    []string         // Tier-2 values present: diff, metadata
    Stage     string           // failure stage; "interrupted" on crash
    Reason    string
    Adopted   bool             // Resolver re-adopted the worktree
    Stale     bool             // state digest drifted; Fork refuses
}

func (m *ScopeManager) FailedSeals() []*FailedSeal
func (m *ScopeManager) FailedSeal(id string) (*FailedSeal, bool)

// Records the host's terminal decision and deletes artifact blobs.
// Does NOT destroy the sandbox — that remains the host's call.
func (m *ScopeManager) DiscardFailedSeal(ctx context.Context, id string) error

// Seeds a fresh isolated child from a failed-seal artifact.
// Baseline = h.Baseline, Seed = h.Workspace, Snapshot = h.Transcript,
// DerivedFrom = h.ArtifactID. Refuses stale, unadopted, or
// transcript-invalid handles.
func (m *ScopeManager) ForkFromFailedSeal(
    ctx context.Context, parentID, childOwnerID string,
    h *FailedSeal, sb sandbox.Sandbox,
) (*Scope, error)
```

`RecoverScopes` additions: scan the five new schema refs, pair by
`artifact_id`, resolve the transcript by `transcript_id` and verify its digest,
resolve worktrees through the existing `Resolver`, compute staleness, and
populate `mgr.failedSeals`. `RecoveryStats` gains `TranscriptFacts`,
`SealArtifacts`, `FailedSeals`, `InterruptedSeals`, `DiscardedSeals`. No scope
is auto-discarded and no sandbox is auto-created.

## 6. yaah integration (`yaah-9nqf`)

- At the body/seal boundary (`retained.go:1034-1055`), after the transcript
  sink has been filled and before `proposalSealOptions`, call
  `mgr.PersistAttemptTranscript(ctx, id, scopeID, json(transcript))` with the
  envelope id as `attemptID`. A `persist_transcript` error is surfaced as its
  own envelope status — the one case where no recoverable handle exists.
- `proposalSealOptions` (`retained_proposal_meta.go:55-87`) sets
  `opts.TranscriptID` (and keeps `opts.Metadata`). It stops embedding the
  transcript as `Snapshot`, so the 8 MiB transcript bound is enforced once,
  by the journal, and the bytes are never written twice.
- On `output == nil` (`retained.go:1178-1192`), stop calling
  `parent.Discard(scope)`. Pull `SealError.ArtifactID`, query
  `mgr.FailedSeal`, and emit an envelope with `status: "seal_failed"`,
  `next_verbs: continue|recover|discard`, and the artifact id. Keep
  `recordFailedReviewDecision`; it now cites the artifact as well as the
  proposal fact.
- `LoadProposalRegistry` (`proposal_rehydrate.go:31-101`) includes recovered
  failed-seal handles as proposals with status `seal_failed`.
  `PendingProposalScopeIDs` (`retained.go:2165-2170`) must include them so the
  scope reaper spares their worktrees however long they wait.
- `proposal continue`/`recover` (`retained.go:1378-1396`, `:1430-1444`) map to
  `ForkFromFailedSeal`, seeding the transcript from `Transcript()` and the
  world from `Workspace()`/`Baseline()`. `nextVerbsFor` (`retained.go:288-296`)
  advertises these verbs for `seal_failed` envelopes.
- Failure classification (`failure_class.go:160-174`, `envelope.go:251-266`):
  `seal_failed` becomes a distinct "retryable via continue/fork, never
  auto-redispatch" class — the current premise that sealed partial work
  always survives becomes explicit and conditional.
- **Worktree lane keeps the world; container lane does not.** Worktree
  handles are re-adopted by recorded path via `Resolver` and remain
  inspectable. Container worlds keep their deferred teardown
  (`dispatch_container.go:349-359`); recovery seeds from captured state, which
  now includes a guaranteed transcript for both lanes.
  `TestContainerProposal_SealFailureDestroysContainer`
  (`git_delta_test.go:889-926`) must be rewritten to assert that the artifact
  and transcript survive and the handle is listed, not that all evidence is
  destroyed.

## 7. Acceptance criteria / test matrix

Runtime (new `failed_seal_test.go`, plus `recovery_test.go`,
`seal_options_test.go`, `transcript_test.go`):

1. `PersistAttemptTranscript` is idempotent: two calls with one `attempt_id`
   return the same fact id and write one blob; its bytes survive a later
   failed seal and are never deleted by `cleanup()`.
2. For each injected finalization-stage failure (artifact, capture, diff,
   persist diff, persist metadata, sealed append): Tier 1 exists, the failed
   record names the stage, the scope stays active, and the handle carries a
   digest-verified transcript.
3. An already-expired attempt ctx still produces a complete sealed fact under
   default finalization; `Mode=Inherit` still fails (regression guard).
4. `RecoverScopes` after a simulated restart returns handles with workspace,
   baseline, diff, metadata **and transcript**; `InterruptedSeals` counts a
   Tier-1 artifact with no terminal record.
5. Corrupt/missing transcript ⇒ `TranscriptInvalid=true` and
   `ForkFromFailedSeal` refuses; stale on-disk digest ⇒ `Stale=true` and same
   refusal.
6. `ForkFromFailedSeal` produces a child with the artifact's changes and
   transcript, and that child seals normally with `derived_from` set.
7. Retry after a failure ⇒ sealed fact cites `artifact_id`; the failed-seal
   handle is no longer surfaced; both records remain in the trace.
8. `DiscardFailedSeal` deletes artifact and transcript blobs and stops
   surfacing the handle, without touching the sandbox.
9. Deadline preflight produces `ErrSealDeadlineBeforePersist` and never
   reaches `PutBlob`/`Append` with a dead context.

yaah: transcript persistence happens before seal; proposal list shows the
failed-seal entry; reaper spares its worktree; continue from it re-seeds
transcript and files; container lane keeps the artifact; the rewritten
teardown test passes.

## 8. Sequencing

1. **Runtime bead `shepherd-runtime-go-mtf`:** transcript fact + API, finalization
   phase, revised `Seal`, recovery handles, `ForkFromFailedSeal`, typed errors,
   tests. Release as a minor version — finalization defaults change.
2. **yaah bead `yaah-9nqf`:** transcript call site, registry, error path,
   continue/recover, reaper, failure classification, test rewrites, dependency
   bump.
3. **Kernel (optional, separate bead):** pre-flight `ctx.Err()` in `PutBlob`
   and `Append` with a shared sentinel, so every caller gets the clear error.

## 9. Non-goals

- Not journaling every body edit. The conversation is written once, durably,
  at the body/seal boundary; per-message journaling is a possible later phase
  if crash-mid-body recovery becomes a requirement.
- Not auto-retrying a failed dispatch (`yaah-1224` is still a decision).
- Not keeping container worlds alive just to preserve a handle.
- Not changing the settlement machine, consume-once semantics, or the review
  pair's authority rules.
- Not adding a garbage collector for named blobs; explicit discard is the
  owner action, as today.

## 10. As-built notes

Implementation landed in `failed_seal.go`, `settlement.go`, `scope_manager.go`,
`recovery.go`, with tests in `failed_seal_test.go`. Four details differ from the
first draft of this document:

- **Blob keys are reserved output ids, not artifact ids.** `Seal` allocates the
  prospective `out:<owner>:<seq>` before writing the artifact and records it in
  the artifact payload. Failed-seal blobs and successful seal blobs therefore
  share one addressing contract, and existing sealed facts and recovery tests
  keep working unchanged.
- **Tier 1 carries the baseline; the first Tier-2 values record carries the
  workspace state and change list.** A `values` record is appended immediately
  after capture/diff, before any blob write, so even a blob failure leaves
  state and paths behind. The second values record names written blobs; the
  failed-seal fact also names them, so a values-record failure cannot hide the
  bytes.
- **Staleness compares revision, not the full state digest**, matching the
  checkpoint rule: a re-adopted sandbox may report non-position data
  differently, but a moved timeline is real drift. `ForkFromFailedSeal` refuses
  stale or transcript-invalid handles.
- **A reseed resolves its source artifact via `derived_from`** as well as the
  new seal's own `artifact_id`. Recovery and the live manager both stop
  surfacing the source handle once a resealed child has sealed successfully.
- `sealFault` is an unexported in-package test seam for injecting a failure at
  a named finalization stage; production code never sets it.

## 11. Decisions

Resolved with the bead owner:

1. **Default finalization mode** — **detached by default**, with a bounded
   finalization budget and `Inherit` as the explicit opt-out. Rejected:
   preserving `Inherit` as default (leaves the failure class intact for any
   caller that forgets) or requiring an explicit mode (breaking signature
   change for no correctness gain).
2. **Guarantee level** — **both workspace state and conversation are
   guaranteed.** This is why the transcript has its own idempotent fact and
   its own detached budget §3-4, rather than being best-effort Tier 2. A
   transcript-persist failure yields a distinct error and no recoverable
   handle; a surfaced handle always has a digest-verified transcript.
3. **Physical worlds** — **worktree kept, container state-only.** The
   worktree is re-adopted by recorded path through `Resolver`; containers are
   torn down and reseeded from captured state.

Still open (implementation details, not architecture):

- Finalization budget value (proposed 30 s) and transcript-write retry count.
- Whether `ForkFromFailedSeal` records a `goatherd.seal.recovered.v1` event
  citing the artifact, or relies on `derived_from` on the child fact.
- Naming: `FailedSeal` as the exported handle type.
