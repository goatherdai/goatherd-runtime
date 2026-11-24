package goatherd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	trace "github.com/goatherdai/goatherd-kernel"
	sandbox "github.com/goatherdai/goatherd-runtime/sandbox"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"
)

// Settlement schema refs for trace recording (plan 04 §2). These are
// Go-kernel extensions like the scope/checkpoint schemas — documented as
// such, not pretending to be shepherd2.run_output.v1.
const (
	// SchemaRunOutputSealed is recorded on the sealing scope's owner path
	// when an output is sealed. Declaration: the sealing is stated intent
	// that the settlement records then observe.
	SchemaRunOutputSealed = "goatherd.run_output.sealed.v1"
	// SchemaRunOutputSettled is recorded on the same owner path when the
	// output is settled, citing the sealed record.
	SchemaRunOutputSettled = "goatherd.run_output.settled.v1"
)

// DefaultDiffCap bounds the diff text persisted by Seal when the caller does
// not set SealOptions.DiffCap: 1 MiB. A diff under the cap is retained
// verbatim; a larger one is truncated with a marker, and the sealed payload
// records both the stored and original sizes so the loss is visible.
const DefaultDiffCap = 1 << 20

// sealInlineLimit is the largest snapshot, metadata, or diff payload stored
// inline in the sealed fact. Larger values ride the kernel's named-blob store
// with only a digest and size in the fact, so a workspace snapshot cannot
// inline megabytes into the append-only trace.
const sealInlineLimit = 4 << 10

// Named-blob kinds for seal payloads. The blob key is the output id.
const (
	sealBlobSnapshot = "sealed_snapshot"
	sealBlobMetadata = "sealed_metadata"
	sealBlobDiff     = "sealed_diff"
)

// SealOptions carries caller state into a seal. The zero value adds nothing:
// no execution snapshot, no metadata, no explicit lineage, and the default
// diff cap.
//
// The snapshot is the opaque execution state the caller wants to keep beside
// the output (a sub-agent's conversation, a replay checkpoint). Metadata is
// arbitrary caller bytes with no contract. Both are frozen at seal time and
// returned byte-identically by a recovered output.
//
// Snapshot and metadata are durable: they are written to the trace, inline or
// in the blob store, and outlive the process. They are caller-opaque and are
// not redacted, so a caller must not put secrets there unless the trace is
// trusted with them.
type SealOptions struct {
	// Snapshot is the opaque execution snapshot to retain with the output. It
	// is persisted durably (inline or as a blob); treat it as trace-visible.
	Snapshot []byte
	// Metadata is opaque caller bytes to retain with the output. It is
	// persisted durably (inline or as a blob); treat it as trace-visible.
	Metadata []byte
	// DiffCap bounds the diff text retained in bytes, marker included. Zero
	// or negative uses DefaultDiffCap. A diff over the cap is truncated with
	// a marker in the text plus diff_truncated/diff_original_size in the
	// sealed payload.
	DiffCap int
	// DerivedFrom is the retained output this one was forked or continued
	// from. Empty uses the scope's fork lineage (set by ForkFromOutput);
	// an explicit value overrides it.
	DerivedFrom string
	// Supersedes is the retained output this one replaces. The superseded
	// output's SupersededBy() reports this output after the seal: live when
	// sealed through ScopeManager.Seal, or rebuilt on recovery.
	Supersedes string
	// AttemptID is the host's opaque identifier for the run that produced the
	// work. It is recorded on the failed-seal artifact so a host can match a
	// handle to the dispatch that created it.
	AttemptID string
	// TranscriptID is the fact id returned by PersistAttemptTranscript. When
	// set, the sealed fact cites the journal instead of duplicating the
	// conversation as a snapshot, and a failed seal can always recover it.
	TranscriptID string
	// Finalization overrides how the durable part of the seal is
	// context-bound. The zero value runs finalization detached from the
	// caller's cancellation with a bounded deadline (see FinalizationOptions).
	Finalization FinalizationOptions
	// SealContext is the v0.25.0 compatibility alias for
	// Finalization.Context with FinalizationInherit mode. Deprecated: use
	// Finalization. Setting both is an error.
	SealContext context.Context
}

// sealBlobRef names a blob written for an output, so a failed seal can remove
// the bytes it wrote before failing.
type sealBlobRef struct {
	kind string
	key  string
}

// singleSealOptions accepts the variadic call shape and rejects more than one
// option set rather than silently ignoring the rest.
func singleSealOptions(opts []SealOptions) (SealOptions, error) {
	switch len(opts) {
	case 0:
		return SealOptions{}, nil
	case 1:
		opt := opts[0]
		if opt.Finalization.Context != nil && opt.SealContext != nil {
			return SealOptions{}, errors.New("seal: set either Finalization.Context or SealContext, not both")
		}
		if opt.SealContext != nil && opt.Finalization.Context == nil {
			// Preserve any mode/timeout the caller also supplied; only the
			// default mode is Inherit, matching v0.25.0's alias semantics.
			if opt.Finalization.Mode == "" {
				opt.Finalization.Mode = FinalizationInherit
			}
			opt.Finalization.Context = opt.SealContext
			// ScopeManager.Seal normalizes options before handing them to
			// Scope.Seal; clearing the alias prevents a second mapping from
			// looking like a conflict.
			opt.SealContext = nil
		}
		switch opt.Finalization.Mode {
		case "", FinalizationDetached, FinalizationInherit:
		default:
			return SealOptions{}, fmt.Errorf("seal: unknown finalization mode %q", opt.Finalization.Mode)
		}
		return opt, nil
	default:
		return SealOptions{}, fmt.Errorf("seal accepts at most one SealOptions, got %d", len(opts))
	}
}

// SettlementAction is how a sealed output is consumed exactly once.
type SettlementAction string

const (
	SettleSelect  SettlementAction = "selected"
	SettleApply   SettlementAction = "applied"
	SettleRelease SettlementAction = "released"
	SettleDiscard SettlementAction = "discarded"
)

// OutputState is the consume-once lifecycle state of a retained output.
type OutputState string

const (
	OutputUnconsumed OutputState = "unconsumed"
	OutputSelected   OutputState = "selected"
	OutputApplied    OutputState = "applied"
	OutputReleased   OutputState = "released"
	OutputDiscarded  OutputState = "discarded"
	// OutputInvalid marks an output whose sealed state was discovered
	// unusable. No core path sets it today; it exists so a caller (or a
	// future recovery pass, plan 05 §4) can mark a state that must never be
	// settled without pretending it was consumed.
	OutputInvalid OutputState = "invalid"
)

// ErrOutputConsumed reports a second settlement of an already-settled
// output. Settlement is consume-once: every action is terminal.
var ErrOutputConsumed = errors.New("goatherd: retained output already settled")

// ErrApplyConflict reports a SettleApply whose changes overlap paths the
// parent changed since the fork baseline. The output is NOT consumed by a
// conflict — apply failure never consumes.
var ErrApplyConflict = errors.New("goatherd: apply conflict")

// ErrSettleEffectLanded reports a settle whose physical effect landed
// on the parent's workspace but whose causal merge failed to complete.
// The workspace change happens before the merge, so a blind retry would
// re-apply an effect that already happened; callers detecting this
// sentinel should treat the work as standing on the parent and end the
// output's lifecycle rather than retry. The failure may be at any merge
// stage — validation, the trace append, or the post-record sandbox
// teardown — and the work is landed in all of them. The lifecycle ending
// differs by stage: SettleRelease always works and keeps the landed
// work; SettleDiscard additionally destroys the child's sandbox, which
// requires the child still active — true when the merge failed before
// its record (validation, append), false after a teardown failure (the
// child is already merged; discard refuses it). The settle's error
// message names the ending its child's state allows. The output stays
// unconsumed — the kernel's own partial-settlement contract. Wrapped in
// the settle error alongside the merge failure.
var ErrSettleEffectLanded = errors.New("goatherd: settle effect landed")

// RetainedOutput is a sealed child result: the child's captured workspace
// frozen at seal time, settle-able exactly once. It outlives the scope that
// sealed it — sealing does not merge — so a supervisor can compare sealed
// outputs and choose, the fork-and-choose rhythm.
//
// All fields are unexported behind lock-taking getters: Settle mutates the
// lifecycle fields under mu, and exported fields would let any caller race
// that write.
type RetainedOutput struct {
	// mu guards the lifecycle fields (state, action, settledAt) across
	// concurrent settle attempts: the whole validate → act → mark sequence
	// holds it, so two settles cannot both claim the unconsumed state.
	mu sync.Mutex

	id        string
	scopeID   string
	workspace trace.WorkspaceState
	// baseline is the parent's fork-time state the changes are against.
	baseline trace.WorkspaceState
	changes  []ProposedChange
	diff     string
	// snapshot and metadata are the caller's opaque seal-time bytes. They
	// are frozen copies, never aliases of the caller's slices.
	snapshot []byte
	metadata []byte
	// diffTruncated reports whether Diff() was bounded by the seal's cap;
	// diffOriginalSize is the pre-truncation length in bytes.
	diffTruncated    bool
	diffOriginalSize int
	// Lineage output ids: derivedFrom is the output this one came from,
	// supersedes is the output it replaces, and supersededBy is filled on
	// the older output when a newer one supersedes it.
	derivedFrom  string
	supersedes   string
	supersededBy string
	state        OutputState
	// action is the settlement that consumed the output; zero until settled.
	action    SettlementAction
	sealedAt  time.Time
	settledAt time.Time

	// mgr is set when the output is registered with a ScopeManager, which is
	// what makes Settle usable. Scope.Seal alone does not register.
	mgr *ScopeManager
	// sealedFactID cites the sealed record from the settled record.
	sealedFactID string
	// transcriptID cites the durable attempt transcript when the seal used
	// the journal instead of duplicating the conversation as a snapshot.
	transcriptID string
	// artifactID cites the Tier-1 failed-seal artifact written before this
	// seal's durable work. A successful seal resolves that artifact.
	artifactID string
}

// ID returns the output's unique identifier ("out:<owner>:<seq>").
func (o *RetainedOutput) ID() string { return o.id }

// ScopeID returns the scope that sealed this output.
func (o *RetainedOutput) ScopeID() string { return o.scopeID }

// Workspace returns the sealed sandbox state.
func (o *RetainedOutput) Workspace() trace.WorkspaceState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.workspace
}

// Baseline returns the parent's fork-time state the sealed changes are
// against.
func (o *RetainedOutput) Baseline() trace.WorkspaceState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.baseline
}

// Changes returns the sealed output's proposed changes, frozen at seal time.
func (o *RetainedOutput) Changes() []ProposedChange {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Copy: the caller must not be able to mutate the frozen record.
	cp := make([]ProposedChange, len(o.changes))
	copy(cp, o.changes)
	return cp
}

// Diff returns the sealed output's full diff against the baseline. When the
// seal's DiffCap truncated it, Diff() returns the truncated text plus the
// marker, and DiffTruncated reports the loss.
func (o *RetainedOutput) Diff() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.diff
}

// DiffTruncated reports whether the retained diff was truncated by the seal's
// DiffCap.
func (o *RetainedOutput) DiffTruncated() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.diffTruncated
}

// DiffOriginalSize returns the diff's size in bytes before any truncation.
func (o *RetainedOutput) DiffOriginalSize() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.diffOriginalSize
}

// DerivedFrom returns the retained output this one was forked or continued
// from, or "" when it has no recorded lineage.
func (o *RetainedOutput) DerivedFrom() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.derivedFrom
}

// Supersedes returns the retained output this one replaces, or "".
func (o *RetainedOutput) Supersedes() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.supersedes
}

// SupersededBy returns the retained output that superseded this one, or "".
// It is linked live by ScopeManager.Seal and rebuilt by recovery from the
// later output's supersedes edge.
func (o *RetainedOutput) SupersededBy() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.supersededBy
}

// Snapshot returns the opaque execution snapshot frozen at seal time, or nil
// when the caller supplied none. The returned slice is a copy: mutating it
// cannot change the retained output.
func (o *RetainedOutput) Snapshot() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.snapshot == nil {
		return nil
	}
	return append([]byte(nil), o.snapshot...)
}

// Metadata returns the opaque caller metadata frozen at seal time, or nil
// when the caller supplied none. The returned slice is a copy.
func (o *RetainedOutput) Metadata() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.metadata == nil {
		return nil
	}
	return append([]byte(nil), o.metadata...)
}

// State returns the consume-once lifecycle state.
func (o *RetainedOutput) State() OutputState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state
}

// Action returns the settlement that consumed the output; the zero value
// until settled.
func (o *RetainedOutput) Action() SettlementAction {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.action
}

// SealedAt returns when the output was sealed.
func (o *RetainedOutput) SealedAt() time.Time { return o.sealedAt }

// SettledAt returns when the output was settled; the zero time until then.
func (o *RetainedOutput) SettledAt() time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.settledAt
}

// Settle consumes the output with the given action. It requires the output
// to be registered with a ScopeManager (ScopeManager.Seal registers;
// Scope.Seal alone does not) — use the manager's Settle directly otherwise.
func (o *RetainedOutput) Settle(ctx context.Context, action SettlementAction) error {
	if o.mgr == nil {
		return fmt.Errorf("retained output %s is not registered with a scope manager; settle through the manager that sealed it", o.id)
	}
	return o.mgr.Settle(ctx, o.id, action)
}

// Seal captures the scope's workspace as a retained output and freezes its
// changes against the fork baseline. The scope stays active — sealing does
// not merge, and the output outlives the scope by design.
//
// The scope must have a sandbox and a fork baseline (a parent with a sandbox
// at fork time); a scope without a baseline has nothing to diff against and
// sealing it would freeze changes with no base to settle them onto.
//
// SealOptions adds the caller's opaque execution snapshot and metadata and
// bounds the retained diff. The diff is always retained (DefaultDiffCap when
// the caller sets none), which fixes the recovered-output-with-no-diff gap.
// Large values are written to the kernel's named-blob store and referenced by
// digest; small values inline in the fact.
//
// Records goatherd.run_output.sealed.v1 on the scope's owner path. The append
// is hard, not advisory: the sealed record is the durable half of the output
// (plan 05 §4 rebuilds metadata from it), so a trace that cannot carry it
// must not hand out an output whose settlement would be unrecordable.
func (s *Scope) Seal(ctx context.Context, opts ...SealOptions) (*RetainedOutput, error) {
	opt, err := singleSealOptions(opts)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	state := s.state
	sb := s.sandbox
	ownerID := s.ownerID
	scopeID := s.id
	baseline := s.baseline
	derivedFrom := s.derivedFrom
	s.mu.RUnlock()

	if state != ScopeActive {
		return nil, fmt.Errorf("cannot seal scope %s in state %s", scopeID, state)
	}
	if sb == nil {
		return nil, sandbox.ErrNoSandbox
	}
	if baseline.Backend == "" {
		return nil, fmt.Errorf("scope %s has no fork baseline: it was forked from a parent without a sandbox, so its changes have no base to settle onto", scopeID)
	}
	if opt.DerivedFrom != "" {
		derivedFrom = opt.DerivedFrom
	}

	// Finalization is a distinct phase: durable work gets its own bounded
	// context instead of inheriting an attempt deadline that may already
	// have expired (the failure mode behind shepherd-runtime-go-mtf).
	sealCtx, cancel := finalize(ctx, opt.Finalization)
	defer cancel()

	// Preflight: never open a doomed transaction on an already-dead context.
	if err := sealCtx.Err(); err != nil {
		return nil, &SealError{Stage: "preflight", Err: fmt.Errorf("%w: %v", ErrSealDeadlineBeforePersist, err)}
	}

	// A caller-supplied transcript must be a real, digest-verified fact on
	// this scope. Rejecting it before the artifact is written keeps every
	// surfaced handle capable of reseeding its own conversation.
	var transcriptBytes []byte
	if opt.TranscriptID != "" {
		var err error
		transcriptBytes, err = verifyTranscriptRef(sealCtx, s.store, ownerID, scopeID, opt.TranscriptID)
		if err != nil {
			return nil, &SealError{Stage: "transcript", Err: err}
		}
	}

	// Tier 1: the recovery handle exists before capture, diff, or any blob
	// write, so a failure at any later stage can always name an artifact.
	// The prospective output id is reserved here too: failed-seal blobs use
	// that key, so a successful seal and its failed counterpart share the
	// same blob-addressing contract.
	seq := nextCheckpointSeq.Add(1)
	outputID := fmt.Sprintf("out:%s:%d", ownerID, seq)
	artifactID := fmt.Sprintf("art:%s:%d", ownerID, seq)
	if err := recordSealArtifact(sealCtx, s.store, ownerID, scopeID, artifactID, outputID, opt.AttemptID, opt.TranscriptID, baseline, derivedFrom, opt.Supersedes); err != nil {
		return nil, &SealError{Stage: "artifact", Err: err}
	}
	var captured trace.WorkspaceState
	var changeList []ProposedChange
	var diffText string
	var metadata []byte
	failWith := func(stage string, cause error, written []sealBlobRef) (*RetainedOutput, error) {
		blobs := valueKinds(written)
		present := make([]string, 0, 3)
		if diffText != "" {
			present = append(present, "diff")
		}
		if len(metadata) > 0 && opt.TranscriptID == "" {
			present = append(present, "metadata")
		}
		recordSealFailed(sealCtx, s.store, ownerID, artifactID, stage, cause.Error(), present, blobs)
		handle := &FailedSeal{
			ID:                artifactID,
			ArtifactID:        artifactID,
			OutputID:          outputID,
			ScopeID:           scopeID,
			OwnerID:           ownerID,
			AttemptID:         opt.AttemptID,
			TranscriptID:      opt.TranscriptID,
			Transcript:        transcriptBytes,
			TranscriptInvalid: opt.TranscriptID != "" && transcriptBytes == nil,
			Workspace:         captured,
			Baseline:          baseline,
			Changes:           changeList,
			Diff:              diffText,
			Metadata:          metadata,
			Values:            present,
			Blobs:             blobs,
			CreatedAt:         time.Now().UnixNano(),
			Stage:             stage,
			Reason:            cause.Error(),
			Adopted:           true, // the scope's own sandbox is still live here
		}
		return nil, &SealError{Stage: stage, ArtifactID: artifactID, Err: cause, Handle: handle}
	}
	fail := func(stage string, cause error) (*RetainedOutput, error) {
		return failWith(stage, cause, nil)
	}

	ws, err := sb.Capture(sealCtx)
	if err != nil {
		return fail("capture", err)
	}

	// The output's delta must carry its merge base recorded now, at seal
	// time, and pinned against backend-side garbage collection — not
	// resolved at settle time from a legacy identity that may already
	// have been reclaimed. Backends without the capability (their deltas
	// need no base) seal unchanged.
	if ann, ok := sb.(sandbox.DeltaBaseAnnotator); ok {
		if err := ann.AnnotateDeltaBase(sealCtx, &ws, baseline); err != nil {
			return fail("delta_base", err)
		}
	}

	diff, files, err := sb.Diff(sealCtx, baseline, 0)
	if err != nil {
		return fail("diff", err)
	}
	retainedDiff, diffTruncated := boundDiff(diff, opt.DiffCap)
	changes := classifyChanges(diff, files)
	// Record the capture before the digest check: capture and diff both
	// succeeded, and a digest_state failure must still hand back a live
	// handle carrying the workspace, changes and diff — that state is what
	// recovery seeds a continuation from, and it exists only in memory
	// here (the state values fact is deliberately never written on this
	// path, so a restarted handle cannot recover it).
	captured, changeList, diffText = ws, changes, retainedDiff
	stateDigest, digestErr := ws.Digest()
	if digestErr != nil {
		return fail("digest_state", fmt.Errorf("digest sealed workspace: %w", digestErr))
	}

	// Tier 2 metadata: workspace state and the change list are small and
	// inline, so they survive even when the larger values cannot be written.
	if err := recordSealValues(sealCtx, s.store, ownerID, artifactID, map[string]any{
		"state":              workspacePayload(ws),
		"state_digest":       stateDigest,
		"changes":            changesPayload(changes),
		"diff_size":          len(retainedDiff),
		"diff_original_size": len(diff),
		"diff_truncated":     diffTruncated,
	}); err != nil {
		return fail("values", err)
	}

	out := &RetainedOutput{
		id:               outputID,
		scopeID:          scopeID,
		workspace:        ws,
		baseline:         baseline,
		changes:          changes,
		diff:             retainedDiff,
		snapshot:         append([]byte(nil), opt.Snapshot...),
		metadata:         append([]byte(nil), opt.Metadata...),
		diffTruncated:    diffTruncated,
		diffOriginalSize: len(diff),
		derivedFrom:      derivedFrom,
		supersedes:       opt.Supersedes,
		state:            OutputUnconsumed,
		sealedAt:         time.Now(),
	}

	baselineDigest, digestErr := baseline.Digest()
	if digestErr != nil {
		return fail("digest_baseline", fmt.Errorf("digest fork baseline: %w", digestErr))
	}
	payload := map[string]any{
		"output_id":       out.id,
		"artifact_id":     artifactID,
		"scope":           scopeID,
		"backend":         ws.Backend,
		"revision":        ws.Revision,
		"state_digest":    stateDigest,
		"baseline_digest": baselineDigest,
		"change_count":    len(out.changes),
		// The full states and the change list ride in the payload so
		// recovery (plan 05 section 4) rebuilds settle-able outputs:
		// digests alone cannot restore a WorkspaceState, and the
		// overlap guard needs the paths.
		"state":    workspacePayload(ws),
		"baseline": workspacePayload(baseline),
		"changes":  changesPayload(out.changes),
	}
	out.artifactID = artifactID
	metadata = out.metadata
	// A later successful seal of the same scope resolves earlier artifacts
	// even when it uses a fresh artifact id (a direct retry).
	payload["sealed_at"] = out.sealedAt.UnixNano()
	if opt.TranscriptID != "" {
		// The conversation is durable in its own fact; do not duplicate it
		// into the sealed payload or the blob store. The in-memory accessor
		// still reports the already-verified bytes, and recovery restores
		// them from the same fact.
		payload["transcript_id"] = opt.TranscriptID
		out.snapshot = transcriptBytes
	}
	// Lineage is recorded on the output's own fact; the reverse edge
	// (supersededBy) is derived from a later output's supersedes value at
	// seal time or on recovery.
	if out.derivedFrom != "" {
		payload["derived_from"] = out.derivedFrom
	}
	if out.supersedes != "" {
		payload["supersedes"] = out.supersedes
	}

	// Values are keyed by the output id reserved when the artifact was
	// written: a failed seal owns them and only DiscardFailedSeal removes
	// them. A failure no longer
	// deletes what it wrote — those bytes are the recovery payload named by
	// the failed-seal record. (A fact never references bytes it did not
	// write: blobs are always written before the fact that cites them.)
	var written []sealBlobRef

	payload["diff_size"] = len(retainedDiff)
	payload["diff_original_size"] = len(diff)
	payload["diff_truncated"] = diffTruncated
	if err := sealCtx.Err(); err != nil {
		return fail("persist_diff", fmt.Errorf("%w: %v", ErrSealDeadlineBeforePersist, err))
	}
	if err := putSealText(sealCtx, s.store, ownerID, outputID, sealBlobDiff, "diff", payload, retainedDiff, &written); err != nil {
		return failWith("persist_diff", err, written)
	}
	if len(out.snapshot) > 0 && opt.TranscriptID == "" {
		if err := sealCtx.Err(); err != nil {
			return failWith("persist_snapshot", fmt.Errorf("%w: %v", ErrSealDeadlineBeforePersist, err), written)
		}
		if err := putSealValue(sealCtx, s.store, ownerID, outputID, sealBlobSnapshot, "snapshot", payload, out.snapshot, &written); err != nil {
			return failWith("persist_snapshot", err, written)
		}
	}
	if len(out.metadata) > 0 {
		if err := sealCtx.Err(); err != nil {
			return failWith("persist_metadata", fmt.Errorf("%w: %v", ErrSealDeadlineBeforePersist, err), written)
		}
		if err := putSealValue(sealCtx, s.store, ownerID, outputID, sealBlobMetadata, "metadata", payload, out.metadata, &written); err != nil {
			return failWith("persist_metadata", err, written)
		}
	}
	// Always record the values that landed, including inline ones: they live
	// only in the sealed payload, which never arrives when the append fails.
	present := []string{"diff"}
	if len(out.metadata) > 0 {
		present = append(present, "metadata")
	}
	if len(out.snapshot) > 0 && opt.TranscriptID == "" {
		present = append(present, "snapshot")
	}
	valuesPayload := map[string]any{
		"values": present,
		"blobs":  valueKinds(written),
	}
	for _, key := range []string{"diff", "metadata", "snapshot"} {
		if inline, ok := payload[key].(string); ok {
			valuesPayload[key] = inline
		}
	}
	if err := recordSealValues(sealCtx, s.store, ownerID, artifactID, valuesPayload); err != nil {
		return failWith("values", err, written)
	}

	if err := sealCtx.Err(); err != nil {
		return failWith("sealed_append", fmt.Errorf("%w: %v", ErrSealDeadlineBeforePersist, err), written)
	}
	if sealFault != nil {
		if err := sealFault("sealed_append"); err != nil {
			return failWith("sealed_append", err, written)
		}
	}
	receipt, err := s.store.Append(sealCtx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:seal:%d", ownerID, seq),
		Groups: []trace.AppendGroup{{
			TraceOwnerID: ownerID,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Declaration,
				SchemaRef: SchemaRunOutputSealed,
				KindLabel: "run_output:sealed",
				Payload:   payload,
			}},
		}},
	})
	if err != nil {
		return failWith("sealed_append", err, written)
	}
	if len(receipt.FactIDs) > 0 {
		out.sealedFactID = receipt.FactIDs[0]
	}
	return out, nil
}

// sealFault is an unexported test seam. In-package tests inject a failure at
// a named finalization stage to exercise failed-seal artifact paths that are
// otherwise hard to provoke deterministically (for example, an append fault
// after the diff blob was written). Production never sets it.
var sealFault func(stage string) error

// valueKinds names the blobs written for an artifact, for the failed-seal
// record and the Tier-2 values fact.
func valueKinds(written []sealBlobRef) []string {
	out := make([]string, 0, len(written))
	for _, b := range written {
		out = append(out, b.kind)
	}
	return out
}

// changesPayload renders a change list as trace payload entries.
func changesPayload(changes []ProposedChange) []map[string]any {
	out := make([]map[string]any, 0, len(changes))
	for _, c := range changes {
		out = append(out, map[string]any{"path": c.Path, "kind": c.Kind})
	}
	return out
}

// boundDiff applies the seal's diff cap. A diff at or under the cap is
// returned verbatim. A larger one is truncated on a rune boundary and gets a
// marker naming the loss; the sealed payload records diff_truncated and the
// original size, so a reader never has to guess. The marker is part of the
// retained text, so the result is never larger than the cap: when the cap is
// too small for content plus marker, the marker's own prefix is retained and
// the payload still carries the full sizes.
func boundDiff(diff string, cap int) (string, bool) {
	if cap <= 0 {
		cap = DefaultDiffCap
	}
	if len(diff) <= cap {
		return diff, false
	}
	marker := fmt.Sprintf("\n\n[goatherd: diff truncated; original was %d bytes]\n", len(diff))
	keep := cap - len(marker)
	if keep < 0 {
		return trimToRuneBoundary(marker, cap), true
	}
	return trimToRuneBoundary(diff, keep) + marker, true
}

// trimToRuneBoundary returns at most n bytes of s without splitting a UTF-8
// rune, so the retained text stays valid when it is inlined in canonical JSON.
func trimToRuneBoundary(s string, n int) string {
	if n >= len(s) {
		return s
	}
	if n <= 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// putSealValue writes one sealed value into the fact payload: base64 inline
// when it fits sealInlineLimit, otherwise a kernel named blob referenced by
// digest and size. prefix selects the payload keys: prefix, prefix_digest,
// and prefix_size.
func putSealValue(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, key, blobKind, prefix string, payload map[string]any, data []byte, written *[]sealBlobRef) error {
	if len(data) <= sealInlineLimit {
		payload[prefix] = base64.StdEncoding.EncodeToString(data)
		payload[prefix+"_size"] = len(data)
		return nil
	}
	digest, err := trace.PutBlob(ctx, store, ownerID, blobKind, key, data)
	if err != nil {
		return err
	}
	payload[prefix+"_digest"] = digest
	payload[prefix+"_size"] = len(data)
	*written = append(*written, sealBlobRef{kind: blobKind, key: key})
	return nil
}

// putSealText writes an already-textual value: inline as the string itself
// when it fits sealInlineLimit (so the diff is readable in the fact), else as
// a named blob referenced by digest and size.
func putSealText(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, key, blobKind, prefix string, payload map[string]any, text string, written *[]sealBlobRef) error {
	if len(text) <= sealInlineLimit {
		payload[prefix] = text
		payload[prefix+"_size"] = len(text)
		return nil
	}
	return putSealValue(ctx, store, ownerID, key, blobKind, prefix, payload, []byte(text), written)
}

// stateFor maps a settlement action to its terminal output state.
func stateFor(action SettlementAction) (OutputState, error) {
	switch action {
	case SettleSelect:
		return OutputSelected, nil
	case SettleApply:
		return OutputApplied, nil
	case SettleRelease:
		return OutputReleased, nil
	case SettleDiscard:
		return OutputDiscarded, nil
	default:
		return "", fmt.Errorf("unknown settlement action %q", action)
	}
}

// Settle consumes a registered output exactly once.
//
// The verbs (plan 04 §2, mirroring the reference settlement machine):
//
//   - selected: the parent's workspace must be bit-identical to the fork
//     baseline (fast-forward-only, strictly). The sealed state is applied
//     onto the parent and the child is causally merged. A parent that
//     moved since the fork — even one whose content is identical but
//     whose timeline advanced — gets an actionable error pointing at
//     apply.
//   - applied: three-way. The parent's changes since the baseline must be
//     path-disjoint from the output's changes; the sealed delta is then
//     merged onto the parent's current workspace (sandbox.DeltaApplier). A path
//     overlap returns ErrApplyConflict and consumes nothing; so does any
//     merge failure — apply failure never consumes.
//
// Both physical verbs record the causal merge AFTER the workspace change:
// if the recording fails, the work has already landed and the error wraps
// ErrSettleEffectLanded — the caller knows the effect happened and must
// not retry it, and the output stays unconsumed for exactly that
// distinction.
//   - released: mark consumed, keep the sealed state addressable. No
//     workspace change; the scope's lifecycle is the caller's to finish.
//   - discarded: the child scope is discarded (destroying a sandbox it
//     owns); the sealed state is retained as a record.
//
// Select and apply end with a causal merge of the child into the parent,
// which also destroys a child-owned sandbox — safe because the sealed state
// outlives it (git worktrees share the object store; a snapshot-backed
// backend pins captured states on a lease).
//
// Records goatherd.run_output.settled.v1 on the sealing scope's owner path.
// The append is advisory, checkpoint-style: by the time it runs the
// settlement's physical effects have happened and cannot be rolled back.
//
// Select and apply are not transactional across the two systems they touch:
// the workspace effect lands first, and the causal merge (trace append plus
// scope teardown) runs after. If that recording fails, the parent already
// holds the output's changes and the error says so; the output stays
// unconsumed, so the caller can end its lifecycle with SettleRelease or
// SettleDiscard rather than retry a physical effect that already happened.
func (m *ScopeManager) Settle(ctx context.Context, outputID string, action SettlementAction) error {
	m.mu.RLock()
	out, ok := m.outputs[outputID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("retained output %s not found", outputID)
	}

	// Hold the output's lock for the whole validate → act → mark sequence:
	// a concurrent settle of the same output must not run its side effects
	// twice. All failure paths below return before mutating anything.
	out.mu.Lock()
	defer out.mu.Unlock()

	if out.state != OutputUnconsumed {
		return fmt.Errorf("%w: output %s is %s", ErrOutputConsumed, out.id, out.state)
	}
	terminal, err := stateFor(action)
	if err != nil {
		return err
	}

	m.mu.RLock()
	scope, scopeOK := m.scopes[out.scopeID]
	m.mu.RUnlock()
	if !scopeOK {
		return fmt.Errorf("scope %s for output %s not found", out.scopeID, out.id)
	}

	switch action {
	case SettleRelease:
		// No workspace change, no scope lifecycle change: release means
		// "keep, don't integrate". The caller discards or merges the scope
		// separately if it owns resources.

	case SettleDiscard:
		parent := scope.Parent()
		if parent == nil {
			return fmt.Errorf("scope %s for output %s has no parent to discard from", out.scopeID, out.id)
		}
		if err := parent.Discard(scope); err != nil {
			return fmt.Errorf("settle %s (discard): %w", out.id, err)
		}

	case SettleSelect:
		parent, parentSb, err := settleParent(scope, out, "select")
		if err != nil {
			return err
		}
		// Serialize against settles of every other output of this parent:
		// the digest check and the apply below must see the same parent
		// workspace, or two selects both pass against an unchanged parent
		// and the tree silently keeps only the last apply.
		unlockParent := m.lockParentSettle(parent.ID())
		defer unlockParent()
		// Fast-forward-only, strictly: the parent's workspace must be
		// bit-identical to the fork baseline, content and timeline.
		// Digest equality is the honest test: since plan 06's content
		// identity a parent with uncommitted content at the fork point
		// digests equal too, so select works over dirty baselines
		// deterministically. A parent whose timeline moved (a commit,
		// even with identical content) still refuses — applying the
		// sealed state resets to the child's HEAD and would discard
		// the parent's commit — so it is told to use apply.
		parentNow, err := parentSb.Capture(ctx)
		if err != nil {
			return fmt.Errorf("settle %s (select): capture parent: %w", out.id, err)
		}
		baseDigest, err := out.baseline.Digest()
		if err != nil {
			return fmt.Errorf("settle %s (select): digest baseline: %w", out.id, err)
		}
		nowDigest, err := parentNow.Digest()
		if err != nil {
			return fmt.Errorf("settle %s (select): digest parent: %w", out.id, err)
		}
		if nowDigest != baseDigest {
			return fmt.Errorf("settle %s (select): parent workspace has moved since the fork baseline; use SettleApply", out.id)
		}
		if err := parent.ApplyWorkspace(ctx, out.workspace); err != nil {
			return fmt.Errorf("settle %s (select): %w", out.id, err)
		}
		if err := parent.Merge(scope); err != nil {
			// The parent's workspace already holds this output's changes;
			// the causal merge is what failed. Say both, so the caller does
			// not retry a physical effect that already happened.
			return fmt.Errorf("settle %s (select): the parent's workspace now holds this output's changes, but completing the causal merge failed: %w (the output stays unconsumed; %s): %w", out.id, err, settleEnding(parent, scope), ErrSettleEffectLanded)
		}

	case SettleApply:
		parent, parentSb, err := settleParent(scope, out, "apply")
		if err != nil {
			return err
		}
		unlockParent := m.lockParentSettle(parent.ID())
		defer unlockParent()
		// The parent's concurrent changes are what it holds now that the
		// baseline does not. Path overlap with the output's own changes
		// means the three-way merge would clobber, so refuse before trying.
		_, parentFiles, err := parentSb.Diff(ctx, out.baseline, 0)
		if err != nil {
			return fmt.Errorf("settle %s (apply): diff parent against baseline: %w", out.id, err)
		}
		if overlap := overlappingPaths(out.changes, parentFiles); len(overlap) > 0 {
			return fmt.Errorf("%w: parent changed %v since the baseline, which the sealed output also changes", ErrApplyConflict, overlap)
		}
		da, canDelta := parentSb.(sandbox.DeltaApplier)
		if !canDelta {
			return fmt.Errorf("settle %s (apply): parent sandbox backend %s does not implement DeltaApplier: %w",
				out.id, parentSb.Backend(), sandbox.ErrUnsupported)
		}
		if err := da.ApplyDelta(ctx, out.workspace); err != nil {
			return fmt.Errorf("settle %s (apply): %w", out.id, err)
		}
		if err := parent.Merge(scope); err != nil {
			return fmt.Errorf("settle %s (apply): the parent's workspace now holds this output's changes, but completing the causal merge failed: %w (the output stays unconsumed; %s): %w", out.id, err, settleEnding(parent, scope), ErrSettleEffectLanded)
		}

	default:
		return fmt.Errorf("unknown settlement action %q", action)
	}

	out.state = terminal
	out.action = action
	out.settledAt = time.Now()
	recordRunOutputSettled(ctx, scope, out, action)
	return nil
}

// lockParentSettle acquires the settle lock for a parent workspace and
// returns its release function. Called only from Settle while the output
// lock is already held, so the acquisition order is always output → parent
// and can never invert.
func (m *ScopeManager) lockParentSettle(parentID string) func() {
	m.mu.Lock()
	lock, ok := m.settleLocks[parentID]
	if !ok {
		if m.settleLocks == nil {
			m.settleLocks = make(map[string]*sync.Mutex)
		}
		lock = &sync.Mutex{}
		m.settleLocks[parentID] = lock
	}
	m.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// settleParent resolves and validates the parent side of a select or apply.
// Both verbs physically change the parent workspace and causally merge the
// child, so the child must still be active and the parent must still be an
// active sandboxed scope. No check here mutates anything.
func settleParent(scope *Scope, out *RetainedOutput, verb string) (*Scope, sandbox.Sandbox, error) {
	if scope.State() != ScopeActive {
		return nil, nil, fmt.Errorf("settle %s (%s): scope %s is %s",
			out.id, verb, scope.ID(), scope.State())
	}
	parent := scope.Parent()
	if parent == nil {
		return nil, nil, fmt.Errorf("settle %s (%s): scope %s has no parent", out.id, verb, scope.ID())
	}
	if parent.State() != ScopeActive {
		return nil, nil, fmt.Errorf("settle %s (%s): parent %s is %s",
			out.id, verb, parent.ID(), parent.State())
	}
	parentSb := parent.Sandbox()
	if parentSb == nil {
		return nil, nil, fmt.Errorf("settle %s (%s): parent %s has no sandbox", out.id, verb, parent.ID())
	}
	return parent, parentSb, nil
}

// settleEnding names the lifecycle ending a partial settle's scopes
// allow. SettleRelease always works. SettleDiscard routes to
// parent.Discard, which requires BOTH scopes active: the merge record
// decides the child (failed-before-record leaves it active; a teardown
// failure has already merged it), and a concurrently retired parent or
// child — halted or discarded while the settle's physical I/O ran —
// refuses Discard just the same. The error names the ending that will
// actually succeed, not the one that usually does.
func settleEnding(parent, scope *Scope) string {
	if scope.State() == ScopeMerged {
		return "the causal merge is recorded; end it with SettleRelease (SettleDiscard refuses a merged child)"
	}
	if parent == nil || parent.State() != ScopeActive || scope.State() != ScopeActive {
		return "end it with SettleRelease (a scope is no longer active; SettleDiscard refuses it)"
	}
	return "end it with SettleRelease or SettleDiscard"
}

// overlappingPaths returns the sealed output's changed paths that the parent
// also changed, in the output's change order.
func overlappingPaths(changes []ProposedChange, parentFiles []string) []string {
	if len(changes) == 0 || len(parentFiles) == 0 {
		return nil
	}
	parentSet := make(map[string]bool, len(parentFiles))
	for _, f := range parentFiles {
		parentSet[f] = true
	}
	var overlap []string
	for _, c := range changes {
		if parentSet[c.Path] {
			overlap = append(overlap, c.Path)
		}
	}
	return overlap
}

// recordRunOutputSettled appends the settlement record on the sealing
// scope's owner path, citing the sealed record. Advisory by design: the
// settlement's physical effects have already happened when this runs.
func recordRunOutputSettled(ctx context.Context, scope *Scope, out *RetainedOutput, action SettlementAction) {
	scope.mu.RLock()
	ownerID := scope.ownerID
	scope.mu.RUnlock()

	causedBy := []string{}
	if out.sealedFactID != "" {
		causedBy = []string{out.sealedFactID}
	}
	stateDigest, digestErr := out.workspace.Digest()
	if digestErr != nil {
		// Settlement's physical effects already happened; this record is
		// advisory. Omit the digest rather than writing an empty one.
		slog.Warn("settlement: digest settled workspace failed (advisory)", "output", out.id, "err", digestErr)
	}
	payload := map[string]any{
		"output_id": out.id,
		"action":    string(action),
	}
	if digestErr == nil {
		payload["state_digest"] = stateDigest
	}
	if out.derivedFrom != "" {
		payload["derived_from"] = out.derivedFrom
	}
	if out.supersedes != "" {
		payload["supersedes"] = out.supersedes
	}

	_, err := scope.store.Append(ctx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:settle:%s:%d", ownerID, action, nextCheckpointSeq.Add(1)),
		Groups: []trace.AppendGroup{{
			TraceOwnerID:  ownerID,
			CausalParents: causedBy,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Capture,
				SchemaRef: SchemaRunOutputSettled,
				KindLabel: "run_output:settled",
				Payload:   payload,
			}},
		}},
	})
	if err != nil {
		slog.Warn("settlement: record settle in trace failed (advisory)", "output", out.id, "action", action, "err", err)
	}
}
