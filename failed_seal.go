package goatherd

// Failed-seal recovery: durable artifacts, transcript journal, and handles.
//
// The sealed fact is all-or-nothing by construction (settlement.go). This file
// adds the durable surroundings that let a host recover when that atomic write
// does not happen: the conversation is written before finalization begins,
// a small recovery handle is written before any payload, and any failure is
// recorded as a fact naming the handle. Recovery (recovery.go) rebuilds the
// handles and can seed a fresh child from one.
//
// Nothing here auto-discards a scope or destroys a sandbox. The host owns
// those decisions; the runtime owns the durable record and the seed primitive.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	trace "github.com/goatherdai/goatherd-kernel"
	sandbox "github.com/goatherdai/goatherd-runtime/sandbox"
)

// Schema refs for the failed-seal recovery records. They are Go-kernel
// extensions, like the settlement and lifecycle schemas.
const (
	// SchemaAttemptTranscript is written at the body/seal boundary by
	// PersistAttemptTranscript. It is idempotent on attempt_id and is never
	// removed by a failed seal.
	SchemaAttemptTranscript = "goatherd.attempt.transcript.v1"
	// SchemaSealArtifact is the Tier-1 recovery handle: inline-only state
	// written before capture/diff/blob work.
	SchemaSealArtifact = "goatherd.seal.artifact.v1"
	// SchemaSealValues carries Tier-2 payload metadata (workspace state,
	// changes, blob digests) citing an artifact_id.
	SchemaSealValues = "goatherd.seal.values.v1"
	// SchemaSealFailed is the terminal failure event for an artifact.
	SchemaSealFailed = "goatherd.seal.failed.v1"
	// SchemaSealArtifactDiscarded is the host's explicit terminal for an
	// artifact it will not recover.
	SchemaSealArtifactDiscarded = "goatherd.seal.artifact.discarded.v1"
)

// Named-blob kind for a persisted conversation. The blob key is attempt_id.
const transcriptBlob = "attempt_transcript"

// DefaultFinalizationTimeout bounds durable seal work once finalization has
// begun. It is independent of the attempt deadline by design: the attempt
// budget governs the body; finalization gets a short, separate window.
const DefaultFinalizationTimeout = 30 * time.Second

// DefaultTranscriptTimeout bounds the pre-seal conversation write.
const DefaultTranscriptTimeout = 10 * time.Second

// DefaultFailureRecordTimeout bounds the best-effort failure fact. It is
// detached from the finalization context because that context is often the
// reason the seal failed.
const DefaultFailureRecordTimeout = 5 * time.Second

// Errors that callers can branch on without string matching.
var (
	// ErrSealFinalization is the root of every typed seal failure.
	ErrSealFinalization = errors.New("goatherd: seal finalization failed")
	// ErrSealDeadlineBeforePersist reports a finalization deadline that
	// expired before a durable write was attempted. No doomed transaction is
	// opened in that case.
	ErrSealDeadlineBeforePersist = errors.New("goatherd: finalization deadline expired before persist")
	// ErrSealTranscriptInvalid reports a failed-seal handle whose transcript
	// is missing, corrupt, or digest-mismatched. Such a handle cannot seed a
	// continuation.
	ErrSealTranscriptInvalid = errors.New("goatherd: failed-seal transcript missing or corrupt")
)

// SealError carries the stage and the artifact id a host needs to recover
// from a failed seal. ArtifactID is empty only when the Tier-1 write itself
// failed — in which case there is no durable handle to recover from.
type SealError struct {
	Stage      string
	ArtifactID string
	Err        error
}

func (e *SealError) Error() string {
	if e.ArtifactID == "" {
		return fmt.Sprintf("seal %s: %v", e.Stage, e.Err)
	}
	return fmt.Sprintf("seal %s (artifact %s): %v", e.Stage, e.ArtifactID, e.Err)
}

func (e *SealError) Unwrap() error { return e.Err }

// FinalizationMode selects how the durable part of a seal is context-bound.
type FinalizationMode string

const (
	// FinalizationDetached (the default) runs finalization under a context
	// detached from the attempt's cancellation, with its own bounded
	// deadline. An expired attempt deadline can no longer destroy finished
	// work at seal time.
	FinalizationDetached FinalizationMode = "detached"
	// FinalizationInherit keeps strict caller cancellation: the attempt
	// context drives finalization too. Hosts that need a hard abort use it.
	FinalizationInherit FinalizationMode = "inherit"
)

// FinalizationOptions overrides how finalization is context-bound. The zero
// value is FinalizationDetached with DefaultFinalizationTimeout.
type FinalizationOptions struct {
	Mode FinalizationMode
	// Timeout bounds finalization. Non-positive uses the mode's default;
	// FinalizationInherit with a non-positive timeout is unbounded, matching
	// plain caller cancellation.
	Timeout time.Duration
	// Context replaces ctx as the parent for the finalization context. A
	// host that has a still-live parent can pass it here; cancellation then
	// flows through that context instead of the attempt context.
	Context context.Context
}

// finalize derives the context used by all durable seal work.
func finalize(ctx context.Context, opt FinalizationOptions) (context.Context, context.CancelFunc) {
	base := ctx
	if opt.Context != nil {
		base = opt.Context
	}
	if opt.Mode == FinalizationInherit {
		if opt.Timeout > 0 {
			return context.WithTimeout(base, opt.Timeout)
		}
		return base, func() {}
	}
	timeout := opt.Timeout
	if timeout <= 0 {
		timeout = DefaultFinalizationTimeout
	}
	// WithoutCancel keeps context values (trace auth travels in the explicit
	// AppendContext, but host values survive too) while dropping the attempt's
	// deadline and cancellation. Finalization owns a new, bounded deadline.
	return context.WithTimeout(context.WithoutCancel(base), timeout)
}

// FailedSeal is the handle recovery rebuilds for an artifact whose seal did
// not complete. A surfaced handle always has a digest-verified transcript;
// that is the guarantee the transcript journal exists to provide.
type FailedSeal struct {
	ID                string
	ArtifactID        string
	OutputID          string
	ScopeID           string
	OwnerID           string
	AttemptID         string
	TranscriptID      string
	Transcript        []byte
	Workspace         trace.WorkspaceState
	Baseline          trace.WorkspaceState
	Changes           []ProposedChange
	Diff              string
	Metadata          []byte
	Values            []string
	Stage             string
	Reason            string
	Adopted           bool
	Stale             bool
	TranscriptInvalid bool
}

// transcriptDigest returns the digest recorded in an attempt transcript fact.
func transcriptDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

// PersistAttemptTranscript writes the body's conversation durably, before
// Seal is called. It is idempotent on attemptID: a retry returns the fact id
// of the first write, so a timeout or a restarted caller cannot create a
// second transcript under the same attempt.
//
// The write uses its own detached, bounded context. This is a durability
// guarantee, not a best-effort sidecar: a failure here means no seal should
// be attempted, because a handle without a conversation is not recoverable.
func (m *ScopeManager) PersistAttemptTranscript(ctx context.Context, attemptID, scopeID string, data []byte) (string, error) {
	if attemptID == "" {
		return "", errors.New("persist attempt transcript: an attempt id is required")
	}
	ownerID := strings.TrimPrefix(scopeID, "scope:")
	if scopeID == "" || ownerID == "" {
		return "", fmt.Errorf("persist attempt transcript %s: a scope id is required", attemptID)
	}

	digest := transcriptDigest(data)
	write := func(wctx context.Context) (string, error) {
		payload := map[string]any{
			"attempt_id": attemptID,
			"scope":      scopeID,
			"size":       len(data),
			"digest":     digest,
		}
		if len(data) <= sealInlineLimit {
			payload["transcript"] = base64.StdEncoding.EncodeToString(data)
		} else {
			if _, err := trace.PutBlob(wctx, m.store, ownerID, transcriptBlob, attemptID, data); err != nil {
				return "", err
			}
			payload["transcript_digest"] = digest
			payload["blob_kind"] = transcriptBlob
			payload["blob_key"] = attemptID
		}
		receipt, err := m.store.Append(wctx, trace.TrustedAppendContext, trace.AppendBatch{
			AppendIntentID: fmt.Sprintf("%s:transcript:%s", ownerID, attemptID),
			Groups: []trace.AppendGroup{{
				TraceOwnerID: ownerID,
				FactDrafts: []trace.RecordDraft{{
					Mode:      trace.Declaration,
					SchemaRef: SchemaAttemptTranscript,
					KindLabel: "attempt:transcript",
					Payload:   payload,
				}},
			}},
		})
		if err != nil {
			return "", err
		}
		if len(receipt.FactIDs) == 0 {
			return "", errors.New("transcript append returned no fact id")
		}
		return receipt.FactIDs[0], nil
	}

	wctx, cancel := finalize(ctx, FinalizationOptions{Timeout: DefaultTranscriptTimeout})
	id, err := write(wctx)
	cancel()
	if err != nil {
		// One bounded retry with a fresh context: a transient deadline or a
		// busy store must not lose the conversation.
		wctx2, cancel2 := finalize(context.WithoutCancel(ctx), FinalizationOptions{Timeout: DefaultTranscriptTimeout})
		id, err = write(wctx2)
		cancel2()
	}
	if err != nil {
		return "", fmt.Errorf("persist attempt transcript %s: %w", attemptID, err)
	}
	return id, nil
}

// readTranscript decodes and verifies a transcript fact's bytes.
func readTranscript(ctx context.Context, store *trace.SQLiteTraceStore, ownerID string, payload map[string]any) ([]byte, bool) {
	want, _ := payload["digest"].(string)
	var data []byte
	if key, _ := payload["blob_key"].(string); key != "" {
		kind, _ := payload["blob_kind"].(string)
		got, _, err := trace.GetBlob(ctx, store, ownerID, kind, key)
		if err != nil {
			slog.Warn("recovery: read attempt transcript blob failed", "owner", ownerID, "key", key, "err", err)
			return nil, false
		}
		data = got
	} else if encoded, _ := payload["transcript"].(string); encoded != "" {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			slog.Warn("recovery: decode attempt transcript failed", "owner", ownerID, "err", err)
			return nil, false
		}
		data = decoded
	} else {
		return nil, false
	}
	if want == "" || transcriptDigest(data) != want {
		return nil, false
	}
	return data, true
}

// recordSealFailed appends the terminal failure fact. It detaches from the
// context that just failed so the failure is still recorded when the seal's
// finalization context expired.
func recordSealFailed(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, artifactID, stage, reason string, values []string) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultFailureRecordTimeout)
	defer cancel()
	payload := map[string]any{
		"artifact_id": artifactID,
		"stage":       stage,
		"reason":      reason,
	}
	if len(values) > 0 {
		payload["values"] = values
	}
	_, err := store.Append(fctx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:seal-failed:%s:%d", ownerID, artifactID, nextCheckpointSeq.Add(1)),
		Groups: []trace.AppendGroup{{
			TraceOwnerID: ownerID,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Capture,
				SchemaRef: SchemaSealFailed,
				KindLabel: "seal:failed",
				Payload:   payload,
			}},
		}},
	})
	if err != nil {
		slog.Warn("seal: failed to record failed-seal artifact", "artifact", artifactID, "stage", stage, "err", err)
	}
}

// workspacePayload renders a workspace state into the shape the sealed facts
// already use. Recovery reads the same keys in workspaceStateFromPayload.
func workspacePayload(ws trace.WorkspaceState) map[string]any {
	return map[string]any{
		"backend":  ws.Backend,
		"revision": ws.Revision,
		"data":     ws.Data,
	}
}

// recordSealArtifact writes the Tier-1 handle. It carries only inline values,
// so no blob write can keep the handle from existing.
func recordSealArtifact(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, scopeID, artifactID, outputID, attemptID, transcriptID string, baseline trace.WorkspaceState, derivedFrom, supersedes string) error {
	payload := map[string]any{
		"artifact_id":   artifactID,
		"output_id":     outputID,
		"attempt_id":    attemptID,
		"transcript_id": transcriptID,
		"scope":         scopeID,
		"owner":         ownerID,
		"created_at":    time.Now().UnixNano(),
		"baseline":      workspacePayload(baseline),
	}
	if derivedFrom != "" {
		payload["derived_from"] = derivedFrom
	}
	if supersedes != "" {
		payload["supersedes"] = supersedes
	}
	_, err := store.Append(ctx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:seal-artifact:%s", ownerID, artifactID),
		Groups: []trace.AppendGroup{{
			TraceOwnerID: ownerID,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Declaration,
				SchemaRef: SchemaSealArtifact,
				KindLabel: "seal:artifact",
				Payload:   payload,
			}},
		}},
	})
	return err
}

// recordSealValues appends one Tier-2 metadata record for an artifact. Values
// themselves are named blobs keyed by artifact id; this record describes them.
func recordSealValues(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, artifactID string, payload map[string]any) error {
	body := map[string]any{"artifact_id": artifactID}
	for k, v := range payload {
		body[k] = v
	}
	_, err := store.Append(ctx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:seal-values:%s:%d", ownerID, artifactID, nextCheckpointSeq.Add(1)),
		Groups: []trace.AppendGroup{{
			TraceOwnerID: ownerID,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Capture,
				SchemaRef: SchemaSealValues,
				KindLabel: "seal:values",
				Payload:   body,
			}},
		}},
	})
	return err
}

// FailedSeals returns a snapshot of the manager's failed-seal handles, sorted
// by artifact id. A handle is omitted once the host discards it or a later
// seal cites the same artifact.
func (m *ScopeManager) FailedSeals() []*FailedSeal {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*FailedSeal, 0, len(m.failedSeals))
	for _, h := range m.failedSeals {
		cp := *h
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ArtifactID < out[j].ArtifactID })
	return out
}

// FailedSeal looks up a handle by its id or artifact id.
func (m *ScopeManager) FailedSeal(id string) (*FailedSeal, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.failedSeals[id]
	if !ok {
		for _, candidate := range m.failedSeals {
			if candidate.ArtifactID == id {
				h, ok = candidate, true
				break
			}
		}
	}
	if !ok {
		return nil, false
	}
	cp := *h
	return &cp, true
}

// DiscardFailedSeal records the host's terminal decision for an artifact and
// deletes the blobs it owns. It never touches the sandbox: destroying a
// worktree or container is the host's call, not the runtime's.
func (m *ScopeManager) DiscardFailedSeal(ctx context.Context, id string) error {
	h, ok := m.FailedSeal(id)
	if !ok {
		return fmt.Errorf("failed-seal artifact %q not found", id)
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultFinalizationTimeout)
	defer cancel()
	_, err := m.store.Append(dctx, trace.TrustedAppendContext, trace.AppendBatch{
		AppendIntentID: fmt.Sprintf("%s:seal-artifact-discard:%s", h.OwnerID, h.ArtifactID),
		Groups: []trace.AppendGroup{{
			TraceOwnerID: h.OwnerID,
			FactDrafts: []trace.RecordDraft{{
				Mode:      trace.Capture,
				SchemaRef: SchemaSealArtifactDiscarded,
				KindLabel: "seal:artifact_discarded",
				Payload: map[string]any{
					"artifact_id": h.ArtifactID,
					"scope":       h.ScopeID,
					"attempt_id":  h.AttemptID,
				},
			}},
		}},
	})
	if err != nil {
		return fmt.Errorf("discard failed-seal artifact %s: %w", h.ArtifactID, err)
	}
	for _, b := range []struct{ kind, key string }{
		{transcriptBlob, h.AttemptID},
		{sealBlobDiff, h.OutputID},
		{sealBlobMetadata, h.OutputID},
		{sealBlobSnapshot, h.OutputID},
	} {
		if b.key == "" {
			continue
		}
		if err := trace.DeleteBlob(dctx, m.store, h.OwnerID, b.kind, b.key); err != nil {
			slog.Warn("seal: failed to remove artifact blob on discard", "artifact", h.ArtifactID, "kind", b.kind, "err", err)
		}
	}
	m.mu.Lock()
	delete(m.failedSeals, h.ID)
	m.mu.Unlock()
	return nil
}

// ForkFromFailedSeal seeds a fresh isolated child from a failed-seal artifact:
// the artifact's baseline becomes the child's baseline, its workspace state
// the seed, and its verified transcript the fork snapshot. The child carries
// the artifact id as derived_from lineage. Stale, unadopted, or
// transcript-invalid handles are refused — never silently reseeded.
func (m *ScopeManager) ForkFromFailedSeal(ctx context.Context, parentID, childOwnerID string, h *FailedSeal, sb sandbox.Sandbox) (*Scope, error) {
	if h == nil {
		return nil, errors.New("fork from failed seal: a handle is required")
	}
	if sb == nil {
		return nil, fmt.Errorf("fork from failed seal %s: an isolated sandbox is required", h.ArtifactID)
	}
	if h.Stale {
		return nil, fmt.Errorf("fork from failed seal %s: the worktree changed since the artifact was recorded", h.ArtifactID)
	}
	if h.TranscriptInvalid || h.Transcript == nil {
		return nil, fmt.Errorf("fork from failed seal %s: %w", h.ArtifactID, ErrSealTranscriptInvalid)
	}
	if h.Workspace.Backend == "" {
		return nil, fmt.Errorf("fork from failed seal %s: the artifact has no workspace state", h.ArtifactID)
	}
	opt := ForkOptions{
		Baseline:    h.Baseline,
		Seed:        h.Workspace,
		Snapshot:    h.Transcript,
		DerivedFrom: h.ArtifactID,
	}
	return m.forkScope(ctx, parentID, childOwnerID, nil, true, sb, []ForkOptions{opt})
}

// payloadStrings decodes a JSON array of strings from a scanned payload.
func payloadStrings(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// recoverFailedSeals rebuilds failed-seal handles from the durable trace. It
// is called after sandbox re-adoption so adoption and staleness can be
// computed against the live world. A handle is surfaced only when a later
// seal has not resolved the same artifact and the host has not discarded it.
func recoverFailedSeals(ctx context.Context, mgr *ScopeManager, store *trace.SQLiteTraceStore, quarantined map[string]bool) (transcriptCount, artifactCount, failedCount, interruptedCount, discardedCount int, err error) {
	transcripts, err := trace.ScanRecordsBySchema(ctx, store, SchemaAttemptTranscript)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan transcripts: %w", err)
	}
	artifacts, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealArtifact)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan artifacts: %w", err)
	}
	values, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealValues)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan values: %w", err)
	}
	failures, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealFailed)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan failures: %w", err)
	}
	discards, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealArtifactDiscarded)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan discards: %w", err)
	}
	sealed, err := trace.ScanRecordsBySchema(ctx, store, SchemaRunOutputSealed)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("recover failed seals: scan sealed outputs: %w", err)
	}

	type transcriptEntry struct {
		owner   string
		payload map[string]any
	}
	byTranscriptID := make(map[string]transcriptEntry, len(transcripts))
	for _, rec := range transcripts {
		byTranscriptID[rec.RecordID] = transcriptEntry{owner: rec.OwnerID, payload: rec.Payload}
		transcriptCount++
	}
	resolved := make(map[string]bool, len(sealed))
	for _, rec := range sealed {
		if id, _ := rec.Payload["artifact_id"].(string); id != "" {
			resolved[id] = true
		}
		// A reseed from a failed-seal handle cites the source artifact as
		// derived_from; it resolves the source just as a direct retry does.
		if id, _ := rec.Payload["derived_from"].(string); id != "" {
			resolved[id] = true
		}
	}
	discarded := make(map[string]bool, len(discards))
	for _, rec := range discards {
		if id, _ := rec.Payload["artifact_id"].(string); id != "" {
			discarded[id] = true
			discardedCount++
		}
	}
	failedBy := make(map[string]map[string]any, len(failures))
	for _, rec := range failures {
		if id, _ := rec.Payload["artifact_id"].(string); id != "" {
			failedBy[id] = rec.Payload
			failedCount++
		}
	}
	valuesBy := make(map[string][]map[string]any, len(values))
	for _, rec := range values {
		if id, _ := rec.Payload["artifact_id"].(string); id != "" {
			valuesBy[id] = append(valuesBy[id], rec.Payload)
		}
	}

	for _, rec := range artifacts {
		artifactCount++
		artifactID, _ := rec.Payload["artifact_id"].(string)
		if artifactID == "" || resolved[artifactID] || discarded[artifactID] {
			continue
		}
		ownerID, _ := rec.Payload["owner"].(string)
		if ownerID == "" {
			ownerID = rec.OwnerID
		}
		if quarantined[ownerID] {
			continue
		}
		scopeID, _ := rec.Payload["scope"].(string)
		h := &FailedSeal{
			ID:           artifactID,
			ArtifactID:   artifactID,
			OutputID:     strPayload(rec.Payload["output_id"]),
			ScopeID:      scopeID,
			OwnerID:      ownerID,
			AttemptID:    strPayload(rec.Payload["attempt_id"]),
			TranscriptID: strPayload(rec.Payload["transcript_id"]),
		}
		if entry, ok := byTranscriptID[h.TranscriptID]; ok {
			data, valid := readTranscript(ctx, store, entry.owner, entry.payload)
			if valid {
				h.Transcript = data
			} else {
				h.TranscriptInvalid = true
			}
		} else {
			h.TranscriptInvalid = true
		}
		if baseRaw, ok := rec.Payload["baseline"].(map[string]any); ok {
			h.Baseline = workspaceStateFromPayload(baseRaw)
		}

		hasState := false
		for _, v := range valuesBy[artifactID] {
			if stateRaw, ok := v["state"].(map[string]any); ok {
				h.Workspace = workspaceStateFromPayload(stateRaw)
				hasState = true
			}
			if changesRaw, ok := v["changes"].([]any); ok {
				for _, raw := range changesRaw {
					c, ok := raw.(map[string]any)
					if !ok {
						continue
					}
					h.Changes = append(h.Changes, ProposedChange{
						Path: strPayload(c["path"]),
						Kind: strPayload(c["kind"]),
					})
				}
			}
			if text, ok := v["diff"].(string); ok {
				h.Diff = text
			}
			for _, name := range payloadStrings(v["values"]) {
				if !containsString(h.Values, name) {
					h.Values = append(h.Values, name)
				}
			}
		}
		if fr, ok := failedBy[artifactID]; ok {
			h.Stage = strPayload(fr["stage"])
			h.Reason = strPayload(fr["reason"])
			for _, name := range payloadStrings(fr["values"]) {
				if !containsString(h.Values, name) {
					h.Values = append(h.Values, name)
				}
			}
		} else {
			h.Stage = "interrupted"
			h.Reason = "seal artifact has no terminal record"
			interruptedCount++
		}
		// Blob bytes are keyed by artifact id and named by the values list;
		// read them directly so a values-record failure cannot hide them.
		if containsString(h.Values, sealBlobDiff) {
			if data, _, getErr := trace.GetBlob(ctx, store, ownerID, sealBlobDiff, h.OutputID); getErr == nil {
				h.Diff = string(data)
			} else {
				slog.Warn("recovery: read failed-seal diff blob failed", "artifact", artifactID, "err", getErr)
			}
		}
		for _, kind := range []string{sealBlobMetadata, sealBlobSnapshot} {
			if !containsString(h.Values, kind) {
				continue
			}
			if data, _, getErr := trace.GetBlob(ctx, store, ownerID, kind, h.OutputID); getErr == nil {
				if kind == sealBlobMetadata {
					h.Metadata = data
				} else if len(h.Transcript) == 0 {
					// Legacy seal without a transcript journal: its snapshot
					// is the best conversation available, and is not verified.
					h.Transcript = data
					h.TranscriptInvalid = false
				}
			} else {
				slog.Warn("recovery: read failed-seal blob failed", "artifact", artifactID, "kind", kind, "err", getErr)
			}
		}

		// Adoption and staleness need the live sandbox; a resolver that
		// refuses re-adoption leaves the handle inspectable but unadopted.
		if scope, ok := mgr.Get(scopeID); ok {
			if sb := scope.Sandbox(); sb != nil {
				h.Adopted = true
				if captured, capErr := sb.Capture(ctx); capErr == nil {
					// Revision, not full digest: staleness means the
					// workspace timeline moved after the artifact, the
					// same rule checkpoints use on recovery. Non-position
					// data in a re-adopted sandbox is not drift.
					recordedRevision := h.Workspace.Revision
					if !hasState {
						h.Workspace = captured
					}
					if recordedRevision != "" && captured.Revision != recordedRevision {
						h.Stale = true
					}
				}
			}
		}
		mgr.mu.Lock()
		mgr.failedSeals[artifactID] = h
		mgr.mu.Unlock()
	}
	return transcriptCount, artifactCount, failedCount, interruptedCount, discardedCount, nil
}

// strPayload decodes one string from a scanned payload.
func strPayload(v any) string {
	s, _ := v.(string)
	return s
}

// loadTranscript reads one transcript fact by id. Seals that cite a journal
// use it to keep the in-memory Snapshot() accessor honest without writing a
// second copy of the conversation.
func loadTranscript(ctx context.Context, store *trace.SQLiteTraceStore, ownerID, transcriptID string) []byte {
	if transcriptID == "" {
		return nil
	}
	records, err := trace.ScanRecordsBySchema(ctx, store, SchemaAttemptTranscript)
	if err != nil {
		slog.Warn("seal: scan attempt transcripts failed", "owner", ownerID, "err", err)
		return nil
	}
	for _, rec := range records {
		if rec.RecordID == transcriptID {
			data, ok := readTranscript(ctx, store, rec.OwnerID, rec.Payload)
			if !ok {
				slog.Warn("seal: cited transcript failed verification", "transcript", transcriptID)
			}
			return data
		}
	}
	return nil
}

// recoverOutputTranscripts restores Snapshot() on outputs whose seal cited a
// durable transcript instead of duplicating the conversation as a snapshot.
func recoverOutputTranscripts(ctx context.Context, mgr *ScopeManager, store *trace.SQLiteTraceStore) error {
	records, err := trace.ScanRecordsBySchema(ctx, store, SchemaAttemptTranscript)
	if err != nil {
		return fmt.Errorf("recover output transcripts: scan: %w", err)
	}
	if len(records) == 0 {
		return nil
	}
	byID := make(map[string]trace.ScannedRecord, len(records))
	for _, rec := range records {
		byID[rec.RecordID] = rec
	}
	mgr.mu.RLock()
	outputs := make([]*RetainedOutput, 0, len(mgr.outputs))
	for _, out := range mgr.outputs {
		outputs = append(outputs, out)
	}
	mgr.mu.RUnlock()
	for _, out := range outputs {
		if out.transcriptID == "" {
			continue
		}
		rec, ok := byID[out.transcriptID]
		if !ok {
			slog.Warn("recovery: sealed output cites a missing transcript", "output", out.id, "transcript", out.transcriptID)
			continue
		}
		data, valid := readTranscript(ctx, store, rec.OwnerID, rec.Payload)
		if !valid {
			slog.Warn("recovery: sealed output transcript failed verification", "output", out.id, "transcript", out.transcriptID)
			continue
		}
		out.snapshot = data
	}
	return nil
}
