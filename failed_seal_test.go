package goatherd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	trace "github.com/goatherdai/goatherd-kernel"
	sandbox "github.com/goatherdai/goatherd-runtime/sandbox"
	git "github.com/goatherdai/goatherd-runtime/sandbox/git"
)

// injectSealFault fails a named finalization stage once, so tests can
// exercise a failed seal whose earlier blobs were already written.
func injectSealFault(t *testing.T, stage string) {
	t.Helper()
	previous := sealFault
	sealFault = func(got string) error {
		if got == stage {
			return errors.New("injected " + stage + " failure")
		}
		return nil
	}
	t.Cleanup(func() { sealFault = previous })
}

func TestPersistAttemptTranscript_IdempotentAndVerified(t *testing.T) {
	ctx := context.Background()
	mgr := NewScopeManager(newMemStore(t))
	data := []byte("the captured conversation")

	first, err := mgr.PersistAttemptTranscript(ctx, "attempt-idem", "scope:idem-root", data)
	if err != nil {
		t.Fatalf("PersistAttemptTranscript: %v", err)
	}
	second, err := mgr.PersistAttemptTranscript(ctx, "attempt-idem", "scope:idem-root", data)
	if err != nil {
		t.Fatalf("repeat PersistAttemptTranscript: %v", err)
	}
	if first != second {
		t.Fatalf("repeat returned %q, want %q", second, first)
	}
	records, err := trace.ScanRecordsBySchema(ctx, mgr.store, SchemaAttemptTranscript)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("transcript facts = %d, want 1 (idempotent replay)", len(records))
	}
	got, valid := readTranscript(ctx, mgr.store, records[0].OwnerID, records[0].Payload)
	if !valid || string(got) != string(data) {
		t.Fatalf("readTranscript = %q, %v; want %q, true", got, valid, data)
	}

	// A digest mismatch is reported, never silently accepted.
	corrupt := map[string]any{"digest": "sha256:wrong", "transcript": records[0].Payload["transcript"]}
	if _, valid := readTranscript(ctx, mgr.store, "idem-root", corrupt); valid {
		t.Fatal("readTranscript accepted a corrupt digest")
	}
}

func TestPersistAttemptTranscript_LargeValueRidesBlob(t *testing.T) {
	ctx := context.Background()
	mgr := NewScopeManager(newMemStore(t))
	data := []byte(strings.Repeat("C", sealInlineLimit+64))

	id, err := mgr.PersistAttemptTranscript(ctx, "attempt-large", "scope:large-root", data)
	if err != nil {
		t.Fatalf("PersistAttemptTranscript: %v", err)
	}
	records, err := trace.ScanRecordsBySchema(ctx, mgr.store, SchemaAttemptTranscript)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("transcript facts = %d, want 1", len(records))
	}
	got, valid := readTranscript(ctx, mgr.store, records[0].OwnerID, records[0].Payload)
	if !valid || string(got) != string(data) {
		t.Fatalf("large transcript did not round-trip (id %s, valid %v)", id, valid)
	}
}

func TestSeal_PreflightExpiredContextIsTyped(t *testing.T) {
	ctx := context.Background()
	mgr := NewScopeManager(newMemStore(t))
	repo := newTestRepo(t)
	parent, _ := mgr.Create("seal-preflight-root", git.NewLocalGitSandbox(repo))
	child, _ := forkWorktreeChild(t, mgr, parent.ID(), "seal-preflight-child", repo, "child")

	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	_, err := mgr.Seal(expired, child.ID(), SealOptions{
		Finalization: FinalizationOptions{Mode: FinalizationInherit},
	})
	var sealErr *SealError
	if !errors.As(err, &sealErr) {
		t.Fatalf("Seal error = %v, want *SealError", err)
	}
	if sealErr.Stage != "preflight" || sealErr.ArtifactID != "" {
		t.Errorf("SealError = %+v, want stage preflight and no artifact", sealErr)
	}
	if !errors.Is(err, ErrSealDeadlineBeforePersist) {
		t.Errorf("Seal error %v does not wrap ErrSealDeadlineBeforePersist", err)
	}
	artifacts, err := trace.ScanRecordsBySchema(ctx, mgr.store, SchemaSealArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("preflight failure wrote %d artifacts, want 0", len(artifacts))
	}
}

func TestSeal_DefaultFinalizationSurvivesExpiredAttempt(t *testing.T) {
	ctx := context.Background()
	mgr := NewScopeManager(newMemStore(t))
	repo := newTestRepo(t)
	parent, _ := mgr.Create("seal-detached-root", git.NewLocalGitSandbox(repo))
	child, wtPath := forkWorktreeChild(t, mgr, parent.ID(), "seal-detached-child", repo, "child")
	if err := os.WriteFile(filepath.Join(wtPath, "result.txt"), []byte("winner"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The attempt deadline expired during the body. Default finalization is
	// detached from it, so the seal still completes.
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	out, err := mgr.Seal(expired, child.ID())
	if err != nil {
		t.Fatalf("Seal with expired attempt ctx: %v", err)
	}
	if out == nil || len(out.Changes()) != 1 {
		t.Fatalf("sealed output = %+v, want one change", out)
	}
	sealed, err := trace.ScanRecordsBySchema(ctx, mgr.store, SchemaRunOutputSealed)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1 || strPayload(sealed[0].Payload["artifact_id"]) == "" {
		t.Fatalf("sealed facts = %d, want 1 citing an artifact", len(sealed))
	}
}

func TestFailedSeal_RetainsArtifactTranscriptAndIsRecoverable(t *testing.T) {
	ctx := context.Background()
	injectSealFault(t, "sealed_append")
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, _ := mgr.Create("seal-fail-root", git.NewLocalGitSandbox(repo))
	child, wtPath := forkWorktreeChild(t, mgr, parent.ID(), "seal-fail-child", repo, "child")
	// A diff over the inline limit forces a named blob that must survive the
	// failure — it is the recovery payload, not garbage to clean up.
	if err := os.WriteFile(filepath.Join(wtPath, "result.txt"), []byte(strings.Repeat("x", sealInlineLimit+64)), 0o644); err != nil {
		t.Fatal(err)
	}
	transcript := []byte(strings.Repeat("T", sealInlineLimit+64))
	transcriptID, err := mgr.PersistAttemptTranscript(ctx, "attempt-seal-fail", child.ID(), transcript)
	if err != nil {
		t.Fatalf("PersistAttemptTranscript: %v", err)
	}

	_, err = mgr.Seal(ctx, child.ID(), SealOptions{
		AttemptID:    "attempt-seal-fail",
		TranscriptID: transcriptID,
	})
	var sealErr *SealError
	if !errors.As(err, &sealErr) {
		t.Fatalf("Seal error = %v, want *SealError", err)
	}
	if sealErr.Stage != "sealed_append" || sealErr.ArtifactID == "" {
		t.Fatalf("SealError = %+v, want stage sealed_append with an artifact", sealErr)
	}
	if state := child.State(); state != ScopeActive {
		t.Errorf("scope state after failed seal = %s, want active (host owns teardown)", state)
	}
	sealed, err := trace.ScanRecordsBySchema(ctx, store, SchemaRunOutputSealed)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 0 {
		t.Fatalf("failed seal wrote %d sealed facts, want 0", len(sealed))
	}
	failures, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealFailed)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || strPayload(failures[0].Payload["stage"]) != "sealed_append" {
		t.Fatalf("failed-seal records = %+v, want one naming sealed_append", failures)
	}
	artifacts, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("artifact records = %d, want 1", len(artifacts))
	}
	outputID := strPayload(artifacts[0].Payload["output_id"])
	if outputID == "" {
		t.Fatal("artifact has no output id")
	}
	if data, _, err := trace.GetBlob(ctx, store, "seal-fail-child", sealBlobDiff, outputID); err != nil || len(data) == 0 {
		t.Fatalf("failed seal's diff blob = %d bytes, err %v; want it kept for recovery", len(data), err)
	}
	if data, _, err := trace.GetBlob(ctx, store, "seal-fail-child", transcriptBlob, "attempt-seal-fail"); err != nil || string(data) != string(transcript) {
		t.Fatalf("transcript blob = %q, err %v; want transcript preserved", data, err)
	}

	// Restart: recovery rebuilds a handle with workspace AND transcript.
	stats := &RecoveryStats{}
	recovered, err := RecoverScopes(store, RecoverOptions{
		Stats: stats,
		Resolver: func(scopeID, backend, path string) (sandbox.Sandbox, error) {
			if path == "" {
				return git.NewLocalGitSandbox(repo), nil
			}
			return git.NewWorktreeSandbox(repo, path), nil
		},
	})
	if err != nil {
		t.Fatalf("RecoverScopes: %v", err)
	}
	if stats.FailedSeals != 1 || stats.SealArtifacts != 1 || stats.TranscriptFacts != 1 {
		t.Errorf("recovery stats = %+v, want one artifact/failure/transcript", stats)
	}
	h, ok := recovered.FailedSeal(sealErr.ArtifactID)
	if !ok {
		t.Fatal("no failed-seal handle after recovery")
	}
	if string(h.Transcript) != string(transcript) || h.TranscriptInvalid {
		t.Errorf("handle transcript = %q, invalid %v; want %q and valid", h.Transcript, h.TranscriptInvalid, transcript)
	}
	if h.Stage != "sealed_append" || h.Reason == "" {
		t.Errorf("handle stage/reason = %q/%q", h.Stage, h.Reason)
	}
	if !h.Adopted || h.Stale {
		t.Errorf("handle adopted/stale = %v/%v, want true/false", h.Adopted, h.Stale)
	}
	if h.Workspace.Backend != "git" || len(h.Changes) != 1 || h.Changes[0].Path != "result.txt" {
		t.Errorf("handle workspace/changes = %+v / %+v", h.Workspace, h.Changes)
	}
	if !containsString(h.Values, sealBlobDiff) || h.Diff == "" {
		t.Errorf("handle values/diff = %v / %d bytes", h.Values, len(h.Diff))
	}
	if len(recovered.FailedSeals()) != 1 {
		t.Fatalf("FailedSeals() = %d handles, want 1", len(recovered.FailedSeals()))
	}

	// The handle can seed a fresh child, and that child seals normally.
	recoveredParent, ok := recovered.Get("scope:seal-fail-root")
	if !ok {
		t.Fatal("root scope missing after recovery")
	}
	seedPath := filepath.Join(t.TempDir(), "reseeded")
	seedSB := git.NewWorktreeSandbox(repo, seedPath)
	if err := seedSB.Create(ctx, sandbox.SandboxSpec{}); err != nil {
		t.Fatalf("seed worktree create: %v", err)
	}
	seeded, err := recovered.ForkFromFailedSeal(ctx, recoveredParent.ID(), "seal-fail-reseeded", h, seedSB)
	if err != nil {
		t.Fatalf("ForkFromFailedSeal: %v", err)
	}
	if seeded.DerivedFrom() != h.ArtifactID {
		t.Errorf("seeded derivedFrom = %q, want %q", seeded.DerivedFrom(), h.ArtifactID)
	}
	// The reseed must seal with the fault cleared: a retry is the recovery
	// path, not another failure.
	sealFault = nil
	sealedOut, err := recovered.Seal(ctx, seeded.ID(), SealOptions{TranscriptID: transcriptID})
	if err != nil {
		t.Fatalf("Seal of reseeded child: %v", err)
	}
	if len(sealedOut.Changes()) != 1 || string(sealedOut.Snapshot()) != string(transcript) {
		t.Errorf("reseeded output changes/snapshot = %+v / %q", sealedOut.Changes(), sealedOut.Snapshot())
	}

	// A successful retry resolves the artifact: no failed-seal handle is
	// surfaced for it anymore.
	if _, ok := recovered.FailedSeal(sealErr.ArtifactID); ok {
		t.Fatal("artifact still surfaced after a successful reseal")
	}
}

func TestDiscardFailedSeal_RemovesHandleAndBlobs(t *testing.T) {
	ctx := context.Background()
	injectSealFault(t, "sealed_append")
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, _ := mgr.Create("seal-discard-root", git.NewLocalGitSandbox(repo))
	child, wtPath := forkWorktreeChild(t, mgr, parent.ID(), "seal-discard-child", repo, "child")
	if err := os.WriteFile(filepath.Join(wtPath, "result.txt"), []byte(strings.Repeat("x", sealInlineLimit+64)), 0o644); err != nil {
		t.Fatal(err)
	}
	transcriptID, err := mgr.PersistAttemptTranscript(ctx, "attempt-discard", child.ID(), []byte(strings.Repeat("T", sealInlineLimit+64)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Seal(ctx, child.ID(), SealOptions{AttemptID: "attempt-discard", TranscriptID: transcriptID})
	var sealErr *SealError
	if !errors.As(err, &sealErr) {
		t.Fatalf("Seal error = %v, want *SealError", err)
	}
	recovered, err := RecoverScopes(store, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := recovered.FailedSeal(sealErr.ArtifactID)
	if !ok {
		t.Fatal("no handle to discard")
	}
	if err := recovered.DiscardFailedSeal(ctx, h.ID); err != nil {
		t.Fatalf("DiscardFailedSeal: %v", err)
	}
	if _, ok := recovered.FailedSeal(sealErr.ArtifactID); ok {
		t.Fatal("handle still present after discard")
	}
	if _, _, err := trace.GetBlob(ctx, store, "seal-discard-child", sealBlobDiff, h.OutputID); !errors.Is(err, trace.ErrBlobNotFound) {
		t.Errorf("diff blob after discard: %v, want ErrBlobNotFound", err)
	}
	if _, _, err := trace.GetBlob(ctx, store, "seal-discard-child", transcriptBlob, h.AttemptID); !errors.Is(err, trace.ErrBlobNotFound) {
		t.Errorf("transcript blob after discard: %v, want ErrBlobNotFound", err)
	}
}

func TestRecover_InterruptedArtifactAndInvalidTranscript(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	_, err := mgr.Create("interrupted-root", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordSealArtifact(ctx, store, "interrupted-root", "scope:interrupted-root",
		"art:interrupted:1", "out:interrupted-root:1", "attempt-interrupted", "missing-transcript",
		trace.WorkspaceState{Backend: "git", Revision: "abc"}, "", ""); err != nil {
		t.Fatalf("recordSealArtifact: %v", err)
	}
	stats := &RecoveryStats{}
	recovered, err := RecoverScopes(store, RecoverOptions{Stats: stats})
	if err != nil {
		t.Fatal(err)
	}
	if stats.InterruptedSeals != 1 || stats.SealArtifacts != 1 {
		t.Errorf("stats = %+v, want one interrupted artifact", stats)
	}
	h, ok := recovered.FailedSeal("art:interrupted:1")
	if !ok {
		t.Fatal("interrupted artifact not surfaced")
	}
	if h.Stage != "interrupted" || !h.TranscriptInvalid {
		t.Errorf("interrupted handle = stage %q, transcript invalid %v", h.Stage, h.TranscriptInvalid)
	}
	if _, err := recovered.ForkFromFailedSeal(ctx, "scope:interrupted-root", "should-not-exist", h, nil); err == nil {
		t.Fatal("ForkFromFailedSeal accepted an invalid transcript")
	}
}
