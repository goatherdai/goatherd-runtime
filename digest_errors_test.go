package goatherd

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	trace "github.com/goatherdai/goatherd-kernel"
	sandbox "github.com/goatherdai/goatherd-runtime/sandbox"
	git "github.com/goatherdai/goatherd-runtime/sandbox/git"
)

// badDigestSandbox overrides Capture with a workspace whose Data cannot be
// canonicalized, forcing WorkspaceState.Digest to fail.
type badDigestSandbox struct{ sandbox.Sandbox }

func (b badDigestSandbox) Capture(context.Context) (trace.WorkspaceState, error) {
	return trace.WorkspaceState{
		Backend:  "git",
		Revision: "unencodable",
		Data:     map[string]any{"channel": make(chan int)},
	}, nil
}

func badDigestState() trace.WorkspaceState {
	return trace.WorkspaceState{
		Backend:  "git",
		Revision: "unencodable",
		Data:     map[string]any{"channel": make(chan int)},
	}
}

// TestCreateCheckpoint_DigestFailureIsHard pins the durable checkpoint path:
// a workspace whose digest cannot be computed must fail the checkpoint, not
// record an empty state_digest.
func TestCreateCheckpoint_DigestFailureIsHard(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	scope, err := mgr.Create("digest-cp-root", badDigestSandbox{git.NewLocalGitSandbox(repo)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.CreateCheckpoint(ctx, nil); err == nil {
		t.Fatal("CreateCheckpoint succeeded with an unencodable workspace")
	}
	records, err := trace.ScanRecordsBySchema(ctx, store, SchemaCheckpointCreated)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("checkpoint facts = %d, want 0", len(records))
	}
}

// TestSeal_DigestFailureIsTypedAndNoSealedFact pins the seal's hard append
// path: a failed digest yields a typed SealError and a failed-seal artifact,
// never a sealed fact with an empty digest.
func TestSeal_DigestFailureIsTypedAndNoSealedFact(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, _ := mgr.Create("digest-seal-root", git.NewLocalGitSandbox(repo))
	wtPath := filepath.Join(t.TempDir(), "wt")
	sb := git.NewWorktreeSandbox(repo, wtPath)
	if err := sb.Create(ctx, sandbox.SandboxSpec{}); err != nil {
		t.Fatal(err)
	}
	child, err := mgr.ForkIsolated(parent.ID(), "digest-seal-child", nil, badDigestSandbox{sb})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Seal(ctx, child.ID())
	var sealErr *SealError
	if !errors.As(err, &sealErr) {
		t.Fatalf("Seal error = %v, want *SealError", err)
	}
	if sealErr.Stage != "digest_state" || sealErr.ArtifactID == "" {
		t.Fatalf("SealError = %+v, want digest_state with an artifact", sealErr)
	}
	sealed, err := trace.ScanRecordsBySchema(ctx, store, SchemaRunOutputSealed)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 0 {
		t.Fatalf("sealed facts = %d, want 0", len(sealed))
	}
	failures, err := trace.ScanRecordsBySchema(ctx, store, SchemaSealFailed)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 {
		t.Fatalf("failed-seal facts = %d, want 1", len(failures))
	}
}

// TestFork_BadBaselineDigestLeavesNoRecord pins the fork's hard append path.
func TestFork_BadBaselineDigestLeavesNoRecord(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, _ := mgr.Create("digest-fork-root", git.NewLocalGitSandbox(repo))
	if _, err := mgr.Fork(parent.ID(), "digest-fork-child", nil, ForkOptions{Baseline: badDigestState()}); err == nil {
		t.Fatal("Fork succeeded with an unencodable baseline")
	}
	forks, err := trace.ScanRecordsBySchema(ctx, store, SchemaScopeForked)
	if err != nil {
		t.Fatal(err)
	}
	if len(forks) != 0 {
		t.Fatalf("fork facts = %d, want 0", len(forks))
	}
}

// TestWorkspaceEvent_OmitsUndecodableDigest pins the advisory path: the event
// is still recorded, but without an empty state_digest.
func TestWorkspaceEvent_OmitsUndecodableDigest(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	scope := NewScope(store, "digest-adv-root")
	scope.recordWorkspaceEvent(ctx, scope.ownerID, trace.Capture, SchemaWorkspaceApplied, "workspace:applied", badDigestState())
	records, err := trace.ScanRecordsBySchema(ctx, store, SchemaWorkspaceApplied)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("workspace facts = %d, want 1", len(records))
	}
	if _, ok := records[0].Payload["state_digest"]; ok {
		t.Errorf("advisory record has an empty state_digest: %v", records[0].Payload["state_digest"])
	}
}
