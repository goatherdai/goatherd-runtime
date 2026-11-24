package goatherd

import (
	"context"
	"testing"

	trace "github.com/goatherdai/goatherd-kernel"
	git "github.com/goatherdai/goatherd-runtime/sandbox/git"
)

// TestFailedForkLeavesNoGhostRecord pins bead shepherd-runtime-go-2jd: a
// parent that changes state after the fork baseline was captured must fail
// the fork before any durable fork fact is written, so recovery cannot
// resurrect a child that never existed in-process.
func TestFailedForkLeavesNoGhostRecord(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, err := mgr.Create("ghost-parent", git.NewLocalGitSandbox(repo))
	if err != nil {
		t.Fatal(err)
	}

	previous := forkTestHook
	t.Cleanup(func() { forkTestHook = previous })
	forkTestHook = func() {
		if err := parent.Halt(); err != nil {
			t.Errorf("halt parent from fork hook: %v", err)
		}
	}

	if _, err := mgr.Fork(parent.ID(), "ghost-child", nil); err == nil {
		t.Fatal("fork succeeded although the parent halted during it")
	}
	if state := parent.State(); state != ScopeDiscarded {
		t.Fatalf("parent state = %s, want discarded", state)
	}

	forks, err := trace.ScanRecordsBySchema(ctx, store, SchemaScopeForked)
	if err != nil {
		t.Fatal(err)
	}
	if len(forks) != 0 {
		t.Fatalf("failed fork wrote %d durable fork records, want 0", len(forks))
	}
	if _, ok := mgr.Get("scope:ghost-child"); ok {
		t.Fatal("failed fork registered a child in-process")
	}

	recovered, err := RecoverScopes(store, RecoverOptions{})
	if err != nil {
		t.Fatalf("RecoverScopes: %v", err)
	}
	if _, ok := recovered.Get("scope:ghost-child"); ok {
		t.Fatal("recovery resurrected a child from a failed fork")
	}
	halts, err := trace.ScanRecordsBySchema(ctx, store, SchemaSupervisorHalt)
	if err != nil {
		t.Fatal(err)
	}
	if len(halts) != 1 {
		t.Fatalf("halt records = %d, want 1", len(halts))
	}
}

// TestSuccessfulForkStillRecordsForkPoint guards the reordered path: a normal
// fork must still write its fact and be recoverable as an active child.
func TestSuccessfulForkStillRecordsForkPoint(t *testing.T) {
	store := newMemStore(t)
	mgr := NewScopeManager(store)
	repo := newTestRepo(t)
	parent, err := mgr.Create("good-fork-parent", git.NewLocalGitSandbox(repo))
	if err != nil {
		t.Fatal(err)
	}
	child, err := mgr.Fork(parent.ID(), "good-fork-child", nil)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if child.ForkPoint() == "" {
		t.Fatal("successful fork has no fork point")
	}
	forks, err := trace.ScanRecordsBySchema(context.Background(), store, SchemaScopeForked)
	if err != nil {
		t.Fatal(err)
	}
	if len(forks) != 1 || strPayload(forks[0].Payload["child_owner"]) != "good-fork-child" {
		t.Fatalf("fork records = %+v, want one naming good-fork-child", forks)
	}
	recovered, err := RecoverScopes(store, RecoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := recovered.Get("scope:good-fork-child"); !ok || got.ForkPoint() == "" {
		t.Errorf("recovered child = %+v, ok %v; want a fork point", got, ok)
	}
}
