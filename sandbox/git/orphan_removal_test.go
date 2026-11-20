package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOrphanRemovalSmoke(t *testing.T) {
	dir := t.TempDir()
	repo := filepath.Join(dir, "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		cmd := execCommandForTest(repo, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	run("config", "user.email", "t@t")
	run("config", "user.name", "t")
	os.WriteFile(filepath.Join(repo, "README.md"), []byte("x"), 0o600)
	run("add", ".")
	run("commit", "-qm", "init")

	wt := filepath.Join(dir, "wt")
	run("worktree", "add", "--detach", wt, "HEAD")
	// Deregister: remove the admin dir, keep the checkout.
	data, err := os.ReadFile(filepath.Join(wt, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	gitdir := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(data)), "gitdir: "))
	os.RemoveAll(gitdir)
	run("worktree", "prune")

	sb := NewWorktreeSandbox(repo, wt)
	ctx := context.Background()

	registered, err := sb.IsRegisteredWorktree(ctx)
	if err != nil || registered {
		t.Fatalf("IsRegisteredWorktree on orphan = %v, %v; want false, nil", registered, err)
	}
	if err := sb.RemoveOrphanDeregistered(ctx); err != nil {
		t.Fatalf("RemoveOrphanDeregistered: %v", err)
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Errorf("worktree still exists: %v", err)
	}

	// Live worktree: IsRegisteredWorktree must be true, and removal via
	// the orphan path must refuse the main-worktree guard only for the
	// repo root itself.
	wt2 := filepath.Join(dir, "wt2")
	run("worktree", "add", "--detach", wt2, "HEAD")
	sb2 := NewWorktreeSandbox(repo, wt2)
	registered, err = sb2.IsRegisteredWorktree(ctx)
	if err != nil || !registered {
		t.Fatalf("IsRegisteredWorktree on live worktree = %v, %v; want true, nil", registered, err)
	}
	if err := sb2.RemoveOrphanDeregistered(ctx); err != nil {
		t.Fatalf("RemoveOrphanDeregistered on live worktree: %v", err)
	}
	if _, err := os.Stat(wt2); !os.IsNotExist(err) {
		t.Errorf("live worktree still exists: %v", err)
	}
}
