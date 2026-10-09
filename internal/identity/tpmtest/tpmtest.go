// Package tpmtest runs a software TPM (swtpm) for tests. Tests that need a TPM
// call Start; without swtpm on PATH they are skipped, unless COLCA_TPM_TESTS is
// "require" (as in CI), where a missing simulator fails the test instead.
package tpmtest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// RequireEnv makes a missing swtpm a failure instead of a skip.
const RequireEnv = "COLCA_TPM_TESTS"

// Start launches swtpm on a Unix socket in a fresh state directory and returns
// the socket path, which tpmkey.Open accepts like a device. The simulator is
// stopped when the test ends.
func Start(t testing.TB) string {
	t.Helper()
	bin, err := exec.LookPath("swtpm")
	if err != nil {
		if os.Getenv(RequireEnv) == "require" {
			t.Fatalf("swtpm not found on PATH and %s=require", RequireEnv)
		}
		t.Skip("swtpm not on PATH; set " + RequireEnv + "=require to fail instead")
	}
	// Unix socket paths are short (104 bytes on macOS); t.TempDir can be longer.
	dir, err := os.MkdirTemp("", "swtpm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	cmd := exec.CommandContext(context.Background(), bin, "socket", "--tpm2", //nolint:gosec // test helper, fixed arguments
		"--tpmstate", "dir="+dir,
		"--server", "type=unixio,path="+sock,
		"--ctrl", "type=unixio,path="+filepath.Join(dir, "c"),
		"--flags", "not-need-init,startup-clear")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start swtpm: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fi, err := os.Stat(sock); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return sock
		}
		if time.Now().After(deadline) {
			t.Fatalf("swtpm socket %s did not appear", sock)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
