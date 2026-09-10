// colca-volume-init performs the bounded ownership migration required before
// Colca's long-running containers start as non-root users.
package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

const (
	uidEnv       = "COLCA_VOLUME_UID"
	gidEnv       = "COLCA_VOLUME_GID"
	tlsCertEnv   = "COLCA_TLS_CERT_SOURCE"
	tlsKeyEnv    = "COLCA_TLS_KEY_SOURCE"
	tlsTargetEnv = "COLCA_TLS_TARGET_DIR"
	copyTreeSrc  = "COLCA_COPY_TREE_SOURCE"
	copyTreeDst  = "COLCA_COPY_TREE_TARGET"
)

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "colca-volume-init: %v\n", err)
		os.Exit(1)
	}
}

func run(paths []string, getenv func(string) string) error {
	uid, err := positiveID(uidEnv, getenv(uidEnv))
	if err != nil {
		return err
	}
	gid, err := positiveID(gidEnv, getenv(gidEnv))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("at least one volume path is required")
	}

	treeSource := getenv(copyTreeSrc)
	treeTarget := getenv(copyTreeDst)
	if (treeSource == "") != (treeTarget == "") {
		return fmt.Errorf("%s and %s must be set together", copyTreeSrc, copyTreeDst)
	}
	if treeSource != "" {
		if err := copyTree(treeSource, treeTarget, uid, gid); err != nil {
			return fmt.Errorf("copy tree: %w", err)
		}
	}

	certSource := getenv(tlsCertEnv)
	keySource := getenv(tlsKeyEnv)
	targetDir := getenv(tlsTargetEnv)
	configured := certSource != "" || keySource != "" || targetDir != ""
	if configured && (certSource == "" || keySource == "" || targetDir == "") {
		return fmt.Errorf("%s, %s, and %s must be set together", tlsCertEnv, tlsKeyEnv, tlsTargetEnv)
	}
	if configured {
		if err := stageTLS(certSource, keySource, targetDir, uid, gid); err != nil {
			return fmt.Errorf("stage TLS material: %w", err)
		}
	}

	// Ownership LAST, and this order is load-bearing rather than tidy.
	//
	// This process runs as uid 0 with every capability dropped but CHOWN and
	// DAC_OVERRIDE. Root's power to chmod a file it does not own is CAP_FOWNER
	// specifically, so once a path belongs to `uid`, this process can no longer
	// change its mode. Chowning first therefore disarmed the copy that follows:
	// chownTree handed /volumes/keys to 65532, copyTree then chmod'd that same
	// directory, and the whole initializer died with
	// "chmod /volumes/keys: operation not permitted" — taking every service
	// that waits on it down with it.
	//
	// Copying while the tree is still root-owned costs nothing and needs no
	// extra capability. Widening to CAP_FOWNER would also have worked and is
	// the wrong trade: the fix is to stop chmod'ing what we have given away.
	for _, path := range paths {
		if err := chownTree(path, uid, gid); err != nil {
			return fmt.Errorf("migrate %s: %w", path, err)
		}
	}
	return nil
}

func copyTree(source, target string, uid, gid int) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink %s", path)
		}
		if entry.IsDir() {
			mode := info.Mode().Perm()
			if err := os.MkdirAll(destination, mode); err != nil {
				return err
			}
			if relative == "." {
				// The ROOT of the copy is a mount point that already exists,
				// and its permissions belong to the deployment that declared
				// it — not to the source. The source here is a directory on a
				// developer's machine whose mode is whatever their umask gave
				// it; the target is a named volume the image created 0750 and
				// owned by the runtime user. Copying 0755 over that is not a
				// correction, it is a downgrade, and it is the chmod that
				// killed this process: the mount point is already owned by
				// `uid`, and without CAP_FOWNER that call can only fail.
				//
				// Chowning it is chownTree's job — the mount point is one of
				// the paths named on the command line.
				return nil
			}
			// Below the root: MkdirAll applies the umask, so a directory this
			// call created may not have the mode asked for, and chmod settles
			// it. One that ALREADY has that mode is left alone, so a second
			// run over a tree now owned by `uid` changes nothing rather than
			// failing to change nothing.
			if err := chmodIfDifferent(destination, mode); err != nil {
				return err
			}
			return os.Chown(destination, uid, gid)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("refusing non-regular file %s", path)
		}
		return copyAtomic(path, destination, info.Mode().Perm(), uid, gid)
	})
}

// chmodIfDifferent sets a path's mode only when it does not already have it.
func chmodIfDifferent(path string, mode os.FileMode) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}
	if current.Mode().Perm() == mode {
		return nil
	}
	return os.Chmod(path, mode)
}

func positiveID(name, raw string) (int, error) {
	id, err := strconv.Atoi(raw)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%s must be a positive numeric ID", name)
	}
	return id, nil
}

func chownTree(dir string, uid, gid int) error {
	// Walking through os.Root keeps a symlink from leading out of the volume.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return fs.WalkDir(root.FS(), ".", func(path string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return root.Lchown(path, uid, gid)
	})
}

func stageTLS(certSource, keySource, targetDir string, uid, gid int) error {
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return err
	}
	if err := os.Chown(targetDir, uid, gid); err != nil {
		return err
	}
	if err := copyAtomic(certSource, filepath.Join(targetDir, "node.crt"), 0o644, uid, gid); err != nil {
		return err
	}
	return copyAtomic(keySource, filepath.Join(targetDir, "node.key"), 0o600, uid, gid)
}

func copyAtomic(source, target string, mode os.FileMode, uid, gid int) (returnErr error) {
	src, err := os.Open(source) //nolint:gosec // the file named in the container arguments
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	tmp, err := os.CreateTemp(filepath.Dir(target), ".colca-volume-init-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	if _, err := io.Copy(tmp, src); err != nil {
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if err := tmp.Chown(uid, gid); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}
