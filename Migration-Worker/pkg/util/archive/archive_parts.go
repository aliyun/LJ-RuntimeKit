/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package archive

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/docker/docker/pkg/idtools"
	"github.com/docker/docker/pkg/pools"
	"github.com/docker/docker/pkg/system"
	"k8s.io/klog/v2"
)

// ============================================================================
// Sharded parallel extract
//
// The checkpoint is split into N self-contained tars; the restore side extracts
// them into the **same** target dir with N concurrent workers. All differences
// between UnpackPart and Unpack stem from "multiple writers on one dir tree":
//
//  1. Never clean the target dir. The caller cleans once before launching the N
//     workers (see restoreIntoTemp in Migration-Worker/pkg/storage/util.go).
//  2. Tolerate EEXIST on dir creation. All N workers carry the full dir
//     skeleton, so the same dir is necessarily created repeatedly; the original
//     Lstat-then-Mkdir has a TOCTOU window.
//  3. Never RemoveAll an existing path. The original Unpack deletes then
//     recreates on name collision - fatal under N-way concurrency: it would both
//     delete a slice range another worker just wrote and make another worker's
//     chown/chmod/Chtimes hit ENOENT (skeleton entries exist in every shard;
//     writing the same path repeatedly is normal, not exceptional). If it
//     exists, someone else already wrote the same content - skip.
//  4. Recognize XDM.split.* records and use WriteAt to write the range at the
//     target file's corresponding offset.
//
// Known trade-off: **dir mtime cannot be preserved**. Standard untar batches
// dir Chtimes to apply at the end, but under concurrency worker A sets a dir's
// mtime and then worker B writes a file into it, changing the mtime again.
// Neither CRIU restore nor container runtime is affected.
// ============================================================================

const (
	// paxSplitOffset / paxSplitTotal mark that an entry carries only a byte range of the original file.
	//
	// Must match the constants in the node-side XDM agent's checkpoint part layout.
	paxSplitOffset = "XDM.split.offset"
	paxSplitTotal  = "XDM.split.total"

	// splitCopyBufSize is the buffer size for writing slice ranges into the target file.
	splitCopyBufSize = 1024 * 1024
)

// SplitFileInfo describes a file that was split across multiple shards.
//
// The metadata of such a file cannot be settled while unpacking a single part:
// whichever worker finishes writing data bumps mtime to now, overriding the one
// set earlier by another worker. So here we only collect the info; the caller
// applies it once after all N workers finish (see ApplySplitFileMetadata).
type SplitFileInfo struct {
	Path string
	Hdr  *tar.Header
}

// SplitPAXOffset parses the slice offset from a header. ok=false means this is a plain entry.
func SplitPAXOffset(hdr *tar.Header) (offset int64, ok bool) {
	if hdr.PAXRecords == nil {
		return 0, false
	}
	raw, exists := hdr.PAXRecords[paxSplitOffset]
	if !exists {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// UnpackPart extracts one checkpoint shard into dest; it may be called concurrently with other shards.
//
// It does **not** clean dest. The return value lists split files seen in this
// shard, for the caller to fix metadata once after all shards finish.
func UnpackPart(ctx context.Context, decompressedArchive io.Reader, dest string, options *TarOptions) ([]SplitFileInfo, error) {
	if options == nil {
		options = &TarOptions{}
	}

	tr := tar.NewReader(decompressedArchive)
	trBuf := pools.BufioReader32KPool.Get(nil)
	defer pools.BufioReader32KPool.Put(trBuf)

	idMapping := idtools.NewIDMappingsFromMaps(options.UIDMaps, options.GIDMaps)
	rootIDs := idMapping.RootPair()
	whiteoutConverter := getWhiteoutConverter(options.WhiteoutFormat, options.InUserNS)

	var dirs []*tar.Header
	var splits []SplitFileInfo

loop:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("untar canceled")
		default:
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break loop
		}
		if err != nil {
			return nil, err
		}

		hdr.Name = filepath.Clean(hdr.Name)

		for _, exclude := range options.ExcludePatterns {
			if strings.HasPrefix(hdr.Name, exclude) {
				continue loop
			}
		}

		// The skeleton should guarantee the parent dir exists, but "skeleton precedes
		// data" is only a generator-side convention; still guard here - the cost is one
		// Lstat.
		if !strings.HasSuffix(hdr.Name, string(os.PathSeparator)) {
			parentPath := filepath.Join(dest, filepath.Dir(hdr.Name))
			if _, err := os.Lstat(parentPath); err != nil && os.IsNotExist(err) {
				if err := idtools.MkdirAllAndChownNew(parentPath, 0777, rootIDs); err != nil && !os.IsExist(err) {
					return nil, err
				}
			}
		}

		path := filepath.Join(dest, hdr.Name)
		rel, err := filepath.Rel(dest, path)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return nil, breakoutError(fmt.Errorf("%q is outside of %q", hdr.Name, dest))
		}

		if err := remapIDs(idMapping, hdr); err != nil {
			return nil, err
		}

		// Split entries take a separate path: never touch an existing file (another
		// shard may have just written a different range); only write the range at the
		// given offset.
		if splitOffset, isSplit := SplitPAXOffset(hdr); isSplit {
			if err := writeSplitChunk(path, hdr, tr, splitOffset); err != nil {
				return nil, err
			}
			splits = append(splits, SplitFileInfo{Path: path, Hdr: hdr})
			continue loop
		}

		// Non-split entry. Here we must **never** follow the original Unpack's
		// "delete then recreate when it exists": the skeleton (dirs, symlinks, device
		// nodes, FIFOs, and size==0 regular files) has an identical entry in every
		// shard, so the N workers necessarily write the same path repeatedly. If one
		// worker RemoveAlls a file another just created, the latter's chown/chmod/
		// Chtimes hits ENOENT - seen in the field as
		//   chmod .../rootfs/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.580.105.08:
		//   no such file or directory
		// (the 0-byte placeholder file nvidia-container-cli reserves for bind mount, which happens to be in the skeleton).
		//
		// The target dir is clean before the N workers start (restoreIntoTemp only
		// pre-creates a memory/ sentinel), so "path exists" has exactly one source:
		// another shard already wrote the same skeleton entry. The headers carried by
		// each shard are byte-identical, so the first writer's result is final - skip.
		if fi, err := os.Lstat(path); err == nil {
			if options.NoOverwriteDirNonDir && fi.IsDir() && hdr.Typeflag != tar.TypeDir {
				return nil, fmt.Errorf("cannot overwrite directory %q with non-directory %q", path, dest)
			}
			if options.NoOverwriteDirNonDir && !fi.IsDir() && hdr.Typeflag == tar.TypeDir {
				return nil, fmt.Errorf("cannot overwrite non-directory %q with directory %q", path, dest)
			}
			if fi.IsDir() && hdr.Name == "." {
				continue loop
			}
			switch {
			case fi.IsDir() && hdr.Typeflag == tar.TypeDir:
				// Dir merge: keep going so createTarFile idempotently re-applies mode/owner.
				// This also covers the memory/ sentinel pre-created by restoreIntoTemp; its
				// attributes get corrected by the real header from the checkpoint.
			case !fi.IsDir() && hdr.Typeflag != tar.TypeDir:
				klog.V(5).Infof("untar part: %s already extracted by another part, skip", path)
				continue loop
			default:
				// Type mismatch (dir vs file). Shards from the same checkpoint cannot produce
				// this conflict; keep it as a guard.
				if err := os.RemoveAll(path); err != nil {
					return nil, err
				}
			}
		}

		trBuf.Reset(tr)

		if whiteoutConverter != nil {
			writeFile, err := whiteoutConverter.ConvertRead(hdr, path)
			if err != nil {
				return nil, err
			}
			if !writeFile {
				continue loop
			}
		}

		if err := createTarFile(path, dest, hdr, trBuf, !options.NoLchown, options.ChownOpts, options.InUserNS); err != nil {
			// The Lstat above only narrows the window, it cannot close it: two workers may
			// both see "not exists" and both create, and the loser gets EEXIST on Mkdir /
			// Symlink / Mknod. Skeleton entries are identical across shards, so what the
			// other worker created is the desired result. (Regular files use O_CREATE
			// without O_EXCL, so they never report EEXIST anyway.)
			if os.IsExist(err) {
				klog.V(5).Infof("untar part: %s already created by another part", path)
			} else {
				return nil, err
			}
		}

		if hdr.Typeflag == tar.TypeDir {
			dirs = append(dirs, hdr)
		}
	}

	// Dir mtime: still do it here so the degenerate "single shard" case behaves
	// like the single-tar path. It is unreliable under N-way concurrency (see the
	// trade-off note at the file head), so failure only logs and never aborts the
	// extract.
	for _, hdr := range dirs {
		p := filepath.Join(dest, hdr.Name)
		if err := system.Chtimes(p, hdr.AccessTime, hdr.ModTime); err != nil {
			klog.V(5).Infof("untar part: cannot set mtime of %s: %v", p, err)
		}
	}

	return splits, nil
}

// ApplySplitFileMetadata fixes the metadata of split files once after all shards finish.
//
// Must be called after the N-way barrier. Every write to a file makes the
// kernel reset its metadata: mtime bumps to now, and file_remove_privs also
// clears setuid/setgid and security.capability. This shard finishing does not
// mean the file is done - other shards are still pwriting other ranges - so any
// attribute set inside a shard would be wiped by whichever worker finishes
// later. mtime was already moved here for this reason; setuid and xattr are the
// same story and must come together.
//
// Order matters:
//  1. Lchown - chown itself clears setuid/setgid, so it must precede Chmod.
//     createTarFile reached the same conclusion for non-split paths
//     that can modify the file mode"）。
//  2. Chmod - restore setuid/setgid.
//  3. Lsetxattr - restore security.capability etc. Split entries go straight to
//     writeSplitChunk and continue inside UnpackPart, bypassing createTarFile's
//     xattr handling entirely, so this is the **only** place where split files
//     get xattrs restored.
//  4. Chtimes - last; it touches neither mode nor xattr.
//
// The same file is reported once by each of its shards; repeated application is
// idempotent (headers are identical across shards, Lsetxattr overwrites the
// same key). The entry count is bounded by the shard count, not worth a dedup
// state.
//
// Also do a length check: the union of ranges should exactly cover the original
// file length. A mismatch means a shard is missing or truncated - must error,
// otherwise the container would run with an incomplete large file.
func ApplySplitFileMetadata(splits []SplitFileInfo) error {
	var xattrWarnings []string

	for _, s := range splits {
		if size, ok := splitTotalSize(s.Hdr); ok {
			fi, err := os.Stat(s.Path)
			if err != nil {
				return fmt.Errorf("stat split file %s: %w", s.Path, err)
			}
			if fi.Size() != size {
				return fmt.Errorf("split file %s is %d bytes but should be %d: "+
					"some checkpoint part is missing or truncated", s.Path, fi.Size(), size)
			}
		}

		// Non-root chown inevitably fails; keep the same behavior as the single-tar path: warn only.
		if err := os.Lchown(s.Path, s.Hdr.Uid, s.Hdr.Gid); err != nil {
			klog.V(5).Infof("untar part: cannot chown split file %s: %v", s.Path, err)
		}
		if err := os.Chmod(s.Path, s.Hdr.FileInfo().Mode()); err != nil {
			return fmt.Errorf("chmod split file %s: %w", s.Path, err)
		}
		// hdr.Xattrs is auto-filled by archive/tar from SCHILY.xattr.* PAX records,
		// the same field createTarFile reads, so the two paths stay consistent.
		for key, value := range s.Hdr.Xattrs {
			if err := system.Lsetxattr(s.Path, key, []byte(value), 0); err != nil {
				// Same degradation as createTarFile: filesystems without xattr support
				// (ENOTSUP) or insufficient privilege (EPERM under userns) only count, other
				// errors must surface.
				if err == syscall.ENOTSUP || err == syscall.EPERM {
					xattrWarnings = append(xattrWarnings,
						fmt.Sprintf("%s [%s]: %v", s.Path, key, err))
					continue
				}
				return fmt.Errorf("set xattr %s of split file %s: %w", key, s.Path, err)
			}
		}
		if err := system.Chtimes(s.Path, s.Hdr.AccessTime, s.Hdr.ModTime); err != nil {
			return fmt.Errorf("set mtime of split file %s: %w", s.Path, err)
		}
	}

	if len(xattrWarnings) > 0 {
		klog.Warningf("untar part: ignored xattrs on split files, "+
			"the underlying filesystem may not support them: %v", xattrWarnings)
	}
	return nil
}

func splitTotalSize(hdr *tar.Header) (int64, bool) {
	if hdr.PAXRecords == nil {
		return 0, false
	}
	raw, ok := hdr.PAXRecords[paxSplitTotal]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// writeSplitChunk writes one byte range at the given offset of the target file.
//
// Use O_CREATE without O_TRUNC: the N workers each write their range; whoever
// arrives first creates the file, later ones must keep existing content. WriteAt
// past EOF auto-extends the file into a sparse file, so neither pre-Truncate nor
// an extra full read/write "stitch" round is needed. Ranges do not overlap, so
// concurrent pwrite on the same fd needs no lock.
func writeSplitChunk(path string, hdr *tar.Header, r io.Reader, offset int64) error {
	hdrInfo := hdr.FileInfo()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, hdrInfo.Mode())
	if err != nil {
		return fmt.Errorf("open split target %s: %w", path, err)
	}
	defer f.Close()

	buf := make([]byte, splitCopyBufSize)
	written := int64(0)
	at := offset
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if _, err := f.WriteAt(buf[:n], at); err != nil {
				return fmt.Errorf("write %s at offset %d: %w", path, at, err)
			}
			at += int64(n)
			written += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read split chunk of %s: %w", path, readErr)
		}
	}
	if written != hdr.Size {
		return fmt.Errorf("split chunk of %s at offset %d: got %d bytes, header declares %d",
			path, offset, written, hdr.Size)
	}

	// mode / owner / xattr / mtime are not set here at all; all go through
	// ApplySplitFileMetadata after the barrier: this shard finishing data does not
	// mean the file is done, other shards will still pwrite other ranges, and every
	// write clears setuid/setgid and security.capability and bumps mtime. O_CREATE
	// with hdrInfo.Mode() only makes the newborn file close to the target perms;
	// the final value is decided by ApplySplitFileMetadata.
	return nil
}
