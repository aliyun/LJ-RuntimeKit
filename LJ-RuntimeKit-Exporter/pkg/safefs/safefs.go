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

// Package safefs walks and writes directory trees relative to directory
// descriptors without following symlinks, so a hostile mount cannot redirect I/O.
package safefs

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const dirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

var ErrNotRegular = errors.New("not a regular file")

// OpenDir opens path as a directory, refusing a symlink as the final component.
func OpenDir(path string) (*os.File, error) {
	fd, err := unix.Open(path, dirFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// OpenDirAt opens the directory name inside dir without following symlinks.
func OpenDirAt(dir *os.File, name string) (*os.File, error) {
	path := filepath.Join(dir.Name(), name)
	fd, err := unix.Openat(int(dir.Fd()), name, dirFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// MkdirAllAt creates and opens dir/parts[0]/parts[1]/..., refusing symlinks
// at every level. The caller owns dir and the returned descriptor.
func MkdirAllAt(dir *os.File, parts []string, perm uint32) (*os.File, error) {
	cur := dir
	for _, part := range parts {
		if err := unix.Mkdirat(int(cur.Fd()), part, perm); err != nil && err != unix.EEXIST {
			if cur != dir {
				cur.Close()
			}
			return nil, &os.PathError{Op: "mkdirat", Path: filepath.Join(cur.Name(), part), Err: err}
		}
		next, err := OpenDirAt(cur, part)
		if cur != dir {
			cur.Close()
		}
		if err != nil {
			return nil, err
		}
		cur = next
	}
	if cur == dir {
		return OpenDirAt(dir, ".")
	}
	return cur, nil
}

// CreateExclAt creates a new file name in dir with O_EXCL|O_NOFOLLOW.
func CreateExclAt(dir *os.File, name string, perm uint32) (*os.File, error) {
	path := filepath.Join(dir.Name(), name)
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, perm)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// OpenFileAt opens a regular file in dir for reading without following symlinks.
func OpenFileAt(dir *os.File, name string) (*os.File, error) {
	path := filepath.Join(dir.Name(), name)
	// O_NONBLOCK keeps a FIFO swapped in after listing from blocking the open.
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: path, Err: err}
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, &os.PathError{Op: "fstat", Path: path, Err: err}
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		unix.Close(fd)
		return nil, &os.PathError{Op: "openat", Path: path, Err: ErrNotRegular}
	}
	if err := unix.SetNonblock(fd, false); err != nil {
		unix.Close(fd)
		return nil, &os.PathError{Op: "fcntl", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// RenameAt renames oldName to newName within dir.
func RenameAt(dir *os.File, oldName, newName string) error {
	if err := unix.Renameat(int(dir.Fd()), oldName, int(dir.Fd()), newName); err != nil {
		return &os.LinkError{Op: "renameat", Old: filepath.Join(dir.Name(), oldName), New: filepath.Join(dir.Name(), newName), Err: err}
	}
	return nil
}

// UnlinkAt removes the file name from dir.
func UnlinkAt(dir *os.File, name string) error {
	if err := unix.Unlinkat(int(dir.Fd()), name, 0); err != nil {
		return &os.PathError{Op: "unlinkat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return nil
}
