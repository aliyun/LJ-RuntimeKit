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

package collector

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/lj-runtimekit/exporter/pkg/safefs"
)

type outputHandle interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type outputFile struct {
	file     outputHandle
	dir      *os.File
	name     string
	relative string
	rename   func(*os.File, string, string) error
}

// openOutputDir walks the entire absolute path by directory descriptor. Neither
// creation nor subsequent writes may follow a symlink in the PVC subtree.
func openOutputDir(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) {
		return nil, fmt.Errorf("output directory must be an absolute subdirectory")
	}
	root, err := safefs.OpenDir(string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return safefs.MkdirAllAt(root, strings.Split(strings.TrimPrefix(filepath.Clean(path), string(filepath.Separator)), string(filepath.Separator)), 0o755)
}

const partialSuffix = ".partial"

// Bound each name so the combined basename fits the filesystem's 255-byte limit.
func filenameComponent(name string) string {
	return boundedFilename(name, 64)
}

func boundedFilename(name string, maxBytes int) string {
	if len(name) <= maxBytes {
		return name
	}
	hash := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%s-%x", name[:maxBytes-1-16], hash[:8])
}

func containerFilenameID(containerID string) string {
	id := NormalizeContainerID(containerID)
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func OutputFileName(podNamespace, podName, containerID, fileName string) string {
	return outputFileName(podNamespace, podName, containerID, fileName, 64, 255)
}

func outputFileName(podNamespace, podName, containerID, fileName string, componentBytes, maxBytes int) string {
	base := fmt.Sprintf("%s_%s_%s_", boundedFilename(podNamespace, componentBytes), boundedFilename(podName, componentBytes),
		boundedFilename(containerFilenameID(containerID), componentBytes))
	return base + boundedFilename(fileName, maxBytes-len(base)-len(partialSuffix))
}

var outputFileSuffix = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,11}_[A-Za-z0-9][A-Za-z0-9_.-]{0,254}(\.partial)?$`)

func removePreviousOutputs(dir *os.File, cfg Config) error {
	prefixes := []struct {
		name     string
		maxBytes int
	}{
		{filenameComponent(cfg.PodNamespace) + "_" + filenameComponent(cfg.PodName) + "_", 255},
		{boundedFilename(cfg.PodNamespace, 24) + "_" + boundedFilename(cfg.PodName, 24) + "_", 128},
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		for _, prefix := range prefixes {
			rest, ok := strings.CutPrefix(name, prefix.name)
			if !ok || len(strings.TrimSuffix(name, partialSuffix))+len(partialSuffix) > prefix.maxBytes || !outputFileSuffix.MatchString(rest) {
				continue
			}
			if err := safefs.UnlinkAt(dir, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			break
		}
	}
	return nil
}

func (c *Collector) createOutput(id, fileName string) (*outputFile, error) {
	dir, err := openOutputDir(c.cfg.OutputDir)
	if err != nil {
		return nil, err
	}
	output, err := c.createOutputAt(dir, id, fileName, safefs.CreateExclAt)
	if err != nil {
		dir.Close()
	}
	return output, err
}

func (c *Collector) createOutputAt(dir *os.File, id, fileName string, create func(*os.File, string, uint32) (*os.File, error)) (*outputFile, error) {
	if !streamFileName.MatchString(fileName) {
		return nil, fmt.Errorf("invalid stream file name %q", fileName)
	}
	name := OutputFileName(c.cfg.PodNamespace, c.cfg.PodName, id, fileName) + partialSuffix
	file, err := create(dir, name, 0o600)
	if errors.Is(err, syscall.ENAMETOOLONG) {
		// Encrypted filesystems can impose a smaller basename limit than 255 bytes.
		name = outputFileName(c.cfg.PodNamespace, c.cfg.PodName, id, fileName, 24, 128) + partialSuffix
		file, err = create(dir, name, 0o600)
	}
	if err != nil {
		return nil, err
	}
	return &outputFile{file: file, dir: dir, name: name, relative: name}, nil
}

func (c *Collector) createOutputWithRetry(ctx context.Context, id, fileName string) (*outputFile, error) {
	var dir *os.File
	err := c.cfg.retry(ctx, c.log, "OpenOutputDir", id, retryableFileError, func() error {
		var err error
		dir, err = openOutputDir(c.cfg.OutputDir)
		return err
	})
	if err != nil {
		return nil, err
	}
	// 文件创建独立计数，重试始终使用同一已安全打开的目录描述符。
	var output *outputFile
	err = c.cfg.retry(ctx, c.log, "CreateOutput", id, retryableFileError, func() error {
		var err error
		output, err = c.createOutputAt(dir, id, fileName, safefs.CreateExclAt)
		return err
	})
	if err != nil {
		dir.Close()
	}
	return output, err
}

func (f *outputFile) finalize() error {
	name := strings.TrimSuffix(f.name, partialSuffix)
	rename := f.rename
	if rename == nil {
		rename = safefs.RenameAt
	}
	if err := rename(f.dir, f.name, name); err != nil {
		return err
	}
	f.name = name
	f.relative = name
	return nil
}
