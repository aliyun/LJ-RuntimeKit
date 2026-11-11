// Package config generates NPD custom plugin monitor configuration.
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"golang.org/x/sys/unix"
)

const FileName = "rt-custom-plugin-monitor.json"

// Options describes paths as seen by the host NPD process.
type Options struct {
	Executable     string
	File           string
	Endpoint       string
	Timeout        time.Duration
	CacheFile      string
	CacheTTL       time.Duration
	MaxOutput      int
	InvokeInterval time.Duration
	PluginTimeout  time.Duration
	Concurrency    int
}

type pluginConfig struct {
	InvokeInterval string `json:"invoke_interval"`
	Timeout        string `json:"timeout"`
	MaxOutput      int    `json:"max_output_length"`
	Concurrency    int    `json:"concurrency"`
}

type condition struct {
	Type    string `json:"type"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type rule struct {
	Type      string   `json:"type"`
	Condition string   `json:"condition"`
	Reason    string   `json:"reason"`
	Path      string   `json:"path"`
	Args      []string `json:"args"`
}

type monitor struct {
	Plugin           string       `json:"plugin"`
	PluginConfig     pluginConfig `json:"pluginConfig"`
	Source           string       `json:"source"`
	MetricsReporting bool         `json:"metricsReporting"`
	Conditions       []condition  `json:"conditions"`
	Rules            []rule       `json:"rules"`
}

// Validate checks timing constraints and prevents conflicting output paths.
func (o Options) Validate() error {
	if o.File == "" {
		return errors.New("config-file must be an absolute path")
	}
	if err := o.ValidatePaths(); err != nil {
		return err
	}
	if o.Timeout <= 0 || o.PluginTimeout <= o.Timeout || o.InvokeInterval <= o.PluginTimeout {
		return errors.New("timing must satisfy 0 < timeout < plugin-timeout < invoke-interval")
	}
	if o.CacheTTL < 0 || (o.CacheFile != "" && o.CacheTTL >= o.InvokeInterval) {
		return errors.New("cache-ttl must be nonnegative and shorter than invoke-interval when caching is enabled")
	}
	if o.MaxOutput <= 0 || o.Concurrency <= 0 {
		return errors.New("max-output-length and concurrency must be positive")
	}
	return nil
}

func (o Options) ValidatePaths() error {
	for name, path := range map[string]string{"executable": o.Executable, "endpoint": o.Endpoint} {
		if !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
			return fmt.Errorf("%s must be an absolute path without NUL bytes", name)
		}
	}
	paths := []string{o.Executable, o.Endpoint}
	for name, path := range map[string]string{"config-file": o.File, "cache-file": o.CacheFile} {
		if path == "" {
			continue
		}
		if !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
			return fmt.Errorf("%s must be an absolute path without NUL bytes or empty", name)
		}
		paths = append(paths, path, path+".lock")
	}
	seen := make(map[string]bool)
	for _, path := range paths {
		path = filepath.Clean(path)
		if seen[path] {
			return errors.New("executable, endpoint, configuration, cache, and lock paths must be distinct")
		}
		seen[path] = true
	}
	return nil
}

// Render validates the complete catalog before producing deterministic JSON.
func Render(catalog *xdmgrpc.GetCategoriesResponse, o Options) ([]byte, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if len(catalog.GetCategories()) == 0 {
		return nil, errors.New("health category catalog is empty")
	}
	categories := append([]*xdmgrpc.CategoryInfo(nil), catalog.GetCategories()...)
	seenCategories, seenConditions := make(map[string]bool), make(map[string]bool)
	for _, c := range categories {
		if c == nil || strings.TrimSpace(c.Category) == "" || strings.TrimSpace(c.Condition) == "" ||
			strings.TrimSpace(c.OkReason) == "" || strings.TrimSpace(c.OkMessage) == "" || strings.TrimSpace(c.BadReason) == "" {
			return nil, errors.New("each category must provide category, condition, ok_reason, ok_message, and bad_reason")
		}
		if seenCategories[c.Category] || seenConditions[c.Condition] {
			return nil, fmt.Errorf("duplicate category or condition: %q, %q", c.Category, c.Condition)
		}
		seenCategories[c.Category], seenConditions[c.Condition] = true, true
	}
	sort.Slice(categories, func(i, j int) bool { return categories[i].Category < categories[j].Category })
	result := monitor{
		Plugin: "custom", Source: "rt-custom-plugin-monitor", MetricsReporting: true,
		PluginConfig: pluginConfig{
			InvokeInterval: o.InvokeInterval.String(), Timeout: o.PluginTimeout.String(),
			MaxOutput: o.MaxOutput, Concurrency: o.Concurrency,
		},
	}
	for _, c := range categories {
		result.Conditions = append(result.Conditions, condition{Type: c.Condition, Reason: c.OkReason, Message: c.OkMessage})
		result.Rules = append(result.Rules, rule{
			Type: "permanent", Condition: c.Condition, Reason: c.BadReason, Path: o.Executable,
			Args: []string{
				"--endpoint=" + o.Endpoint, "--category=" + c.Category,
				"--config-file=" + o.File, "--cache-file=" + o.CacheFile,
				"--timeout=" + o.Timeout.String(), "--cache-ttl=" + o.CacheTTL.String(),
				"--max-output-length=" + strconv.Itoa(o.MaxOutput),
				"--invoke-interval=" + o.InvokeInterval.String(), "--plugin-timeout=" + o.PluginTimeout.String(),
				"--concurrency=" + strconv.Itoa(o.Concurrency),
			},
		})
	}
	data, err := json.MarshalIndent(result, "", "  ")
	return append(data, '\n'), err
}

// Generator serializes writers and atomically publishes a complete configuration.
// The containing directory must only be writable by trusted administrators.
type Generator struct {
	Options Options
	Fetch   func(context.Context) (*xdmgrpc.GetCategoriesResponse, error)
}

// Generate refreshes an existing file only when overwrite is true.
func (g Generator) Generate(ctx context.Context, overwrite bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !overwrite {
		exists, err := regularFileExists(g.Options.File)
		if exists || err != nil {
			return err
		}
	}
	if err := g.Options.Validate(); err != nil {
		return err
	}
	if g.Fetch == nil {
		return errors.New("category fetch function is not configured")
	}
	if err := os.MkdirAll(filepath.Dir(g.Options.File), 0755); err != nil {
		return err
	}
	lock, err := lockFile(ctx, g.Options.File+".lock")
	if err != nil {
		return err
	}
	// Keep the lock inode stable across writers and process restarts.
	defer lock.Close()
	exists, err := regularFileExists(g.Options.File)
	if err != nil || (exists && !overwrite) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	catalog, err := g.Fetch(ctx)
	if err != nil {
		return err
	}
	data, err := Render(catalog, g.Options)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(g.Options.File), ".npd-config-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := file.Chmod(0644); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(file.Name(), g.Options.File)
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("configuration is not a regular file: %s", path)
	}
	return true, nil
}

func lockFile(ctx context.Context, path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			file.Close()
			return nil, err
		}
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
