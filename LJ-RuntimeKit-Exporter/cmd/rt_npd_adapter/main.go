package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/lj-runtimekit/exporter/pkg/exporter"
	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/adapter"
	npdconfig "github.com/lj-runtimekit/exporter/pkg/npdadapter/config"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/exitcode"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/output"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/xdmclient"
)

const (
	CLIEndpoint        = "endpoint"
	CLICategory        = "category"
	CLITimeout         = "timeout"
	CLIMaxOutputLength = "max-output-length"
	CLICacheFile       = "cache-file"
	CLICacheTTL        = "cache-ttl"
	CLIGenerateConfig  = "generate-config"
	CLIConfigFile      = "config-file"
	CLIInvokeInterval  = "invoke-interval"
	CLIPluginTimeout   = "plugin-timeout"
	CLIConcurrency     = "concurrency"
)

type Config struct {
	GenerateConfig bool
	Category       string
	Options        npdconfig.Options
}

func main() {
	// Release resources in run before os.Exit skips deferred calls.
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(parent context.Context, args []string, stdout, stderr io.Writer) int {
	app := newApp(stderr)
	code := exitcode.Healthy
	app.Action = func(c *cli.Context) error {
		config, err := contextToConfig(c)
		if err != nil {
			return err
		}
		// Keep NPD status codes separate from CLI errors and defer process exit to main.
		code = Run(c.Context, config, stdout, stderr)
		return nil
	}
	if err := app.RunContext(parent, append([]string{app.Name}, args...)); err != nil {
		log.New(stderr, "rt_npd_adapter: ", 0).Printf("invalid arguments: %v", err)
		return exitcode.Error
	}
	return code
}

func newApp(stderr io.Writer) *cli.App {
	defaultConfig, defaultCache := "", ""
	if executable, err := os.Executable(); err == nil {
		defaultConfig = filepath.Join(filepath.Dir(executable), npdconfig.FileName)
		defaultCache = filepath.Join(filepath.Dir(executable), "health.cache")
	}

	app := cli.NewApp()
	app.Name = "rt_npd_adapter"
	app.HelpName = app.Name
	app.Usage = "Report a health category to NPD or generate its monitor configuration"
	app.UsageText = "rt_npd_adapter --category CATEGORY [options]\n   rt_npd_adapter --generate-config [options]"
	app.Description = "Exit codes: 0 healthy, 1 unhealthy, 2 unknown (including unavailable health data), 3 error.\n   Only condition messages are written to stdout; help and diagnostics use stderr."
	app.HideHelpCommand = true
	app.HideVersion = true
	app.Writer, app.ErrWriter = stderr, stderr
	app.OnUsageError = func(_ *cli.Context, err error, _ bool) error {
		return err
	}
	app.Flags = []cli.Flag{
		// Health query and output.
		&cli.StringFlag{
			Name:    CLIEndpoint,
			Value:   "/run/amp/eKitC/xdm/sockets/dp2mgr.sock",
			Usage:   "Health service absolute Unix socket path or unix:///path",
			EnvVars: []string{"AMPERF_GRPC_PATH"},
		},
		&cli.StringFlag{
			Name:  CLICategory,
			Usage: "Health category to evaluate (required unless --generate-config is set)",
		},
		&cli.DurationFlag{
			Name:  CLITimeout,
			Value: 3 * time.Second,
			Usage: "Total budget for queries, locks, and missing configuration generation; must be positive",
		},
		&cli.IntFlag{
			Name:  CLIMaxOutputLength,
			Value: 160,
			Usage: "Positive stdout byte limit; also sets NPD max_output_length",
		},
		// Shared snapshot cache.
		&cli.StringFlag{
			Name:  CLICacheFile,
			Value: defaultCache,
			Usage: "Absolute shared cache path; an empty string disables caching",
		},
		&cli.DurationFlag{
			Name:  CLICacheTTL,
			Value: 10 * time.Second,
			Usage: "Nonnegative cache TTL; 0 disables caching; must be shorter than invoke-interval when generating configuration with caching enabled",
		},
		// NPD configuration generation.
		&cli.BoolFlag{
			Name:  CLIGenerateConfig,
			Usage: "Generate or refresh NPD configuration without running a health check",
		},
		&cli.StringFlag{
			Name:  CLIConfigFile,
			Value: defaultConfig,
			Usage: "Absolute NPD configuration path; an empty string disables automatic generation during health checks",
		},
		&cli.DurationFlag{
			Name:  CLIInvokeInterval,
			Value: 30 * time.Second,
			Usage: "Generated NPD invoke interval; must exceed plugin-timeout",
		},
		&cli.DurationFlag{
			Name:  CLIPluginTimeout,
			Value: 5 * time.Second,
			Usage: "Generated NPD process timeout; must exceed timeout",
		},
		&cli.IntFlag{
			Name:  CLIConcurrency,
			Value: 3,
			Usage: "Positive plugin concurrency for generated NPD configuration",
		},
	}
	return app
}

func contextToConfig(c *cli.Context) (*Config, error) {
	if c.Args().Present() {
		return nil, errors.New("positional arguments are not supported")
	}
	config := &Config{
		GenerateConfig: c.Bool(CLIGenerateConfig),
		Category:       c.String(CLICategory),
		Options: npdconfig.Options{
			Endpoint:       strings.TrimPrefix(strings.TrimSpace(c.String(CLIEndpoint)), "unix://"),
			Timeout:        c.Duration(CLITimeout),
			MaxOutput:      c.Int(CLIMaxOutputLength),
			CacheFile:      c.String(CLICacheFile),
			CacheTTL:       c.Duration(CLICacheTTL),
			File:           c.String(CLIConfigFile),
			InvokeInterval: c.Duration(CLIInvokeInterval),
			PluginTimeout:  c.Duration(CLIPluginTimeout),
			Concurrency:    c.Int(CLIConcurrency),
		},
	}
	o := &config.Options
	if o.Endpoint == "" {
		return nil, errors.New("--endpoint is required")
	}
	if !filepath.IsAbs(o.Endpoint) || strings.ContainsRune(o.Endpoint, 0) {
		return nil, errors.New("--endpoint must be an absolute Unix socket path or unix:///path without NUL bytes")
	}
	if !config.GenerateConfig && strings.TrimSpace(config.Category) == "" {
		return nil, errors.New("--category is required for health checks")
	}
	if o.Timeout <= 0 {
		return nil, errors.New("--timeout must be positive")
	}
	if o.CacheTTL < 0 {
		return nil, errors.New("--cache-ttl must be nonnegative")
	}
	if o.MaxOutput <= 0 {
		return nil, errors.New("--max-output-length must be positive")
	}
	if o.CacheFile != "" && (!filepath.IsAbs(o.CacheFile) || strings.ContainsRune(o.CacheFile, 0)) {
		return nil, errors.New("--cache-file must be an absolute path without NUL bytes or empty")
	}
	if config.GenerateConfig && o.File == "" {
		return nil, errors.New("--config-file is required with --generate-config")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to locate adapter executable: %w", err)
	}
	o.Executable = executable
	if config.GenerateConfig || o.File != "" {
		err = o.Validate()
	} else {
		err = o.ValidatePaths()
	}
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return config, nil
}

func Run(parent context.Context, config *Config, stdout, stderr io.Writer) int {
	o := config.Options
	logger := log.New(stderr, "rt_npd_adapter: ", 0)
	ctx, cancel := context.WithTimeout(parent, o.Timeout)
	defer cancel()
	client, err := exporter.NewXdmGrpcClient(&exporter.Config{XdmGrpcPath: o.Endpoint}, nil, xdmgrpc.ExporterType_Collector)
	if err != nil {
		logger.Printf("failed to initialize health service connection: %v", err)
		if config.GenerateConfig {
			return exitcode.Error
		}
		return emit(stdout, stderr, exitcode.Unknown, "health service unavailable", o.MaxOutput)
	}
	defer client.Close()
	generator := npdconfig.Generator{Options: o, Fetch: client.GetCategories}
	if config.GenerateConfig {
		if err := generator.Generate(ctx, true); err != nil {
			logger.Printf("failed to generate NPD configuration: %v", err)
			return exitcode.Error
		}
		return exitcode.Healthy
	}
	fetcher := xdmclient.CachedFetcher{
		Path: o.CacheFile, Key: "unix://" + o.Endpoint, TTL: o.CacheTTL,
		Fetch: client.GetAllHealth, Logf: logger.Printf,
	}
	snapshot, err := fetcher.Get(ctx)
	code, message := exitcode.Unknown, "health service unavailable"
	if err != nil {
		logger.Printf("failed to fetch health snapshot: %v", err)
	} else {
		code, message = adapter.Evaluate(snapshot, config.Category, time.Now().UnixMilli(), o.MaxOutput)
	}
	if o.File != "" {
		// Use only the remaining query budget and preserve the health result.
		if err := generator.Generate(ctx, false); err != nil {
			logger.Printf("failed to generate missing NPD configuration: %v", err)
		}
	}
	return emit(stdout, stderr, code, message, o.MaxOutput)
}

func emit(stdout, stderr io.Writer, code int, message string, maxBytes int) int {
	// Do not append a newline or exceed the byte limit. Healthy results emit no content.
	if _, err := io.WriteString(stdout, output.Truncate(message, maxBytes)); err != nil {
		fmt.Fprintf(stderr, "rt_npd_adapter: failed to write condition message: %v\n", err)
		return exitcode.Error
	}
	return code
}
