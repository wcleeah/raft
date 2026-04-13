package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/runtime"
	rserver "com.lwc.raft/internal/runtime/server"
)

type serverConfig struct {
	Id        string                `json:"id"`
	Addr      string                `json:"addr"`
	StorePath string                `json:"store_path"`
	Peers     []serverPeerConfig    `json:"peers"`
	LogLevel  string                `json:"log_level"`
	Transport serverTransportConfig `json:"transport"`
	Timers    serverTimersConfig    `json:"timers"`
}

type serverPeerConfig struct {
	Id   string `json:"id"`
	Addr string `json:"addr"`
}

type serverTransportConfig struct {
	ReadTimeoutMs  int `json:"read_timeout_ms"`
	WriteTimeoutMs int `json:"write_timeout_ms"`
}

type serverTimersConfig struct {
	Heartbeat          serverTimerConfig `json:"heartbeat"`
	ElectionTimeout    serverTimerConfig `json:"election_timeout"`
	WaitBeforeElection serverTimerConfig `json:"wait_before_election"`
	RestartElection    serverTimerConfig `json:"restart_election"`
}

type serverTimerConfig struct {
	MinMs          int `json:"min_ms"`
	MaxMs          int `json:"max_ms"`
	StartupGraceMs int `json:"startup_grace_ms"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to server config JSON")
	flag.Parse()

	if *configPath == "" {
		return errors.New("missing required -config")
	}

	cfg, err := loadServerConfig(*configPath)
	if err != nil {
		return err
	}

	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(cfg.StorePath), 0o755); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	store := &rserver.FileStore{
		Cond: sync.NewCond(&sync.Mutex{}),
		Path: cfg.StorePath,
	}

	transport := &rserver.Transport{
		ListenGcf: net.Listen,
		DialGcf:   net.Dial,
	}

	deps := &runtime.BrainDeps{
		ElectionTimer:        newTimer(cfg.Timers.RestartElection),
		WaitForElectionTimer: newTimer(cfg.Timers.WaitBeforeElection),
		ElectionTimeoutTimer: newTimer(cfg.Timers.ElectionTimeout),
		HeartbeatTimer:       newTimer(cfg.Timers.Heartbeat),
		EntriesStore:         store,
		Transport:            transport,
	}

	brain := runtime.NewBrain(logger, deps, &runtime.BrainConfig{
		Id:      cfg.Id,
		Addr:    cfg.Addr,
		Fellows: buildFellows(cfg.Peers),
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brain.Start(ctx, core.TransportCfg{
		ReadDln:  durationMs(cfg.Transport.ReadTimeoutMs),
		WriteDln: durationMs(cfg.Transport.WriteTimeoutMs),
	})

	<-brain.CloseCh()
	store.WaitForDone()
	err = brain.CloseReason()
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}

func loadServerConfig(path string) (serverConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return serverConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg serverConfig
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return serverConfig{}, fmt.Errorf("decode config: %w", err)
	}

	applyServerDefaults(&cfg)
	if err := validateServerConfig(cfg); err != nil {
		return serverConfig{}, err
	}

	return cfg, nil
}

func applyServerDefaults(cfg *serverConfig) {
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Transport.ReadTimeoutMs == 0 {
		cfg.Transport.ReadTimeoutMs = 10000
	}
	if cfg.Transport.WriteTimeoutMs == 0 {
		cfg.Transport.WriteTimeoutMs = 10000
	}
	applyTimerDefaults(&cfg.Timers.Heartbeat, 100, 100)
	applyTimerDefaults(&cfg.Timers.ElectionTimeout, 500, 500)
	applyTimerDefaults(&cfg.Timers.WaitBeforeElection, 150, 150)
	applyTimerDefaults(&cfg.Timers.RestartElection, 300, 300)
}

func validateServerConfig(cfg serverConfig) error {
	if cfg.Id == "" {
		return errors.New("config id is required")
	}
	if cfg.Addr == "" {
		return errors.New("config addr is required")
	}
	if cfg.StorePath == "" {
		return errors.New("config store_path is required")
	}
	if cfg.Transport.ReadTimeoutMs <= 0 {
		return errors.New("transport.read_timeout_ms must be > 0")
	}
	if cfg.Transport.WriteTimeoutMs <= 0 {
		return errors.New("transport.write_timeout_ms must be > 0")
	}
	if err := validateTimerConfig("timers.heartbeat", cfg.Timers.Heartbeat); err != nil {
		return err
	}
	if err := validateTimerConfig("timers.election_timeout", cfg.Timers.ElectionTimeout); err != nil {
		return err
	}
	if err := validateTimerConfig("timers.wait_before_election", cfg.Timers.WaitBeforeElection); err != nil {
		return err
	}
	if err := validateTimerConfig("timers.restart_election", cfg.Timers.RestartElection); err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		if peer.Id == "" {
			return errors.New("peer id is required")
		}
		if peer.Addr == "" {
			return fmt.Errorf("peer %q addr is required", peer.Id)
		}
		if peer.Id == cfg.Id {
			return fmt.Errorf("peer %q duplicates self id", peer.Id)
		}
		if _, ok := seen[peer.Id]; ok {
			return fmt.Errorf("duplicate peer id %q", peer.Id)
		}
		seen[peer.Id] = struct{}{}
	}

	return nil
}

func buildFellows(peers []serverPeerConfig) map[string]*runtime.Fellow {
	fellows := make(map[string]*runtime.Fellow, len(peers))
	for _, peer := range peers {
		fellows[peer.Id] = &runtime.Fellow{
			Id:   peer.Id,
			Addr: peer.Addr,
		}
	}

	return fellows
}

func parseLogLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q", level)
	}
}

func applyTimerDefaults(cfg *serverTimerConfig, defaultMin int, defaultMax int) {
	switch {
	case cfg.MinMs == 0 && cfg.MaxMs == 0:
		cfg.MinMs = defaultMin
		cfg.MaxMs = defaultMax
	case cfg.MinMs == 0:
		cfg.MinMs = cfg.MaxMs
	case cfg.MaxMs == 0:
		cfg.MaxMs = cfg.MinMs
	}
}

func validateTimerConfig(name string, cfg serverTimerConfig) error {
	if cfg.MinMs <= 0 {
		return fmt.Errorf("%s.min_ms must be > 0", name)
	}
	if cfg.MaxMs <= 0 {
		return fmt.Errorf("%s.max_ms must be > 0", name)
	}
	if cfg.MaxMs < cfg.MinMs {
		return fmt.Errorf("%s.max_ms must be >= %s.min_ms", name, name)
	}
	if cfg.StartupGraceMs < 0 {
		return fmt.Errorf("%s.startup_grace_ms must be >= 0", name)
	}

	return nil
}

func newTimer(cfg serverTimerConfig) core.Timer {
	return &rserver.Timer{
		Min:          durationMs(cfg.MinMs),
		Max:          durationMs(cfg.MaxMs),
		StartupGrace: durationMs(cfg.StartupGraceMs),
		After:        time.After,
	}
}

func durationMs(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
