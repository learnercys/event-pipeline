// Package config reads settings from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// String returns the variable or a default.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Must returns the variable or exits if it is missing.
func Must(key string) string {
	v := os.Getenv(key)
	if v == "" {
		Fatal(fmt.Errorf("%s is required", key))
	}
	return v
}

// Int returns the variable as an int or a default.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		Fatal(fmt.Errorf("%s: %w", key, err))
	}
	return n
}

// Duration returns the variable as a time.Duration or a default.
func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		Fatal(fmt.Errorf("%s: %w", key, err))
	}
	return d
}

// Logger returns a JSON logger tagged with the service name. LOG_LEVEL=debug
// turns on debug output.
func Logger(service string) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	l := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})).
		With("service", service)
	slog.SetDefault(l)
	return l
}

// Fatal logs the error and exits.
func Fatal(err error) {
	slog.Error("fatal", "err", err)
	os.Exit(1)
}
