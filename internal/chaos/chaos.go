// Package chaos injects failures at named points so tests can prove the
// pipeline stays correct when processes die at the worst possible moment.
//
// Failure points are configured with the CHAOS environment variable:
//
//	CHAOS="relay.crash_after_publish=0.2,ledger.crash_after_commit=0.1"
//
// Each value is the probability (0..1) that Should returns true at that point.
// With CHAOS unset, Should always returns false.
package chaos

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Injector decides whether to inject a failure at a given point.
type Injector struct {
	rates map[string]float64

	mu  sync.Mutex
	rng *rand.Rand

	countsMu sync.Mutex
	counts   map[string]*atomic.Int64
}

// Disabled never injects failures.
var Disabled = &Injector{}

// Parse builds an Injector from a spec such as "a=0.1,b=0.5".
func Parse(spec string) (*Injector, error) {
	inj := &Injector{
		rates:  map[string]float64{},
		rng:    rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		counts: map[string]*atomic.Int64{},
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, val, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("chaos: %q is not name=probability", part)
		}
		p, err := strconv.ParseFloat(val, 64)
		if err != nil || p < 0 || p > 1 {
			return nil, fmt.Errorf("chaos: probability for %q must be between 0 and 1", name)
		}
		inj.rates[strings.TrimSpace(name)] = p
	}
	return inj, nil
}

// FromEnv builds an Injector from the CHAOS environment variable.
func FromEnv() (*Injector, error) {
	return Parse(os.Getenv("CHAOS"))
}

// Should reports whether to inject a failure at point.
func (i *Injector) Should(point string) bool {
	p, ok := i.rates[point]
	if !ok || p == 0 {
		return false
	}
	i.mu.Lock()
	hit := i.rng.Float64() < p
	i.mu.Unlock()
	if hit {
		i.counter(point).Add(1)
	}
	return hit
}

// Injected returns how many failures were injected at point.
func (i *Injector) Injected(point string) int64 {
	if i.counts == nil {
		return 0
	}
	return i.counter(point).Load()
}

// Enabled reports whether any failure point is configured.
func (i *Injector) Enabled() bool { return len(i.rates) > 0 }

func (i *Injector) counter(point string) *atomic.Int64 {
	i.countsMu.Lock()
	defer i.countsMu.Unlock()
	c, ok := i.counts[point]
	if !ok {
		c = &atomic.Int64{}
		i.counts[point] = c
	}
	return c
}
