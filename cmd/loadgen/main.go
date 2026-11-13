// Command loadgen plays the role of a payment provider that retries webhooks.
//
// It sends N unique events through the ingest API, re-sends a share of them
// as duplicates, retries every failure until the event is accepted, and
// publishes a few poison messages straight to SNS. It writes what the
// pipeline *should* end up with to an expectations file for cmd/verify.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/learnercys/event-pipeline/internal/awsx"
	"github.com/learnercys/event-pipeline/internal/event"
	"github.com/learnercys/event-pipeline/internal/verify"
)

func main() {
	var (
		url      = flag.String("url", "http://localhost:58080", "ingest API base URL")
		n        = flag.Int("events", 2000, "unique events to send")
		accounts = flag.Int("accounts", 50, "number of accounts")
		dupRate  = flag.Float64("duplicates", 0.3, "share of events sent a second time")
		poison   = flag.Int("poison", 5, "malformed messages to publish directly to SNS")
		topicARN = flag.String("topic", "arn:aws:sns:us-east-1:123456789012:payment-events", "SNS topic for poison messages")
		workers  = flag.Int("concurrency", 16, "concurrent HTTP clients")
		rate     = flag.Int("rate", 0, "max requests per second (0 = unlimited)")
		seed     = flag.Uint64("seed", 42, "random seed (same seed, same events)")
		out      = flag.String("out", "expected.json", "where to write the expectations")
		runID    = flag.String("run", time.Now().UTC().Format("20060102T150405"), "prefix for event IDs")
	)
	flag.Parse()

	rng := rand.New(rand.NewPCG(*seed, *seed))
	events := make([]event.Event, *n)
	exp := verify.Expected{RunID: *runID, Poison: *poison, Balances: map[string]int64{}}
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := range events {
		e := event.Event{
			ID:          fmt.Sprintf("%s_evt_%06d", *runID, i),
			Type:        event.PaymentCaptured,
			AccountID:   fmt.Sprintf("%s_acct_%03d", *runID, rng.IntN(*accounts)),
			AmountCents: 100 + rng.Int64N(99_900),
			Currency:    []string{"USD", "EUR", "GTQ"}[rng.IntN(3)],
			OccurredAt:  base.Add(time.Duration(rng.Int64N(int64(14 * 24 * time.Hour)))).Truncate(time.Second),
		}
		if rng.Float64() < 0.2 {
			e.Type = event.RefundIssued
		}
		events[i] = e
		exp.Balances[verify.BalanceKey(e.AccountID, e.Currency)] += e.SignedAmount()
	}
	exp.Events = len(events)

	// Build the send list: every event once, some twice, in shuffled order so
	// duplicates arrive both close together and far apart.
	sends := slices.Clone(events)
	for _, e := range events {
		if rng.Float64() < *dupRate {
			sends = append(sends, e)
		}
	}
	rng.Shuffle(len(sends), func(i, j int) { sends[i], sends[j] = sends[j], sends[i] })

	client := &http.Client{Timeout: 10 * time.Second}
	var (
		mu        sync.Mutex
		latencies []time.Duration
		statuses  = map[int]int{}
		retries   atomic.Int64
		next      atomic.Int64
		wg        sync.WaitGroup
	)
	var tick <-chan time.Time
	if *rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(*rate))
		defer t.Stop()
		tick = t.C
	}
	start := time.Now()
	for range *workers {
		wg.Go(func() {
			for {
				i := next.Add(1) - 1
				if i >= int64(len(sends)) {
					return
				}
				body, _ := json.Marshal(sends[i])
				for attempt := 0; ; attempt++ {
					if tick != nil {
						<-tick
					}
					t0 := time.Now()
					resp, err := client.Post(*url+"/v1/events", "application/json", bytes.NewReader(body))
					d := time.Since(t0)
					code := 0
					if err == nil {
						code = resp.StatusCode
						resp.Body.Close()
					}
					mu.Lock()
					latencies = append(latencies, d)
					statuses[code]++
					mu.Unlock()
					if code == http.StatusCreated || code == http.StatusOK {
						break
					}
					if code == http.StatusConflict || code == http.StatusBadRequest {
						log.Fatalf("event %s rejected with %d: generator bug", sends[i].ID, code)
					}
					retries.Add(1) // 503 or network error: retry like a real provider
					time.Sleep(time.Duration(min(attempt+1, 10)) * 20 * time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	if *poison > 0 {
		publishPoison(*topicARN, *runID, *poison)
	}

	slices.Sort(latencies)
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
	fmt.Printf("sent %d unique events as %d requests (+%d retries) in %s\n",
		len(events), len(sends), retries.Load(), elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f req/s   latency p50=%s p95=%s p99=%s\n",
		float64(len(latencies))/elapsed.Seconds(),
		pct(0.50).Round(time.Microsecond*10), pct(0.95).Round(time.Microsecond*10), pct(0.99).Round(time.Microsecond*10))
	fmt.Printf("responses: %v   poison published: %d\n", statuses, *poison)

	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(exp); err != nil {
		log.Fatal(err)
	}
}

// publishPoison simulates an upstream bug: messages that skip the ingest
// API's validation and can never be processed. They must end up in the DLQs.
func publishPoison(topicARN, runID string, n int) {
	ctx := context.Background()
	cfg, err := awsx.Load(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client := awsx.SNS(cfg)
	for i := range n {
		msg := fmt.Sprintf(`{"id":"%s_poison_%d","type":"payment.captured","amount_cents":"not-a-number"}`, runID, i)
		if _, err := client.Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(topicARN), Message: aws.String(msg),
		}); err != nil {
			log.Fatalf("publish poison: %v", err)
		}
	}
}
