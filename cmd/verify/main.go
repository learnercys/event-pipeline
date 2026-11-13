// Command verify waits for the pipeline to settle and checks every invariant
// against the expectations written by loadgen. It exits non-zero on failure.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/learnercys/event-pipeline/internal/awsx"
	"github.com/learnercys/event-pipeline/internal/verify"
)

func main() {
	var (
		expFile = flag.String("expected", "expected.json", "file written by loadgen")
		dbURL   = flag.String("db", "postgres://postgres:postgres@localhost:55432/pipeline?sslmode=disable", "Postgres URL")
		sqsBase = flag.String("queues", "http://localhost:55000/123456789012", "SQS queue URL prefix")
		timeout = flag.Duration("timeout", 3*time.Minute, "how long to wait for the pipeline to settle")
	)
	flag.Parse()
	ctx := context.Background()

	raw, err := os.ReadFile(*expFile)
	if err != nil {
		fail(err)
	}
	var exp verify.Expected
	if err := json.Unmarshal(raw, &exp); err != nil {
		fail(err)
	}
	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		fail(err)
	}
	defer pool.Close()
	cfg, err := awsx.Load(ctx)
	if err != nil {
		fail(err)
	}

	v := &verify.Verifier{
		DB:  pool,
		SQS: awsx.SQS(cfg),
		DLQs: map[string]string{
			"ledger":    *sqsBase + "/ledger-events-dlq",
			"analytics": *sqsBase + "/analytics-events-dlq",
		},
		Exp: exp,
	}

	start := time.Now()
	deadline := start.Add(*timeout)
	var checks []verify.Check
	for {
		checks, err = v.Run(ctx)
		if err != nil {
			fail(err)
		}
		if allOK(checks) || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("run %s: %d events, %d poison messages, settled check after %s\n\n",
		exp.RunID, exp.Events, exp.Poison, time.Since(start).Round(time.Second))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\tINVARIANT\tGOT\tWANT")
	for _, c := range checks {
		mark := "✅"
		if !c.OK {
			mark = "❌"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", mark, c.Name, c.Got, c.Want)
	}
	w.Flush()

	if !allOK(checks) {
		fmt.Println("\nFAIL: invariants did not hold before the timeout")
		os.Exit(1)
	}
	fmt.Println("\nPASS: every event was applied exactly once by every consumer")
}

func allOK(checks []verify.Check) bool {
	for _, c := range checks {
		if !c.OK {
			return false
		}
	}
	return true
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "verify:", err)
	os.Exit(2)
}
