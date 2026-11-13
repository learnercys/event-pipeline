// Package verify checks the pipeline's end state against what the load
// generator sent. It is the oracle for the chaos test.
package verify

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/learnercys/event-pipeline/internal/event"
)

// Expected is what loadgen sent, computed independently of the pipeline.
type Expected struct {
	RunID    string           `json:"run_id"`
	Events   int              `json:"events"`
	Poison   int              `json:"poison"`
	Balances map[string]int64 `json:"balances"` // "account/currency" -> cents
}

// BalanceKey is the map key for one account balance.
func BalanceKey(account, currency string) string { return account + "/" + currency }

// Check is one named invariant.
type Check struct {
	Name string
	OK   bool
	Got  string
	Want string
}

// Verifier evaluates the invariants. It keeps state between calls because it
// drains this run's poison messages from the dead-letter queues as it finds them.
type Verifier struct {
	DB   *pgxpool.Pool
	SQS  *sqs.Client
	DLQs map[string]string // consumer name -> DLQ URL
	Exp  Expected

	poisonSeen map[string]map[string]bool // consumer -> message IDs
	lostInDLQ  map[string]int             // valid events in the DLQ that were never applied
	safeInDLQ  map[string]int             // valid events in the DLQ that were already applied
}

// Run evaluates every invariant once.
func (v *Verifier) Run(ctx context.Context) ([]Check, error) {
	if v.poisonSeen == nil {
		v.poisonSeen = map[string]map[string]bool{}
		v.lostInDLQ, v.safeInDLQ = map[string]int{}, map[string]int{}
	}
	like := v.Exp.RunID + "\\_%"
	var checks []Check
	add := func(name string, got, want any) {
		g, w := fmt.Sprint(got), fmt.Sprint(want)
		checks = append(checks, Check{Name: name, OK: g == w, Got: g, Want: w})
	}

	var stored, unpublished int
	if err := v.DB.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE o.published_at IS NULL)
		FROM events e JOIN outbox o ON o.event_id = e.id
		WHERE e.id LIKE $1`, like).Scan(&stored, &unpublished); err != nil {
		return nil, err
	}
	add("every unique event stored exactly once", stored, v.Exp.Events)
	add("outbox fully published", unpublished, 0)

	for _, consumer := range []string{"ledger", "analytics"} {
		var processed int
		if err := v.DB.QueryRow(ctx, `
			SELECT count(*) FROM processed_messages
			WHERE consumer = $1 AND event_id LIKE $2`, consumer, like).Scan(&processed); err != nil {
			return nil, err
		}
		add(consumer+": each event processed exactly once", processed, v.Exp.Events)
	}

	ledger, err := v.sums(ctx, `
		SELECT account_id, currency, amount_cents FROM balances WHERE account_id LIKE $1`, like)
	if err != nil {
		return nil, err
	}
	add("ledger: balances match", diff(ledger, v.Exp.Balances), "no differences")

	analytics, err := v.sums(ctx, `
		SELECT account_id, currency, sum(captured_cents - refunded_cents)::bigint
		FROM account_daily_stats WHERE account_id LIKE $1 GROUP BY 1, 2`, like)
	if err != nil {
		return nil, err
	}
	add("analytics: daily totals match", diff(analytics, v.Exp.Balances), "no differences")

	var counted int
	if err := v.DB.QueryRow(ctx, `
		SELECT coalesce(sum(event_count), 0) FROM account_daily_stats
		WHERE account_id LIKE $1`, like).Scan(&counted); err != nil {
		return nil, err
	}
	add("analytics: event count matches", counted, v.Exp.Events)

	for consumer, url := range v.DLQs {
		if err := v.drainDLQ(ctx, consumer, url); err != nil {
			return nil, err
		}
		add(consumer+": poison messages in DLQ", len(v.poisonSeen[consumer]), v.Exp.Poison)
		add(consumer+": unapplied events in DLQ", v.lostInDLQ[consumer], 0)
		// An event can reach the DLQ after it was applied, if the consumer
		// crashed before every ack. Redriving it is a harmless duplicate, so
		// this is reported for information only.
		checks = append(checks, Check{Name: consumer + ": applied events also in DLQ (info)",
			OK: true, Got: fmt.Sprint(v.safeInDLQ[consumer]), Want: "any"})
	}
	return checks, nil
}

func (v *Verifier) sums(ctx context.Context, query, like string) (map[string]int64, error) {
	rows, err := v.DB.Query(ctx, query, like)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var account, currency string
		var cents int64
		if err := rows.Scan(&account, &currency, &cents); err != nil {
			return nil, err
		}
		got[BalanceKey(account, strings.TrimSpace(currency))] = cents
	}
	return got, rows.Err()
}

// drainDLQ removes this run's messages from a DLQ and classifies them.
// Messages from other runs are left alone (they become visible again in 1s).
func (v *Verifier) drainDLQ(ctx context.Context, consumer, url string) error {
	if v.poisonSeen[consumer] == nil {
		v.poisonSeen[consumer] = map[string]bool{}
	}
	for {
		out, err := v.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, VisibilityTimeout: 1,
		})
		if err != nil {
			return fmt.Errorf("receive from %s DLQ: %w", consumer, err)
		}
		mine := 0
		for _, m := range out.Messages {
			body := aws.ToString(m.Body)
			if !strings.Contains(body, `"`+v.Exp.RunID+"_") {
				continue
			}
			mine++
			if e, err := event.Decode([]byte(body)); err == nil {
				var applied bool
				if err := v.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM processed_messages
					WHERE consumer = $1 AND event_id = $2)`, consumer, e.ID).Scan(&applied); err != nil {
					return err
				}
				if applied {
					v.safeInDLQ[consumer]++
				} else {
					v.lostInDLQ[consumer]++
				}
			} else {
				v.poisonSeen[consumer][aws.ToString(m.MessageId)] = true
			}
			if _, err := v.SQS.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				return fmt.Errorf("delete from %s DLQ: %w", consumer, err)
			}
		}
		if mine == 0 {
			return nil
		}
	}
}

// diff summarises how two balance maps differ.
func diff(got, want map[string]int64) string {
	var wrong, missing, extra int
	for k, w := range want {
		g, ok := got[k]
		switch {
		case !ok:
			missing++
		case g != w:
			wrong++
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			extra++
		}
	}
	if wrong+missing+extra == 0 {
		return "no differences"
	}
	return fmt.Sprintf("%d wrong, %d missing, %d unexpected of %d", wrong, missing, extra, len(want))
}
