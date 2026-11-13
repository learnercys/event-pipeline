// Package event defines the payment events that flow through the pipeline.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Type is the kind of payment event.
type Type string

const (
	PaymentCaptured Type = "payment.captured"
	RefundIssued    Type = "refund.issued"
)

// Event is a payment event as sent by the upstream payment provider.
//
// ID is the provider's event ID. Providers retry webhooks, so the same ID can
// arrive many times; it is the idempotency key for the whole pipeline.
type Event struct {
	ID          string    `json:"id"`
	Type        Type      `json:"type"`
	AccountID   string    `json:"account_id"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// ErrInvalid wraps every validation failure.
var ErrInvalid = errors.New("invalid event")

// Validate checks that the event is well formed.
func (e Event) Validate() error {
	switch {
	case e.ID == "" || len(e.ID) > 128:
		return fmt.Errorf("%w: id must be 1-128 characters", ErrInvalid)
	case e.Type != PaymentCaptured && e.Type != RefundIssued:
		return fmt.Errorf("%w: unknown type %q", ErrInvalid, e.Type)
	case e.AccountID == "" || len(e.AccountID) > 128:
		return fmt.Errorf("%w: account_id must be 1-128 characters", ErrInvalid)
	case e.AmountCents <= 0:
		return fmt.Errorf("%w: amount_cents must be positive", ErrInvalid)
	case !currencyRe.MatchString(e.Currency):
		return fmt.Errorf("%w: currency must be a 3-letter ISO code", ErrInvalid)
	case e.OccurredAt.IsZero():
		return fmt.Errorf("%w: occurred_at is required", ErrInvalid)
	}
	return nil
}

// SignedAmount is the effect of the event on the account balance:
// captures add money, refunds remove it.
func (e Event) SignedAmount() int64 {
	if e.Type == RefundIssued {
		return -e.AmountCents
	}
	return e.AmountCents
}

// SameAs reports whether two events with the same ID carry the same data.
// A provider that reuses an ID for different data is a bug we must surface,
// not silently deduplicate.
func (e Event) SameAs(o Event) bool {
	return e.ID == o.ID && e.Type == o.Type && e.AccountID == o.AccountID &&
		e.AmountCents == o.AmountCents && e.Currency == o.Currency &&
		e.OccurredAt.Equal(o.OccurredAt)
}

// Decode parses and validates a JSON-encoded event. Unknown fields are
// rejected so schema drift fails loudly instead of being half-processed.
func Decode(data []byte) (Event, error) {
	var e Event
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return e, nil
}
