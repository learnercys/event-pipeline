package event

import (
	"errors"
	"testing"
	"time"
)

func valid() Event {
	return Event{
		ID:          "evt_1",
		Type:        PaymentCaptured,
		AccountID:   "acct_1",
		AmountCents: 1250,
		Currency:    "USD",
		OccurredAt:  time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC),
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
		ok     bool
	}{
		{"valid capture", func(*Event) {}, true},
		{"valid refund", func(e *Event) { e.Type = RefundIssued }, true},
		{"missing id", func(e *Event) { e.ID = "" }, false},
		{"unknown type", func(e *Event) { e.Type = "payment.voided" }, false},
		{"missing account", func(e *Event) { e.AccountID = "" }, false},
		{"zero amount", func(e *Event) { e.AmountCents = 0 }, false},
		{"negative amount", func(e *Event) { e.AmountCents = -5 }, false},
		{"lowercase currency", func(e *Event) { e.Currency = "usd" }, false},
		{"missing occurred_at", func(e *Event) { e.OccurredAt = time.Time{} }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid()
			tt.mutate(&e)
			err := e.Validate()
			if tt.ok && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestSignedAmount(t *testing.T) {
	e := valid()
	if got := e.SignedAmount(); got != 1250 {
		t.Fatalf("capture: got %d", got)
	}
	e.Type = RefundIssued
	if got := e.SignedAmount(); got != -1250 {
		t.Fatalf("refund: got %d", got)
	}
}

func TestDecode(t *testing.T) {
	e, err := Decode([]byte(`{"id":"evt_1","type":"payment.captured","account_id":"acct_1",
		"amount_cents":1250,"currency":"USD","occurred_at":"2026-11-13T05:00:00-05:00"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !e.SameAs(valid()) {
		t.Fatalf("decoded event differs: %+v", e)
	}
	if e.OccurredAt.Location() != time.UTC {
		t.Fatal("occurred_at should be normalized to UTC")
	}

	for _, bad := range []string{
		`not json`,
		`{"id":"evt_1"}`,
		`{"id":"evt_1","type":"payment.captured","account_id":"a","amount_cents":1,"currency":"USD","occurred_at":"2026-11-13T00:00:00Z","extra":true}`,
	} {
		if _, err := Decode([]byte(bad)); !errors.Is(err, ErrInvalid) {
			t.Errorf("Decode(%q): expected ErrInvalid, got %v", bad, err)
		}
	}
}

func TestSameAs(t *testing.T) {
	a, b := valid(), valid()
	b.OccurredAt = b.OccurredAt.In(time.FixedZone("EST", -5*3600))
	if !a.SameAs(b) {
		t.Fatal("same instant in different zones should be equal")
	}
	b.AmountCents++
	if a.SameAs(b) {
		t.Fatal("different amounts should not be equal")
	}
}
