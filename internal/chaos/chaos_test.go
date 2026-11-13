package chaos

import "testing"

func TestParse(t *testing.T) {
	inj, err := Parse("a=1, b=0 ,c=0.5")
	if err != nil {
		t.Fatal(err)
	}
	if !inj.Should("a") {
		t.Error("probability 1 should always inject")
	}
	if inj.Should("b") {
		t.Error("probability 0 should never inject")
	}
	if inj.Should("unknown") {
		t.Error("unconfigured point should never inject")
	}
	if inj.Injected("a") != 1 {
		t.Errorf("expected 1 injection at a, got %d", inj.Injected("a"))
	}
}

func TestParseErrors(t *testing.T) {
	for _, spec := range []string{"a", "a=x", "a=1.5", "a=-0.1"} {
		if _, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q): expected error", spec)
		}
	}
}

func TestDisabled(t *testing.T) {
	if Disabled.Should("anything") || Disabled.Enabled() || Disabled.Injected("anything") != 0 {
		t.Fatal("Disabled must never inject")
	}
}

func TestEmptySpec(t *testing.T) {
	inj, err := Parse("")
	if err != nil || inj.Enabled() {
		t.Fatalf("empty spec should be disabled, err=%v", err)
	}
}
