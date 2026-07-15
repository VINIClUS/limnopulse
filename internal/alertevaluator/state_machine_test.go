package alertevaluator

import (
	"testing"
	"time"
)

var testSlot = time.Date(2026, 7, 15, 12, 0, 45, 0, time.UTC)

func testRule() Rule {
	return Rule{
		TenantID:           "tnt_1",
		RuleID:             "rule_1",
		Version:            3,
		EvaluationRevision: 2,
		Duration:           2 * time.Minute,
		Cooldown:           30 * time.Minute,
		Channels:           []Channel{ChannelEmail, ChannelTelegram},
	}
}

func sufficient(slot time.Time, breached bool) Evaluation {
	return Evaluation{Slot: slot, Quality: QualitySufficient, Breached: breached, Value: 4.2}
}

func TestDurationOpensExactlyOnceAfterRequiredSlots(t *testing.T) {
	rule := testRule()
	first := Decide(rule, State{}, sufficient(testSlot, true), time.Minute)
	if first.Next.Mode != ModePending || first.Next.ConfirmedSlots != 1 || first.Transition != TransitionNone {
		t.Fatalf("first decision = %#v", first)
	}

	second := Decide(rule, first.Next, sufficient(testSlot.Add(time.Minute), true), time.Minute)
	if second.Transition != TransitionOpened || second.Next.Mode != ModeActive || second.Next.ActiveStatus != StatusOpen {
		t.Fatalf("second decision = %#v", second)
	}
	if len(second.Outboxes) != 2 || second.Next.ActiveEventID == "" {
		t.Fatalf("opening did not create event and per-channel outboxes: %#v", second)
	}

	repeated := Decide(rule, second.Next, sufficient(testSlot.Add(2*time.Minute), true), time.Minute)
	if repeated.Transition != TransitionNone || len(repeated.Outboxes) != 0 || repeated.Next.ActiveEventID != second.Next.ActiveEventID {
		t.Fatalf("repeated decision = %#v", repeated)
	}
}

func TestGapRestartsPendingConfirmation(t *testing.T) {
	rule := testRule()
	first := Decide(rule, State{}, sufficient(testSlot, true), time.Minute)
	afterGap := Decide(rule, first.Next, Evaluation{Slot: testSlot.Add(3 * time.Minute), Quality: QualitySufficient, Breached: true, MissedSlots: 2}, time.Minute)
	if afterGap.Next.Mode != ModePending || afterGap.Next.ConfirmedSlots != 1 || !afterGap.Next.PendingSince.Equal(testSlot.Add(3*time.Minute)) {
		t.Fatalf("after gap = %#v", afterGap)
	}
}

func TestIndeterminateDataNeverOpensOrResolves(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	indeterminate := Decide(rule, pending, Evaluation{Slot: testSlot.Add(time.Minute), Quality: QualityStaleData}, time.Minute)
	if indeterminate.Next.Mode != ModePending || indeterminate.Transition != TransitionNone {
		t.Fatalf("pending indeterminate = %#v", indeterminate)
	}

	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	activeIndeterminate := Decide(rule, opened, Evaluation{Slot: testSlot.Add(2 * time.Minute), Quality: QualityQueryError}, time.Minute)
	if activeIndeterminate.Next.Mode != ModeActive || activeIndeterminate.Transition != TransitionNone {
		t.Fatalf("active indeterminate = %#v", activeIndeterminate)
	}
}

func TestCooldownCreatesDurableSuppressedEpisodeWithoutOutbox(t *testing.T) {
	rule := testRule()
	state := State{CooldownUntil: testSlot.Add(time.Hour), LastNotifiedEventID: "alert_previous"}
	first := Decide(rule, state, sufficient(testSlot, true), time.Minute)
	opened := Decide(rule, first.Next, sufficient(testSlot.Add(time.Minute), true), time.Minute)
	if opened.Transition != TransitionSuppressed || opened.Next.ActiveStatus != StatusSuppressed {
		t.Fatalf("suppressed decision = %#v", opened)
	}
	if len(opened.Outboxes) != 0 || opened.Next.SuppressionSourceEventID != "alert_previous" {
		t.Fatalf("suppressed outbox/source = %#v", opened)
	}
}

func TestCleanWindowResolvesAndCreatesBlockedChannelDependencies(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	recovered := Decide(rule, opened, sufficient(testSlot.Add(2*time.Minute), false), time.Minute)
	if recovered.Transition != TransitionRecovered || recovered.Next.Mode != ModeHealthy || recovered.ResolvedEventID != opened.ActiveEventID {
		t.Fatalf("recovery = %#v", recovered)
	}
	if len(recovered.Outboxes) != 2 {
		t.Fatalf("recovery outboxes = %#v", recovered.Outboxes)
	}
	for _, outbox := range recovered.Outboxes {
		if outbox.Status != OutboxBlocked || outbox.DependsOnOutboxID == "" {
			t.Fatalf("recovery dependency = %#v", outbox)
		}
	}
}

func TestEventIdentityIncludesOpeningWindowAndVersions(t *testing.T) {
	rule := testRule()
	a := EventID(rule, testSlot)
	b := EventID(rule, testSlot.Add(time.Minute))
	rule.Version++
	c := EventID(rule, testSlot)
	if a == b || a == c || len(a) != len("alert_")+64 {
		t.Fatalf("event identities not distinct: %q %q %q", a, b, c)
	}
}
