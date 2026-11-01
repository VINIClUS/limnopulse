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
	return Evaluation{Slot: slot, Quality: QualitySufficient, Breached: breached, Value: Measured(4.2)}
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
	afterGap := Decide(rule, first.Next, Evaluation{Slot: testSlot.Add(3 * time.Minute), Quality: QualitySufficient, Breached: true, Value: Measured(0), MissedSlots: 2}, time.Minute)
	if afterGap.Next.Mode != ModePending || afterGap.Next.ConfirmedSlots != 1 || !afterGap.Next.PendingSince.Equal(testSlot.Add(3*time.Minute)) {
		t.Fatalf("after gap = %#v", afterGap)
	}
}

func TestIndeterminateDataNeverOpensOrResolves(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	indeterminate := Decide(rule, pending, Evaluation{Slot: testSlot.Add(time.Minute), Quality: QualityStaleData, Value: Measured(0)}, time.Minute)
	if indeterminate.Next.Mode != ModePending || indeterminate.Transition != TransitionNone {
		t.Fatalf("pending indeterminate = %#v", indeterminate)
	}

	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	activeIndeterminate := Decide(rule, opened, Evaluation{Slot: testSlot.Add(2 * time.Minute), Quality: QualityQueryError, Value: Measured(0)}, time.Minute)
	if activeIndeterminate.Next.Mode != ModeActive || activeIndeterminate.Transition != TransitionNone {
		t.Fatalf("active indeterminate = %#v", activeIndeterminate)
	}
}

func TestIndeterminateDataPreservesLastSufficientValue(t *testing.T) {
	rule := testRule()
	state := State{
		Mode:          ModeActive,
		ActiveEventID: "alert_1",
		ActiveStatus:  StatusOpen,
		LastValue:     Measured(4.2),
	}

	decision := Decide(
		rule,
		state,
		Evaluation{Slot: testSlot, Quality: QualityQueryError, Value: Measured(0)},
		time.Minute,
	)

	if value, ok := decision.Next.LastValue.Get(); !ok || value != 4.2 {
		t.Fatalf("last value = %v, want last sufficient value 4.2", decision.Next.LastValue)
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

func TestRecoveryUsesPersistedOpeningChannelsInStableOrder(t *testing.T) {
	rule := testRule()
	rule.Channels = []Channel{ChannelTelegram, Channel("webhook")}
	state := State{
		Mode:          ModeActive,
		ActiveEventID: "alert_episode",
		ActiveStatus:  StatusOpen,
		OpeningOutboxes: map[Channel]string{
			ChannelTelegram: "outbox_open_telegram",
			ChannelEmail:    "outbox_open_email",
		},
	}

	recovered := Decide(rule, state, sufficient(testSlot, false), time.Minute)

	if len(recovered.Outboxes) != 2 {
		t.Fatalf("recovery outboxes = %#v", recovered.Outboxes)
	}
	want := []struct {
		channel   Channel
		openingID string
	}{
		{channel: ChannelEmail, openingID: "outbox_open_email"},
		{channel: ChannelTelegram, openingID: "outbox_open_telegram"},
	}
	for index, expected := range want {
		outbox := recovered.Outboxes[index]
		if outbox.Channel != expected.channel || outbox.DependsOnOutboxID != expected.openingID {
			t.Fatalf("recovery outbox %d = %#v", index, outbox)
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

// absentButSufficient is an inconsistent input: quality says sufficient but no
// value is present. It isolates presence from quality, so these tests fail if
// Decide gates on quality alone.
func absentButSufficient(slot time.Time, breached bool) Evaluation {
	return Evaluation{Slot: slot, Quality: QualitySufficient, Breached: breached}
}

func TestAbsentValueWithoutGapNeverConfirmsOpensOrResetsPendingIncident(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	if pending.Mode != ModePending || pending.ConfirmedSlots != 1 {
		t.Fatalf("setup pending = %#v", pending)
	}

	for _, breached := range []bool{true, false} {
		decision := Decide(rule, pending, absentButSufficient(testSlot.Add(time.Minute), breached), time.Minute)
		if decision.Transition != TransitionNone || len(decision.Outboxes) != 0 || decision.EventID != "" {
			t.Fatalf("breached=%v absent value moved the incident: %#v", breached, decision)
		}
		next := decision.Next
		if next.Mode != ModePending || next.ConfirmedSlots != 1 ||
			!next.PendingSince.Equal(testSlot) || !next.LastBreachSlot.Equal(testSlot) {
			t.Fatalf("breached=%v absent value changed pending confirmation: %#v", breached, next)
		}
		if value, ok := next.LastValue.Get(); !ok || value != 4.2 {
			t.Fatalf("breached=%v absent value replaced last value: %v", breached, next.LastValue)
		}
	}
}

func TestAbsentValueNeverResolvesActiveIncident(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	if opened.Mode != ModeActive {
		t.Fatalf("setup active = %#v", opened)
	}

	decision := Decide(rule, opened, absentButSufficient(testSlot.Add(2*time.Minute), false), time.Minute)

	if decision.Transition != TransitionNone || decision.ResolvedEventID != "" || len(decision.Outboxes) != 0 {
		t.Fatalf("absent clean value resolved the incident: %#v", decision)
	}
	if decision.Next.Mode != ModeActive || decision.Next.ActiveEventID != opened.ActiveEventID ||
		decision.Next.ActiveStatus != StatusOpen || len(decision.Next.OpeningOutboxes) != len(opened.OpeningOutboxes) {
		t.Fatalf("absent clean value changed the active episode: %#v", decision.Next)
	}
	if value, ok := decision.Next.LastValue.Get(); !ok || value != 4.2 {
		t.Fatalf("absent value replaced last value: %v", decision.Next.LastValue)
	}
}

func TestIndeterminateEvaluationNeverCreatesAnImplicitZeroLastValue(t *testing.T) {
	rule := testRule()
	for _, test := range []struct {
		evaluation  Evaluation
		wantQuality Quality
	}{
		{Evaluation{Slot: testSlot, Quality: QualityInsufficientData}, QualityInsufficientData},
		{Evaluation{Slot: testSlot, Quality: QualityStaleData, Value: Measured(9.9)}, QualityStaleData},
		{Evaluation{Slot: testSlot, Quality: QualityQueryError, Value: Measured(0)}, QualityQueryError},
		// Sufficient without a value is recorded as insufficient, so a stored
		// LastQuality of sufficient always comes with a real value.
		{absentButSufficient(testSlot, true), QualityInsufficientData},
	} {
		decision := Decide(rule, State{}, test.evaluation, time.Minute)
		if decision.Next.LastValue.IsPresent() {
			t.Fatalf("%s evaluation produced last value %v, want absent", test.evaluation.Quality, decision.Next.LastValue)
		}
		if decision.Next.LastQuality != test.wantQuality || !decision.Next.LastEvaluatedSlot.Equal(testSlot) {
			t.Fatalf("%s evaluation recorded quality %s at %v, want %s at %v",
				test.evaluation.Quality, decision.Next.LastQuality, decision.Next.LastEvaluatedSlot, test.wantQuality, testSlot)
		}
	}
}

func TestCoalescedGapStillResetsPendingOnIndeterminateEvaluation(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	for _, evaluation := range []Evaluation{
		{Slot: testSlot.Add(3 * time.Minute), Quality: QualityStaleData, MissedSlots: 2},
		{Slot: testSlot.Add(3 * time.Minute), Quality: QualitySufficient, Breached: true, MissedSlots: 2},
	} {
		decision := Decide(rule, pending, evaluation, time.Minute)
		if decision.Transition != TransitionNone || decision.Next.Mode != ModeHealthy || decision.Next.ConfirmedSlots != 0 ||
			!decision.Next.PendingSince.IsZero() || !decision.Next.LastBreachSlot.IsZero() {
			t.Fatalf("%s gap did not reset pending: %#v", evaluation.Quality, decision)
		}
		if value, ok := decision.Next.LastValue.Get(); !ok || value != 4.2 {
			t.Fatalf("%s gap replaced last value: %v", evaluation.Quality, decision.Next.LastValue)
		}
	}

	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	decision := Decide(rule, opened, Evaluation{Slot: testSlot.Add(4 * time.Minute), Quality: QualityQueryError, MissedSlots: 2}, time.Minute)
	if decision.Transition != TransitionNone || decision.Next.Mode != ModeActive || decision.Next.ActiveEventID != opened.ActiveEventID {
		t.Fatalf("gap with an indeterminate evaluation changed the active incident: %#v", decision)
	}
}

func TestPresentZeroIsDeterminateForOpeningAndRecovery(t *testing.T) {
	rule := testRule()
	rule.Duration = time.Minute
	zero := func(slot time.Time, breached bool) Evaluation {
		return Evaluation{Slot: slot, Quality: QualitySufficient, Breached: breached, Value: Measured(0)}
	}

	opened := Decide(rule, State{}, zero(testSlot, true), time.Minute)
	if opened.Transition != TransitionOpened || opened.Next.Mode != ModeActive {
		t.Fatalf("breached present zero did not open: %#v", opened)
	}
	if value, ok := opened.Next.LastValue.Get(); !ok || value != 0 {
		t.Fatalf("opening last value = %v, want present 0", opened.Next.LastValue)
	}

	recovered := Decide(rule, opened.Next, zero(testSlot.Add(time.Minute), false), time.Minute)
	if recovered.Transition != TransitionRecovered || recovered.ResolvedEventID != opened.EventID {
		t.Fatalf("clean present zero did not recover: %#v", recovered)
	}
	if value, ok := recovered.Next.LastValue.Get(); !ok || value != 0 {
		t.Fatalf("recovery last value = %v, want present 0", recovered.Next.LastValue)
	}
}

func TestDeterminateValueReplacesRetainedLastValue(t *testing.T) {
	rule := testRule()
	state := State{Mode: ModeHealthy, LastValue: Measured(4.2)}

	decision := Decide(rule, state, Evaluation{Slot: testSlot, Quality: QualitySufficient, Value: Measured(-3.5)}, time.Minute)

	if value, ok := decision.Next.LastValue.Get(); !ok || value != -3.5 {
		t.Fatalf("last value = %v, want present -3.5", decision.Next.LastValue)
	}
}

func TestPresentValueWithIndeterminateQualityNeverMovesAnIncident(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	confirm := Decide(rule, pending, Evaluation{Slot: testSlot.Add(time.Minute), Quality: QualityStaleData, Breached: true, Value: Measured(1)}, time.Minute)
	if confirm.Transition != TransitionNone || confirm.Next.Mode != ModePending || confirm.Next.ConfirmedSlots != 1 {
		t.Fatalf("stale breaching value confirmed the pending incident: %#v", confirm)
	}

	opened := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	resolve := Decide(rule, opened, Evaluation{Slot: testSlot.Add(2 * time.Minute), Quality: QualityStaleData, Value: Measured(0)}, time.Minute)
	if resolve.Transition != TransitionNone || resolve.Next.Mode != ModeActive || resolve.Next.ActiveEventID != opened.ActiveEventID {
		t.Fatalf("stale clean value resolved the active incident: %#v", resolve)
	}
}

func TestIndeterminateSlotBreaksTheConfirmationChain(t *testing.T) {
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	for _, gap := range []Evaluation{
		{Slot: testSlot.Add(time.Minute), Quality: QualityStaleData, Value: Measured(1)},
		absentButSufficient(testSlot.Add(time.Minute), true),
		{Slot: testSlot.Add(time.Minute), Quality: QualityQueryError},
	} {
		afterGap := Decide(rule, pending, gap, time.Minute).Next
		next := Decide(rule, afterGap, sufficient(testSlot.Add(2*time.Minute), true), time.Minute)
		if next.Transition != TransitionNone || next.Next.Mode != ModePending || next.Next.ConfirmedSlots != 1 ||
			!next.Next.PendingSince.Equal(testSlot.Add(2*time.Minute)) {
			t.Fatalf("%s slot did not break the chain: %#v", gap.Quality, next)
		}
	}
}

func TestDecideNeverRecordsSufficientQualityWithoutAValue(t *testing.T) {
	// statePut trusts this invariant instead of checking it, so a failure here
	// would write a record whose quality claims a value it does not carry.
	rule := testRule()
	pending := Decide(rule, State{}, sufficient(testSlot, true), time.Minute).Next
	active := Decide(rule, pending, sufficient(testSlot.Add(time.Minute), true), time.Minute).Next
	legacyAbsent := State{Mode: ModeActive, ActiveEventID: "alert_1", ActiveStatus: StatusOpen, LastQuality: QualitySufficient}
	slot := testSlot.Add(2 * time.Minute)
	for _, current := range []State{{}, pending, active, legacyAbsent} {
		for _, quality := range []Quality{QualitySufficient, QualityInsufficientData, QualityStaleData, QualityQueryError, ""} {
			for _, value := range []Measurement{{}, Measured(0), Measured(7)} {
				for _, breached := range []bool{false, true} {
					evaluation := Evaluation{Slot: slot, Quality: quality, Value: value, Breached: breached}
					next := Decide(rule, current, evaluation, time.Minute).Next
					if next.LastQuality == QualitySufficient && !next.LastValue.IsPresent() {
						t.Fatalf("Decide(%#v, %#v) recorded sufficient without a value: %#v", current, evaluation, next)
					}
					if evaluation.IsDeterminate() && next.LastValue != value {
						t.Fatalf("Decide(%#v) kept %v, want the determinate value %v", evaluation, next.LastValue, value)
					}
				}
			}
		}
	}
}
