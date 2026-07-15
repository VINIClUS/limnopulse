package alertevaluator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

type Quality string

const (
	QualitySufficient       Quality = "sufficient"
	QualityInsufficientData Quality = "insufficient_data"
	QualityStaleData        Quality = "stale_data"
	QualityQueryError       Quality = "query_error"
)

type Mode string

const (
	ModeHealthy Mode = "healthy"
	ModePending Mode = "pending"
	ModeActive  Mode = "active"
)

type EventStatus string

const (
	StatusOpen         EventStatus = "open"
	StatusAcknowledged EventStatus = "acknowledged"
	StatusSuppressed   EventStatus = "suppressed"
	StatusResolved     EventStatus = "resolved"
)

type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelTelegram Channel = "telegram"
)

type Transition string

const (
	TransitionNone       Transition = "none"
	TransitionOpened     Transition = "opened"
	TransitionSuppressed Transition = "suppressed"
	TransitionRecovered  Transition = "recovered"
)

type OutboxKind string

const (
	OutboxOpening  OutboxKind = "opening"
	OutboxRecovery OutboxKind = "recovery"
)

type OutboxStatus string

const (
	OutboxReady   OutboxStatus = "ready"
	OutboxBlocked OutboxStatus = "blocked"
)

type Rule struct {
	TenantID           string
	RuleID             string
	Version            int64
	EvaluationRevision int64
	Duration           time.Duration
	Cooldown           time.Duration
	Channels           []Channel
}

type Evaluation struct {
	Slot        time.Time
	Quality     Quality
	Breached    bool
	Value       float64
	MissedSlots int
}

type State struct {
	Mode                     Mode
	ConfirmedSlots           int
	PendingSince             time.Time
	LastBreachSlot           time.Time
	LastEvaluatedSlot        time.Time
	LastQuality              Quality
	LastValue                float64
	ActiveEventID            string
	ActiveStatus             EventStatus
	ActiveOpenedAt           time.Time
	OpeningOutboxes          map[Channel]string
	CooldownUntil            time.Time
	LastNotifiedEventID      string
	SuppressionSourceEventID string
}

type OutboxDecision struct {
	OutboxID          string
	Channel           Channel
	Kind              OutboxKind
	Status            OutboxStatus
	DependsOnOutboxID string
}

type Decision struct {
	Next            State
	Transition      Transition
	EventID         string
	ResolvedEventID string
	Outboxes        []OutboxDecision
}

func Decide(rule Rule, current State, evaluation Evaluation, cadence time.Duration) Decision {
	next := cloneState(current)
	if next.Mode == "" {
		next.Mode = ModeHealthy
	}
	next.LastEvaluatedSlot = evaluation.Slot
	next.LastQuality = evaluation.Quality

	if evaluation.MissedSlots > 0 && next.Mode == ModePending {
		resetPending(&next)
	}
	if evaluation.Quality != QualitySufficient {
		return Decision{Next: next, Transition: TransitionNone}
	}
	next.LastValue = evaluation.Value

	if next.Mode == ModeActive {
		if evaluation.Breached {
			return Decision{Next: next, Transition: TransitionNone}
		}
		return recoverIncident(rule, next, evaluation)
	}

	if !evaluation.Breached {
		resetPending(&next)
		return Decision{Next: next, Transition: TransitionNone}
	}

	consecutive := next.Mode == ModePending &&
		evaluation.MissedSlots == 0 &&
		!next.LastBreachSlot.IsZero() &&
		next.LastBreachSlot.Add(cadence).Equal(evaluation.Slot)
	if consecutive {
		next.ConfirmedSlots++
	} else {
		next.Mode = ModePending
		next.ConfirmedSlots = 1
		next.PendingSince = evaluation.Slot
	}
	next.LastBreachSlot = evaluation.Slot

	if next.ConfirmedSlots < requiredSlots(rule.Duration, cadence) {
		return Decision{Next: next, Transition: TransitionNone}
	}
	return openIncident(rule, next, evaluation)
}

func openIncident(rule Rule, next State, evaluation Evaluation) Decision {
	eventID := EventID(rule, evaluation.Slot)
	next.Mode = ModeActive
	next.ActiveEventID = eventID
	next.ActiveOpenedAt = evaluation.Slot
	next.ConfirmedSlots = 0
	next.PendingSince = time.Time{}
	next.OpeningOutboxes = map[Channel]string{}

	if next.CooldownUntil.After(evaluation.Slot) {
		next.ActiveStatus = StatusSuppressed
		next.SuppressionSourceEventID = next.LastNotifiedEventID
		return Decision{Next: next, Transition: TransitionSuppressed, EventID: eventID}
	}

	next.ActiveStatus = StatusOpen
	next.SuppressionSourceEventID = ""
	next.CooldownUntil = evaluation.Slot.Add(rule.Cooldown)
	next.LastNotifiedEventID = eventID
	outboxes := make([]OutboxDecision, 0, len(rule.Channels))
	for _, channel := range rule.Channels {
		outboxID := OutboxID(eventID, channel, OutboxOpening)
		next.OpeningOutboxes[channel] = outboxID
		outboxes = append(outboxes, OutboxDecision{
			OutboxID: outboxID,
			Channel:  channel,
			Kind:     OutboxOpening,
			Status:   OutboxReady,
		})
	}
	return Decision{Next: next, Transition: TransitionOpened, EventID: eventID, Outboxes: outboxes}
}

func recoverIncident(rule Rule, next State, evaluation Evaluation) Decision {
	resolvedEventID := next.ActiveEventID
	outboxes := make([]OutboxDecision, 0, len(next.OpeningOutboxes))
	if next.ActiveStatus != StatusSuppressed {
		channels := make([]Channel, 0, len(next.OpeningOutboxes))
		for channel := range next.OpeningOutboxes {
			channels = append(channels, channel)
		}
		sort.Slice(channels, func(i, j int) bool { return channels[i] < channels[j] })
		for _, channel := range channels {
			openingID := next.OpeningOutboxes[channel]
			if openingID == "" {
				continue
			}
			outboxes = append(outboxes, OutboxDecision{
				OutboxID:          OutboxID(resolvedEventID, channel, OutboxRecovery),
				Channel:           channel,
				Kind:              OutboxRecovery,
				Status:            OutboxBlocked,
				DependsOnOutboxID: openingID,
			})
		}
	}
	next.Mode = ModeHealthy
	next.ActiveEventID = ""
	next.ActiveStatus = ""
	next.ActiveOpenedAt = time.Time{}
	next.OpeningOutboxes = nil
	next.SuppressionSourceEventID = ""
	next.LastBreachSlot = time.Time{}
	return Decision{
		Next:            next,
		Transition:      TransitionRecovered,
		ResolvedEventID: resolvedEventID,
		Outboxes:        outboxes,
	}
}

func requiredSlots(duration, cadence time.Duration) int {
	if duration <= cadence {
		return 1
	}
	return int((duration + cadence - 1) / cadence)
}

func resetPending(state *State) {
	state.Mode = ModeHealthy
	state.ConfirmedSlots = 0
	state.PendingSince = time.Time{}
	state.LastBreachSlot = time.Time{}
}

func cloneState(state State) State {
	if state.OpeningOutboxes == nil {
		return state
	}
	original := state.OpeningOutboxes
	state.OpeningOutboxes = make(map[Channel]string, len(original))
	for channel, outboxID := range original {
		state.OpeningOutboxes[channel] = outboxID
	}
	return state
}

func EventID(rule Rule, confirmedOpenWindowEnd time.Time) string {
	canonical := fmt.Sprintf(
		"%s\x00%s\x00%d\x00%d\x00%s",
		rule.TenantID,
		rule.RuleID,
		rule.Version,
		rule.EvaluationRevision,
		FixedUTCTimestamp(confirmedOpenWindowEnd),
	)
	return "alert_" + sha256Hex(canonical)
}

func OutboxID(eventID string, channel Channel, kind OutboxKind) string {
	return "outbox_" + sha256Hex(eventID+"\x00"+string(channel)+"\x00"+string(kind))
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
