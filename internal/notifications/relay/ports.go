package relay

import (
	"context"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

type Candidate struct {
	PK          string
	SK          string
	RelayPK     string
	RelaySK     string
	Kind        notifications.WorkKind
	AvailableAt time.Time
}

type DueRequest struct {
	Bucket     int
	DueThrough time.Time
	PageSize   int
	NextToken  string
}

type DuePage struct {
	Candidates []Candidate
	NextToken  string
}

type Work struct {
	Candidate
	TenantID            string
	ItemID              string
	OutboxID            string
	DeliveryID          string
	EventID             string
	RuleID              string
	DependsOnOutboxID   string
	NotificationKind    notifications.NotificationKind
	Channel             notifications.Channel
	State               string
	Revision            int64
	Cursor              string
	ExpansionStartedAt  time.Time
	RecipientsExamined  int
	DeliveriesCreated   int
	DeliveriesCancelled int
	RecipientsFiltered  int
	LeaseOwner          string
	LeaseEpoch          int64
	Traceparent         string
}

type LeaseRequest struct {
	Owner      string
	Now        time.Time
	ExpiresAt  time.Time
	DueThrough time.Time
}

type ExpandRequest struct {
	RelayTime time.Time
	PageSize  int
}

type WorkResult struct {
	DeliveriesCreated   int
	DeliveriesCancelled int
	RecipientsExamined  int
	RecipientsFiltered  int
	FilteredBySeverity  int
}

type QueuedResult struct {
	QueuedAt  time.Time
	MessageID string
}

type PublishRequest struct {
	Job         notifications.JobEnvelope
	Traceparent string
}

type WorkStore interface {
	QueryDue(context.Context, DueRequest) (DuePage, error)
	Reload(context.Context, Candidate, time.Time) (Work, bool, error)
	Claim(context.Context, Work, LeaseRequest) (Work, bool, error)
	ExpandIntent(context.Context, Work, ExpandRequest) (WorkResult, error)
	ExpandDependency(context.Context, Work, ExpandRequest) (WorkResult, error)
	MarkQueued(context.Context, Work, QueuedResult) error
	Reschedule(context.Context, Work, time.Time) error
}

type Publisher interface {
	Publish(context.Context, PublishRequest) (string, error)
}
