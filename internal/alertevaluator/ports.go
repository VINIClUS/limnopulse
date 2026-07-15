package alertevaluator

import (
	"context"
	"errors"
	"time"
)

var ErrLeaseConflict = errors.New("alert evaluation lease conflict")

type Candidate struct {
	PK     string
	SK     string
	RuleID string
	Bucket int
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

type LeaseRequest struct {
	Owner      string
	Now        time.Time
	ExpiresAt  time.Time
	DueThrough time.Time
}

type EvaluationRule struct {
	Rule
	Name        string
	PondID      string
	DeviceID    string
	Metric      string
	Operator    string
	Threshold   float64
	Aggregation Aggregation
	Window      time.Duration
	Severity    string
}

type Work struct {
	PK               string
	SK               string
	Rule             EvaluationRule
	NextEvaluationAt time.Time
	LeaseOwner       string
	LeaseEpoch       int64
}

type VersionedState struct {
	State    State
	Revision int64
}

type CommitRequest struct {
	Work          Work
	PreviousState VersionedState
	Evaluation    Evaluation
	Decision      Decision
	Slot          time.Time
	NextDue       time.Time
}

type WorkStore interface {
	QueryDue(context.Context, DueRequest) (DuePage, error)
	Claim(context.Context, Candidate, LeaseRequest) (Work, error)
	LoadState(context.Context, Work) (VersionedState, error)
	Commit(context.Context, CommitRequest) error
}

type WindowReader interface {
	Evaluate(context.Context, EvaluationRule, WindowSpec) (WindowResult, error)
}
