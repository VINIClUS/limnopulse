package notifications

import "fmt"

const RelaySchemaVersion int64 = 1

type OutboxStatus string

const (
	OutboxStatusReady   OutboxStatus = "ready"
	OutboxStatusBlocked OutboxStatus = "blocked"
)

func (status OutboxStatus) Validate() error {
	switch status {
	case OutboxStatusReady, OutboxStatusBlocked:
		return nil
	default:
		return fmt.Errorf("unknown notification outbox status %q", status)
	}
}

func ClassifyOutboxRelayWork(
	kind NotificationKind,
	status OutboxStatus,
) (WorkKind, error) {
	if err := kind.Validate(); err != nil {
		return "", err
	}
	if err := status.Validate(); err != nil {
		return "", err
	}
	switch {
	case kind == NotificationKindOpening && status == OutboxStatusReady:
		return WorkKindIntent, nil
	case kind == NotificationKindRecovery && status == OutboxStatusBlocked:
		return WorkKindDependency, nil
	default:
		return "", fmt.Errorf("notification outbox kind and status are inconsistent")
	}
}

func OwnedRelayBuckets(shard, shardCount int) ([]int, error) {
	if shardCount < 1 || shardCount > RelayBucketCount {
		return nil, fmt.Errorf("shard count must be between 1 and %d", RelayBucketCount)
	}
	if shard < 0 || shard >= shardCount {
		return nil, fmt.Errorf("shard must be between 0 and shard count minus 1")
	}
	buckets := make([]int, 0, RelayBucketCount/shardCount+1)
	for bucket := 0; bucket < RelayBucketCount; bucket++ {
		if bucket%shardCount == shard {
			buckets = append(buckets, bucket)
		}
	}
	return buckets, nil
}
