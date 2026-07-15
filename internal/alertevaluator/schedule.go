package alertevaluator

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

const EvaluationBucketCount = 64

func EvaluationBucket(tenantID, ruleID string) int {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + ruleID))
	return int(binary.BigEndian.Uint64(digest[:8]) % EvaluationBucketCount)
}

func ValidateShard(shard, shardCount int) ([]int, error) {
	if shardCount < 1 || shardCount > EvaluationBucketCount {
		return nil, fmt.Errorf("shard-count must be between 1 and %d", EvaluationBucketCount)
	}
	if shard < 0 || shard >= shardCount {
		return nil, fmt.Errorf("shard must be between 0 and shard-count-1")
	}
	buckets := make([]int, 0, EvaluationBucketCount/shardCount+1)
	for bucket := 0; bucket < EvaluationBucketCount; bucket++ {
		if bucket%shardCount == shard {
			buckets = append(buckets, bucket)
		}
	}
	return buckets, nil
}

func OwnedBuckets(shard, shardCount int) []int {
	buckets, _ := ValidateShard(shard, shardCount)
	return buckets
}

func LatestCompleteSlot(evaluationTime time.Time, cadence, allowedLateness time.Duration) time.Time {
	return evaluationTime.UTC().Truncate(cadence).Add(-allowedLateness)
}

func FixedUTCTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
