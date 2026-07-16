package notifications

import "fmt"

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
