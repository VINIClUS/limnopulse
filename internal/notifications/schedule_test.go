package notifications

import (
	"reflect"
	"testing"
)

func TestOwnedRelayBucketsUseStableModuloSharding(t *testing.T) {
	got, err := OwnedRelayBuckets(1, 3)
	if err != nil {
		t.Fatalf("OwnedRelayBuckets() error = %v", err)
	}
	want := []int{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 58, 61}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OwnedRelayBuckets() = %v, want %v", got, want)
	}
}

func TestOwnedRelayBucketsValidateShardBounds(t *testing.T) {
	for _, input := range [][2]int{{0, 0}, {0, 65}, {-1, 1}, {1, 1}, {3, 3}} {
		if _, err := OwnedRelayBuckets(input[0], input[1]); err == nil {
			t.Fatalf("OwnedRelayBuckets(%d, %d) succeeded", input[0], input[1])
		}
	}
}

func TestEachRelayBucketHasExactlyOneOwner(t *testing.T) {
	owners := make([]int, RelayBucketCount)
	for shard := 0; shard < 7; shard++ {
		buckets, err := OwnedRelayBuckets(shard, 7)
		if err != nil {
			t.Fatalf("OwnedRelayBuckets(%d, 7): %v", shard, err)
		}
		for _, bucket := range buckets {
			owners[bucket]++
		}
	}
	for bucket, count := range owners {
		if count != 1 {
			t.Fatalf("bucket %d owner count = %d, want 1", bucket, count)
		}
	}
}
