package alertevaluator

import (
	"reflect"
	"testing"
	"time"
)

func TestEvaluationBucketGoldenVectors(t *testing.T) {
	tests := []struct {
		tenant string
		rule   string
		want   int
	}{
		{"tnt_1", "rule_1", 29},
		{"tnt_alpha", "rule_beta", 6},
	}
	for _, tt := range tests {
		if got := EvaluationBucket(tt.tenant, tt.rule); got != tt.want {
			t.Fatalf("EvaluationBucket(%q, %q) = %d, want %d", tt.tenant, tt.rule, got, tt.want)
		}
	}
}

func TestOwnedBucketsAreIndependentOfPersistedBucketCount(t *testing.T) {
	if got, want := OwnedBuckets(1, 3), []int{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 58, 61}; !reflect.DeepEqual(got, want) {
		t.Fatalf("OwnedBuckets() = %v, want %v", got, want)
	}
}

func TestOwnedBucketsRejectInvalidShard(t *testing.T) {
	for _, input := range [][2]int{{0, 0}, {-1, 1}, {1, 1}, {0, 65}} {
		if _, err := ValidateShard(input[0], input[1]); err == nil {
			t.Fatalf("ValidateShard(%d, %d) succeeded", input[0], input[1])
		}
	}
}

func TestLatestCompleteSlotIsStableWithinCadence(t *testing.T) {
	evaluationTime := time.Date(2026, 7, 15, 12, 0, 30, 0, time.UTC)
	want := time.Date(2026, 7, 15, 11, 59, 45, 0, time.UTC)
	if got := LatestCompleteSlot(evaluationTime, time.Minute, 15*time.Second); !got.Equal(want) {
		t.Fatalf("LatestCompleteSlot() = %s, want %s", got, want)
	}
}
