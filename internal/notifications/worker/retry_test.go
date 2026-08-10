package worker

import (
	"testing"
	"time"
)

func TestFullJitterBoundsAndAmbiguousGrace(t *testing.T) {
	tests := []struct {
		attempt int
		wantCap time.Duration
	}{
		{attempt: 1, wantCap: 30 * time.Second},
		{attempt: 2, wantCap: time.Minute},
		{attempt: 5, wantCap: 8 * time.Minute},
		{attempt: 7, wantCap: 15 * time.Minute},
	}
	for _, test := range tests {
		if got := RetryCap(test.attempt); got != test.wantCap {
			t.Fatalf("RetryCap(%d) = %s, want %s", test.attempt, got, test.wantCap)
		}
		if got := FullJitter(test.attempt, 0); got != 0 {
			t.Fatalf("FullJitter(%d, 0) = %s", test.attempt, got)
		}
		if got := FullJitter(test.attempt, 0.999999); got < 0 || got >= test.wantCap {
			t.Fatalf("FullJitter(%d) = %s, cap %s", test.attempt, got, test.wantCap)
		}
	}
	if got := RetryDelay(3, true, 0.5); got != 2*time.Minute+time.Minute {
		t.Fatalf("ambiguous RetryDelay = %s", got)
	}
}

func TestRetryPolicyNeverMakesSixthProviderCall(t *testing.T) {
	for attempts := 0; attempts <= 7; attempts++ {
		want := attempts < MaxProviderCalls
		if got := CanCallProvider(attempts); got != want {
			t.Fatalf("CanCallProvider(%d) = %t, want %t", attempts, got, want)
		}
	}
}
