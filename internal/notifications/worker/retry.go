package worker

import "time"

const (
	MaxProviderCalls = 5
	RetryBase        = 30 * time.Second
	RetryMaximum     = 15 * time.Minute
	AmbiguousGrace   = 2 * time.Minute
)

func CanCallProvider(attemptCount int) bool {
	return attemptCount >= 0 && attemptCount < MaxProviderCalls
}

func RetryCap(retryIndex int) time.Duration {
	if retryIndex < 1 {
		return 0
	}
	cap := RetryBase
	for index := 1; index < retryIndex && cap < RetryMaximum; index++ {
		if cap > RetryMaximum/2 {
			return RetryMaximum
		}
		cap *= 2
	}
	if cap > RetryMaximum {
		return RetryMaximum
	}
	return cap
}

func FullJitter(retryIndex int, fraction float64) time.Duration {
	if fraction < 0 {
		fraction = 0
	}
	if fraction >= 1 {
		fraction = 0.9999999999999999
	}
	return time.Duration(float64(RetryCap(retryIndex)) * fraction)
}

func RetryDelay(retryIndex int, ambiguous bool, fraction float64) time.Duration {
	delay := FullJitter(retryIndex, fraction)
	if ambiguous {
		delay += AmbiguousGrace
	}
	return delay
}
