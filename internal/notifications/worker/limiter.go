package worker

import (
	"fmt"
	"math"

	"golang.org/x/time/rate"
)

func NewTokenLimiter(eventsPerSecond float64, burst int) (Limiter, error) {
	if eventsPerSecond <= 0 || math.IsNaN(eventsPerSecond) || math.IsInf(eventsPerSecond, 0) || burst < 1 {
		return nil, fmt.Errorf("notification send rate and burst must be positive")
	}
	return rate.NewLimiter(rate.Limit(eventsPerSecond), burst), nil
}
