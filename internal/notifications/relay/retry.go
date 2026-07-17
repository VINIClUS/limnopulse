package relay

import (
	"errors"
	"fmt"
	"time"
)

type RetryAtError struct {
	At time.Time
}

func (err *RetryAtError) Error() string {
	return fmt.Sprintf("relay work is not ready before %s", err.At.UTC().Format(time.RFC3339Nano))
}

func RetryAt(err error) (time.Time, bool) {
	var retry *RetryAtError
	if !errors.As(err, &retry) || retry.At.IsZero() {
		return time.Time{}, false
	}
	return retry.At.UTC(), true
}
