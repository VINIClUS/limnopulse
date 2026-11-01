package workerproc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// LookupEnv reads one environment variable; os.LookupEnv in production.
type LookupEnv func(string) (string, bool)

func lookupOrOS(lookup LookupEnv) LookupEnv {
	if lookup == nil {
		return os.LookupEnv
	}
	return lookup
}

// Notifier sends sd_notify datagrams to NOTIFY_SOCKET. Without a socket (local runs)
// every send is a no-op.
type Notifier struct {
	socket string
}

func NotifierFromEnv(lookup LookupEnv) (*Notifier, error) {
	socket, _ := lookupOrOS(lookup)("NOTIFY_SOCKET")
	if socket == "" {
		return &Notifier{}, nil
	}
	if !strings.HasPrefix(socket, "/") && !strings.HasPrefix(socket, "@") {
		return nil, Configuration(errors.New("NOTIFY_SOCKET must be an absolute or abstract socket"))
	}
	return &Notifier{socket: socket}, nil
}

func (notifier *Notifier) Enabled() bool {
	return notifier != nil && notifier.socket != ""
}

func (notifier *Notifier) Ready() error    { return notifier.send("READY=1") }
func (notifier *Notifier) Stopping() error { return notifier.send("STOPPING=1") }
func (notifier *Notifier) Watchdog() error { return notifier.send("WATCHDOG=1") }

func (notifier *Notifier) send(state string) error {
	if !notifier.Enabled() {
		return nil
	}
	// Go maps a leading @ to the Linux abstract namespace.
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: notifier.socket, Net: "unixgram"})
	if err != nil {
		return fmt.Errorf("sd_notify dial: %w", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("sd_notify write: %w", err)
	}
	return nil
}

// WatchdogTimeout returns WATCHDOG_USEC when systemd armed a watchdog for this process,
// and zero when none is armed or WATCHDOG_PID names another process.
func WatchdogTimeout(lookup LookupEnv, pid int) (time.Duration, error) {
	lookup = lookupOrOS(lookup)
	raw, ok := lookup("WATCHDOG_USEC")
	if !ok || raw == "" {
		return 0, nil
	}
	if target, ok := lookup("WATCHDOG_PID"); ok && target != "" {
		targetPID, err := strconv.Atoi(target)
		if err != nil {
			return 0, Configuration(errors.New("WATCHDOG_PID must be an integer"))
		}
		if targetPID != pid {
			return 0, nil
		}
	}
	usec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || usec <= 0 {
		return 0, Configuration(errors.New("WATCHDOG_USEC must be a positive integer"))
	}
	return time.Duration(usec) * time.Microsecond, nil
}
