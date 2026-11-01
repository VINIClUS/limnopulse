package workerproc

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func env(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// listenNotify stands in for systemd's NOTIFY_SOCKET. The directory stays short
// because unix socket paths are limited to 108 bytes.
func listenNotify(t *testing.T) (string, <-chan string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "wp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "notify")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	messages := make(chan string, 256)
	go func() {
		buffer := make([]byte, 256)
		for {
			n, _, err := conn.ReadFromUnix(buffer)
			if err != nil {
				close(messages)
				return
			}
			messages <- string(buffer[:n])
		}
	}()
	return path, messages
}

func expectMessage(t *testing.T, messages <-chan string, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-messages:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("no %q notification", want)
		}
	}
}

func TestNotifierSendsStatesToThePathSocket(t *testing.T) {
	path, messages := listenNotify(t)
	notifier, err := NotifierFromEnv(env(map[string]string{"NOTIFY_SOCKET": path}))
	if err != nil {
		t.Fatal(err)
	}
	for _, send := range []func() error{notifier.Ready, notifier.Watchdog, notifier.Stopping} {
		if err := send(); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"READY=1", "WATCHDOG=1", "STOPPING=1"} {
		if got := <-messages; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestNotifierReachesAnAbstractSocket(t *testing.T) {
	name := "@limnopulse-workerproc-" + strconv.Itoa(os.Getpid())
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: name, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	notifier, err := NotifierFromEnv(env(map[string]string{"NOTIFY_SOCKET": name}))
	if err != nil {
		t.Fatal(err)
	}
	if err := notifier.Ready(); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _, err := conn.ReadFromUnix(buffer)
	if err != nil || string(buffer[:n]) != "READY=1" {
		t.Fatalf("got %q, %v", buffer[:n], err)
	}
}

func TestNotifierWithoutSocketIsANoOp(t *testing.T) {
	notifier, err := NotifierFromEnv(env(nil))
	if err != nil || notifier.Enabled() {
		t.Fatalf("notifier enabled without NOTIFY_SOCKET: %v", err)
	}
	if err := notifier.Ready(); err != nil {
		t.Fatal(err)
	}
}

func TestNotifierRejectsARelativeSocket(t *testing.T) {
	_, err := NotifierFromEnv(env(map[string]string{"NOTIFY_SOCKET": "run/notify"}))
	if !IsConfiguration(err) {
		t.Fatalf("relative NOTIFY_SOCKET must be a configuration error, got %v", err)
	}
}

func TestWatchdogTimeout(t *testing.T) {
	pid := os.Getpid()
	cases := []struct {
		name    string
		values  map[string]string
		want    time.Duration
		wantErr bool
	}{
		{"unset", nil, 0, false},
		{"armed", map[string]string{"WATCHDOG_USEC": "120000000"}, 120 * time.Second, false},
		{"armed for this pid", map[string]string{"WATCHDOG_USEC": "2000000", "WATCHDOG_PID": strconv.Itoa(pid)}, 2 * time.Second, false},
		{"armed for another pid", map[string]string{"WATCHDOG_USEC": "2000000", "WATCHDOG_PID": strconv.Itoa(pid + 1)}, 0, false},
		{"not a number", map[string]string{"WATCHDOG_USEC": "soon"}, 0, true},
		{"zero", map[string]string{"WATCHDOG_USEC": "0"}, 0, true},
		{"bad pid", map[string]string{"WATCHDOG_USEC": "1", "WATCHDOG_PID": "me"}, 0, true},
	}
	for _, tc := range cases {
		got, err := WatchdogTimeout(env(tc.values), pid)
		if tc.wantErr {
			if !IsConfiguration(err) {
				t.Errorf("%s: want configuration error, got %v", tc.name, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: got %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}
