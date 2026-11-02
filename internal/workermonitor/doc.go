// Package workermonitor publishes ADR-020 unit health. For every role whose unit is
// enabled it reads the unit state through systemctl and sends one Limnopulse/Workers
// UnitHealthy datum (1 or 0) to CloudWatch. It is one-shot: a systemd timer runs it once
// per schedule interval, and it keeps no loop, sleep or state of its own.
//
// # Health rules
//
// A role publishes only when its service (continuous) or its timer (one-shot) has
// UnitFileState=enabled. A continuous role is healthy while its service is
// active/running. A one-shot role is healthy when its timer is active and its last
// finished run exited with a code in oneshot_success_exit_codes, with Result=success,
// at most oneshot_stale_after_intervals schedules ago. systemd clears Result and every
// ExecMain* property when a run starts, so a one-shot sampled mid-run reads unhealthy:
// its previous outcome is no longer visible. worker-monitor reports itself as healthy
// while its own timer is active, because it is always mid-run when it looks.
//
// # What the units must provide
//
// These rules give the right answer only when the units are built and enabled this way:
//
//   - Order limnopulse-worker-monitor.service After= every other one-shot service
//     (limnopulse-alert-evaluator.service and limnopulse-notification-relay.service).
//     systemd holds a start job while an After= unit still has a job installed, so a
//     monitor run queued during another one-shot's run waits for it to finish and
//     samples it between runs.
//   - Offset the monitor's timer from the other one-shot timers, for example by half an
//     interval, and set AccuracySec=1s. Timers that elapse in the same accuracy window
//     are coalesced and would start together.
//   - Give every continuous service and every one-shot timer an [Install] section, and
//     enable a role persistently with systemctl enable. The gate reads
//     UnitFileState=enabled: a unit that is only linked reads "linked", one enabled with
//     --runtime reads "enabled-runtime", and neither publishes.
//   - Allow AF_UNIX and the system D-Bus socket in the monitor's sandbox. As a
//     non-root user, systemctl show reaches systemd over D-Bus. dbus-daemon drops a
//     user it cannot resolve, and it cannot resolve a dynamic one, so the unit names
//     a static user (ops/workers/README.md, "Units").
//   - Set AWS_REGION in the monitor's env file. Without a region it exits 78.
//   - Ship the whole role table as roles.json, json.dumps(yaml.safe_load(roles.yaml)), at
//     DefaultTablePath. ParseTable must accept the shipped file.
//   - Record the mid-run caveat in ops/workers/README.md "Unit health" with the units.
//
// # What the alarms must tolerate
//
// The monitor publishes 0 by design for short spells: a one-shot sampled mid-run (still
// possible as a race despite the ordering above), a one-shot with no finished run in
// the first interval after boot or enable, a continuous service while it is activating,
// and a one-shot whose exit time lies ahead of a wall clock that stepped back. The datum
// is stamped with the time of the run, so timer jitter can also leave a one-minute
// period without a datapoint, which treat_missing_data = breaching counts as a breach.
// Each unit-health alarm therefore evaluates several periods with an M-of-N rule, for
// example 2 of 3 one-minute periods or a period of at least 120 s. It keeps
// treat_missing_data = breaching and sets no unit: data points carry Unit=None.
//
// The monitor's IAM principal needs only cloudwatch:PutMetricData, conditioned on
// cloudwatch:namespace = Limnopulse/Workers.
package workermonitor
