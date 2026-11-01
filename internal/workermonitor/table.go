// Package workermonitor publishes ADR-020 unit health. For every role whose unit is
// enabled it reads the unit state through systemctl and sends one Limnopulse/Workers
// UnitHealthy datum (1 or 0) to CloudWatch. It is one-shot: a systemd timer runs it once
// per schedule interval, and it keeps no loop, sleep or state of its own.
package workermonitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// DefaultTablePath is paths.current + "/" + bundle.role_table in ops/workers/roles.yaml.
// The table cannot locate itself, so a test pins this constant to roles.yaml instead.
const DefaultTablePath = "/opt/limnopulse/workers/current/roles.json"

// SelfRole is the role this process runs as. It reports its own unit health too.
const SelfRole = "worker-monitor"

const (
	LifecycleContinuous = "continuous"
	LifecycleOneshot    = "oneshot"
	UnitHealthSystemd   = "systemd"
	UnitHealthNone      = "none"

	supportedSchemaVersion = 1
	publishedWhenEnabled   = "unit_file_enabled"
)

// Table is the part of roles.json (roles.yaml rendered as JSON) the monitor reads. Every
// other field of the role table is ignored.
type Table struct {
	SchemaVersion int        `json:"schema_version"`
	UnitHealth    UnitHealth `json:"unit_health"`
	Roles         []Role     `json:"roles"`
}

// UnitHealth is the table's top-level unit_health block: the metric names and the rules.
type UnitHealth struct {
	Namespace                  string `json:"namespace"`
	Metric                     string `json:"metric"`
	Dimension                  string `json:"dimension"`
	OneshotStaleAfterIntervals int    `json:"oneshot_stale_after_intervals"`
	OneshotSuccessExitCodes    []int  `json:"oneshot_success_exit_codes"`
	PublishedWhen              string `json:"published_when"`
}

// Role is one row of the table. Its unit_health is a string ("systemd" or "none"), unlike
// the table's top-level unit_health block.
type Role struct {
	Name            string `json:"name"`
	Lifecycle       string `json:"lifecycle"`
	ScheduleSeconds int    `json:"schedule_seconds"`
	Units           Units  `json:"units"`
	UnitHealth      string `json:"unit_health"`
}

type Units struct {
	Service string `json:"service"`
	Timer   string `json:"timer"`
}

var (
	roleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	// Unit names reach systemctl as arguments, after "--". They still may not look like
	// an option.
	unitNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@:-]*$`)
)

// LoadTable reads and validates the role table. Every failure is a configuration error.
func LoadTable(path string) (Table, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Table{}, workerproc.Configuration(fmt.Errorf("read role table: %w", err))
	}
	return ParseTable(raw)
}

// ParseTable decodes and validates roles.json. Every failure is a configuration error.
func ParseTable(raw []byte) (Table, error) {
	var table Table
	if err := json.Unmarshal(raw, &table); err != nil {
		return Table{}, workerproc.Configuration(fmt.Errorf("decode role table: %w", err))
	}
	if err := table.validate(); err != nil {
		return Table{}, workerproc.Configuration(fmt.Errorf("role table: %w", err))
	}
	return table, nil
}

// Monitored returns the roles with systemd unit health, in table order. Roles with
// unit_health none (the operator CLI and the Compose ingest worker) are skipped.
func (table Table) Monitored() []Role {
	var roles []Role
	for _, role := range table.Roles {
		if role.UnitHealth == UnitHealthSystemd {
			roles = append(roles, role)
		}
	}
	return roles
}

func (table Table) validate() error {
	if table.SchemaVersion != supportedSchemaVersion {
		return fmt.Errorf("schema_version %d is not %d", table.SchemaVersion, supportedSchemaVersion)
	}
	if err := table.UnitHealth.validate(); err != nil {
		return err
	}
	seen := make(map[string]bool, len(table.Roles))
	for _, role := range table.Roles {
		if err := role.validate(); err != nil {
			return err
		}
		if seen[role.Name] {
			return fmt.Errorf("role %s is listed twice", role.Name)
		}
		seen[role.Name] = true
	}
	for _, role := range table.Monitored() {
		if role.Name == SelfRole && role.Lifecycle == LifecycleOneshot {
			return nil
		}
	}
	return fmt.Errorf("role %s must be a one-shot role with systemd unit health", SelfRole)
}

func (health UnitHealth) validate() error {
	if health.Namespace == "" || health.Metric == "" || health.Dimension == "" {
		return errors.New("unit_health needs a namespace, a metric and a dimension")
	}
	if health.PublishedWhen != publishedWhenEnabled {
		return fmt.Errorf("unit_health published_when %q is not %q", health.PublishedWhen, publishedWhenEnabled)
	}
	if health.OneshotStaleAfterIntervals < 1 {
		return errors.New("unit_health oneshot_stale_after_intervals must be at least 1")
	}
	if len(health.OneshotSuccessExitCodes) == 0 {
		return errors.New("unit_health oneshot_success_exit_codes is empty")
	}
	for _, code := range health.OneshotSuccessExitCodes {
		if code < 0 || code > 255 {
			return fmt.Errorf("unit_health exit code %d is not 0-255", code)
		}
	}
	return nil
}

func (role Role) validate() error {
	if !roleNamePattern.MatchString(role.Name) {
		return fmt.Errorf("invalid role name %q", role.Name)
	}
	switch role.UnitHealth {
	case UnitHealthNone:
		return nil
	case UnitHealthSystemd:
	default:
		return fmt.Errorf("role %s: unit_health %q is not %s or %s", role.Name, role.UnitHealth, UnitHealthSystemd, UnitHealthNone)
	}
	if !validUnit(role.Units.Service, ".service") {
		return fmt.Errorf("role %s: invalid service unit %q", role.Name, role.Units.Service)
	}
	switch role.Lifecycle {
	case LifecycleContinuous:
		return nil
	case LifecycleOneshot:
		if !validUnit(role.Units.Timer, ".timer") {
			return fmt.Errorf("role %s: invalid timer unit %q", role.Name, role.Units.Timer)
		}
		if role.ScheduleSeconds < 1 {
			return fmt.Errorf("role %s: schedule_seconds must be at least 1", role.Name)
		}
		return nil
	default:
		return fmt.Errorf("role %s: lifecycle %q has no systemd unit health", role.Name, role.Lifecycle)
	}
}

func validUnit(name, suffix string) bool {
	return unitNamePattern.MatchString(name) && strings.HasSuffix(name, suffix)
}
