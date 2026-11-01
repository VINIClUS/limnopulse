package workermonitor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/VINIClUS/limnopulse/internal/workerproc"
)

// testdata/roles.json is ops/workers/roles.yaml rendered as JSON, as the workers bundle
// ships it. tests/unit/test_worker_monitor_table.py fails when the two differ; regenerate
// it with:
//
//	.venv/bin/python -c 'import json, yaml; print(json.dumps(yaml.safe_load(open("ops/workers/roles.yaml", encoding="utf-8")), indent=2, ensure_ascii=False))' > internal/workermonitor/testdata/roles.json
var testTablePath = filepath.Join("testdata", "roles.json")

var rolesYAMLPath = filepath.Join("..", "..", "ops", "workers", "roles.yaml")

func loadTestTable(t *testing.T) Table {
	t.Helper()
	table, err := LoadTable(testTablePath)
	if err != nil {
		t.Fatalf("load %s: %v", testTablePath, err)
	}
	return table
}

func tableRole(t *testing.T, table Table, name string) Role {
	t.Helper()
	for _, role := range table.Roles {
		if role.Name == name {
			return role
		}
	}
	t.Fatalf("role table has no role %s", name)
	return Role{}
}

func TestLoadTableReadsTheRenderedRoleTable(t *testing.T) {
	table := loadTestTable(t)
	var monitored []string
	for _, role := range table.Monitored() {
		monitored = append(monitored, role.Name)
	}
	want := []string{"alert-evaluator", "notification-relay", "email-worker", "ses-feedback-worker", "telegram-worker", SelfRole}
	if !slices.Equal(monitored, want) {
		t.Fatalf("monitored roles = %v, want %v (the CLI and the Compose worker have unit_health none)", monitored, want)
	}
	self := tableRole(t, table, SelfRole)
	if self.Lifecycle != LifecycleOneshot || self.ScheduleSeconds != 60 ||
		self.Units != (Units{Service: "limnopulse-worker-monitor.service", Timer: "limnopulse-worker-monitor.timer"}) {
		t.Fatalf("worker-monitor role = %+v", self)
	}
	email := tableRole(t, table, "email-worker")
	if email.Lifecycle != LifecycleContinuous || email.Units != (Units{Service: "limnopulse-email-worker.service"}) {
		t.Fatalf("email-worker role = %+v", email)
	}
}

// rolesYAML is what a line scan of roles.yaml finds: the two-space "key: value" lines of
// each top-level block and the role names in order. roles.yaml keeps one key per line, so
// the scan needs no YAML library.
type rolesYAML struct {
	blocks map[string]map[string]string
	roles  []string
}

var (
	yamlTopLevelKey = regexp.MustCompile(`^([a-z_]+):`)
	yamlBlockKey    = regexp.MustCompile(`^  ([a-z_]+): (.+)$`)
	yamlRoleName    = regexp.MustCompile(`^  - name: (\S+)$`)
)

func scanRolesYAML(t *testing.T) rolesYAML {
	t.Helper()
	raw, err := os.ReadFile(rolesYAMLPath)
	if err != nil {
		t.Fatalf("read %s: %v", rolesYAMLPath, err)
	}
	scanned := rolesYAML{blocks: make(map[string]map[string]string)}
	block := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if match := yamlTopLevelKey.FindStringSubmatch(line); match != nil {
			block = match[1]
			scanned.blocks[block] = make(map[string]string)
			continue
		}
		if block == "" {
			continue
		}
		if match := yamlBlockKey.FindStringSubmatch(line); match != nil {
			scanned.blocks[block][match[1]] = match[2]
		}
		if match := yamlRoleName.FindStringSubmatch(line); match != nil && block == "roles" {
			scanned.roles = append(scanned.roles, match[1])
		}
	}
	return scanned
}

// The metric names, the health rules and the table path all come from roles.yaml.
func TestTableAgreesWithRolesYAML(t *testing.T) {
	scanned := scanRolesYAML(t)
	table := loadTestTable(t)
	health := scanned.blocks["unit_health"]
	names := [3]string{health["namespace"], health["metric"], health["dimension"]}
	if names != [3]string{"Limnopulse/Workers", "UnitHealthy", "Role"} {
		t.Fatalf("roles.yaml unit_health names = %v", names)
	}
	if got := [3]string{table.UnitHealth.Namespace, table.UnitHealth.Metric, table.UnitHealth.Dimension}; got != names {
		t.Fatalf("decoded unit_health names = %v, roles.yaml has %v", got, names)
	}
	if got := fmt.Sprint(table.UnitHealth.OneshotStaleAfterIntervals); got != health["oneshot_stale_after_intervals"] {
		t.Fatalf("decoded oneshot_stale_after_intervals = %s, roles.yaml has %s", got, health["oneshot_stale_after_intervals"])
	}
	codes, err := json.Marshal(table.UnitHealth.OneshotSuccessExitCodes)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.ReplaceAll(health["oneshot_success_exit_codes"], " ", ""); string(codes) != want {
		t.Fatalf("decoded oneshot_success_exit_codes = %s, roles.yaml has %s", codes, want)
	}
	if table.UnitHealth.PublishedWhen != health["published_when"] {
		t.Fatalf("decoded published_when = %q, roles.yaml has %q", table.UnitHealth.PublishedWhen, health["published_when"])
	}
	var roles []string
	for _, role := range table.Roles {
		roles = append(roles, role.Name)
	}
	if len(scanned.roles) == 0 || !slices.Equal(roles, scanned.roles) {
		t.Fatalf("decoded roles = %v, roles.yaml has %v", roles, scanned.roles)
	}
	if want := scanned.blocks["paths"]["current"] + "/" + scanned.blocks["bundle"]["role_table"]; DefaultTablePath != want {
		t.Fatalf("DefaultTablePath = %s, roles.yaml paths.current/bundle.role_table = %s", DefaultTablePath, want)
	}
}

func readTableJSON(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(testTablePath)
	if err != nil {
		t.Fatal(err)
	}
	var table map[string]any
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	return table
}

func jsonUnitHealth(table map[string]any) map[string]any {
	return table["unit_health"].(map[string]any)
}

func jsonRole(table map[string]any, name string) map[string]any {
	for _, item := range table["roles"].([]any) {
		if role := item.(map[string]any); role["name"] == name {
			return role
		}
	}
	panic("role table has no role " + name)
}

func jsonUnits(table map[string]any, name string) map[string]any {
	return jsonRole(table, name)["units"].(map[string]any)
}

func withoutRole(table map[string]any, name string) {
	var roles []any
	for _, item := range table["roles"].([]any) {
		if item.(map[string]any)["name"] != name {
			roles = append(roles, item)
		}
	}
	table["roles"] = roles
}

func TestParseTableRejectsAnInvalidTableAsConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(table map[string]any)
	}{
		{"other schema version", func(table map[string]any) { table["schema_version"] = 2 }},
		{"no namespace", func(table map[string]any) { jsonUnitHealth(table)["namespace"] = "" }},
		{"no metric", func(table map[string]any) { delete(jsonUnitHealth(table), "metric") }},
		{"no dimension", func(table map[string]any) { delete(jsonUnitHealth(table), "dimension") }},
		{"other publish gate", func(table map[string]any) { jsonUnitHealth(table)["published_when"] = "always" }},
		{"stale intervals below one", func(table map[string]any) { jsonUnitHealth(table)["oneshot_stale_after_intervals"] = 0 }},
		{"no success exit codes", func(table map[string]any) { jsonUnitHealth(table)["oneshot_success_exit_codes"] = []any{} }},
		{"exit code out of range", func(table map[string]any) { jsonUnitHealth(table)["oneshot_success_exit_codes"] = []any{0, 256} }},
		{"unknown unit health", func(table map[string]any) { jsonRole(table, "email-worker")["unit_health"] = "docker" }},
		{"systemd health on the CLI", func(table map[string]any) {
			role := jsonRole(table, "limnopulse-admin")
			role["unit_health"] = UnitHealthSystemd
			role["units"] = map[string]any{"service": "limnopulse-admin.service"}
		}},
		{"one-shot without a timer", func(table map[string]any) { delete(jsonUnits(table, "alert-evaluator"), "timer") }},
		{"one-shot without a schedule", func(table map[string]any) { delete(jsonRole(table, "alert-evaluator"), "schedule_seconds") }},
		{"continuous without a service", func(table map[string]any) { delete(jsonRole(table, "email-worker"), "units") }},
		{"service without its suffix", func(table map[string]any) { jsonUnits(table, "email-worker")["service"] = "limnopulse-email-worker" }},
		{"timer in the service slot", func(table map[string]any) {
			jsonUnits(table, "alert-evaluator")["service"] = "limnopulse-alert-evaluator.timer"
		}},
		{"unit that reads as an option", func(table map[string]any) { jsonUnits(table, "email-worker")["service"] = "--all.service" }},
		{"invalid role name", func(table map[string]any) { jsonRole(table, "email-worker")["name"] = "Email Worker" }},
		{"duplicate role", func(table map[string]any) {
			roles := table["roles"].([]any)
			table["roles"] = append(roles, roles[0])
		}},
		{"no worker-monitor role", func(table map[string]any) { withoutRole(table, SelfRole) }},
		{"worker-monitor continuous", func(table map[string]any) { jsonRole(table, SelfRole)["lifecycle"] = LifecycleContinuous }},
		{"worker-monitor without unit health", func(table map[string]any) { jsonRole(table, SelfRole)["unit_health"] = UnitHealthNone }},
		{"roles not a list", func(table map[string]any) { table["roles"] = "all" }},
		{"namespace not a string", func(table map[string]any) { jsonUnitHealth(table)["namespace"] = 7 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := readTableJSON(t)
			tc.mutate(table)
			raw, err := json.Marshal(table)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseTable(raw); !workerproc.IsConfiguration(err) {
				t.Fatalf("ParseTable error = %v, want a configuration error", err)
			}
		})
	}
}

func TestLoadTableRejectsAMissingOrUnreadableTableAsConfiguration(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"empty":      "",
		"not json":   "schema_version: 1\n",
		"null":       "null",
		"array":      "[]",
		"truncated":  `{"schema_version": 1, "unit_health": {`,
		"no content": "{}",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadTable(path); !workerproc.IsConfiguration(err) {
				t.Fatalf("LoadTable error = %v, want a configuration error", err)
			}
		})
	}
	t.Run("missing", func(t *testing.T) {
		if _, err := LoadTable(filepath.Join(dir, "roles.json")); !workerproc.IsConfiguration(err) {
			t.Fatalf("LoadTable error = %v, want a configuration error", err)
		}
	})
}
