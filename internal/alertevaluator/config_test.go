package alertevaluator

import (
	"testing"
	"time"
)

func env(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadRunConfigUsesBalancedLocalDefaults(t *testing.T) {
	config, err := LoadRunConfig(nil, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if config.GlobalDeadline != 45*time.Second || config.DrainGrace != 5*time.Second || config.PerRuleTimeout != 8*time.Second || config.LeaseTTL != 15*time.Second {
		t.Fatalf("timeouts = %#v", config)
	}
	if config.EvaluationParallelism != 8 || config.QueryParallelism != 4 || config.MaxRules != 250 {
		t.Fatalf("limits = %#v", config)
	}
	if config.ExpectedSampleInterval != 10*time.Second || config.MinimumCoverageRatio != 0.8 || config.MinimumPoints != 3 || config.AllowedLateness != 15*time.Second || config.MaximumSampleAge != 30*time.Second {
		t.Fatalf("quality defaults = %#v", config)
	}
	if config.TelegramDeliveryEnabled {
		t.Fatal("Telegram delivery writer must default off")
	}
}

func TestLoadRunConfigParsesTelegramWriterFeatureFlag(t *testing.T) {
	config, err := LoadRunConfig(nil, env(map[string]string{
		"TELEGRAM_DELIVERY_ENABLED": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !config.TelegramDeliveryEnabled {
		t.Fatal("Telegram delivery writer was not enabled")
	}
}

func TestLoadRunConfigAppliesCLIOverEnvironment(t *testing.T) {
	config, err := LoadRunConfig(
		[]string{"--shard=2", "--shard-count=4", "--max-rules=20", "--evaluation-time=2026-07-15T12:00:00Z"},
		env(map[string]string{
			"ALERT_EVALUATOR_SHARD":       "1",
			"ALERT_EVALUATOR_SHARD_COUNT": "3",
			"ALERT_EVALUATOR_MAX_RULES":   "10",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if config.Shard != 2 || config.ShardCount != 4 || config.MaxRules != 20 {
		t.Fatalf("precedence = %#v", config)
	}
	want := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if config.EvaluationTime == nil || !config.EvaluationTime.Equal(want) {
		t.Fatalf("evaluation time = %v", config.EvaluationTime)
	}
}

func TestLoadRunConfigRejectsInvalidShardAndTimeoutRelationships(t *testing.T) {
	for _, args := range [][]string{
		{"--shard=1", "--shard-count=1"},
		{"--per-rule-timeout=20s", "--lease-ttl=15s"},
		{"--drain-grace=45s", "--global-deadline=45s"},
	} {
		if _, err := LoadRunConfig(args, env(nil)); err == nil {
			t.Fatalf("LoadRunConfig(%v) succeeded", args)
		}
	}
}

func TestStagingRequiresExplicitQualitySettings(t *testing.T) {
	if _, err := LoadRunConfig(nil, env(map[string]string{"APP_ENV": "staging"})); err == nil {
		t.Fatal("staging config succeeded without explicit quality settings")
	}
	hosted := map[string]string{
		"APP_ENV": "staging",
		"ALERT_EVALUATOR_EXPECTED_SAMPLE_INTERVAL": "20s",
		"ALERT_EVALUATOR_MIN_COVERAGE_RATIO":       "0.9",
		"ALERT_EVALUATOR_MIN_POINTS":               "4",
		"ALERT_EVALUATOR_ALLOWED_LATENESS":         "30s",
		"ALERT_EVALUATOR_MAX_SAMPLE_AGE":           "1m",
	}
	if _, err := LoadRunConfig(nil, env(hosted)); err == nil {
		t.Fatal("staging config succeeded without explicit Telegram writer flag")
	}
	hosted["TELEGRAM_DELIVERY_ENABLED"] = "false"
	config, err := LoadRunConfig(nil, env(hosted))
	if err != nil {
		t.Fatal(err)
	}
	if config.ExpectedSampleInterval != 20*time.Second || config.MinimumCoverageRatio != 0.9 {
		t.Fatalf("staging quality = %#v", config)
	}
}

func TestLoadRunConfigDefaultsTelemetrySourceToLegacy(t *testing.T) {
	config, err := LoadRunConfig(nil, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if config.TelemetrySource != "legacy" {
		t.Fatalf("telemetry source = %q, want legacy", config.TelemetrySource)
	}
}

func TestLoadRunConfigReadsTelemetrySourceFromEnvironmentAndFlag(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{name: "env v2", env: map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": "v2"}, want: "v2"},
		{name: "env legacy", env: map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": "legacy"}, want: "legacy"},
		{name: "flag v2", args: []string{"--telemetry-source=v2"}, want: "v2"},
		{name: "flag over env", args: []string{"--telemetry-source", "v2"}, env: map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": "legacy"}, want: "v2"},
		{name: "flag legacy over env v2", args: []string{"--telemetry-source=legacy"}, env: map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": "v2"}, want: "legacy"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := LoadRunConfig(test.args, env(test.env))
			if err != nil {
				t.Fatal(err)
			}
			if config.TelemetrySource != test.want {
				t.Fatalf("telemetry source = %q, want %q", config.TelemetrySource, test.want)
			}
		})
	}
}

func TestLoadRunConfigRejectsInvalidTelemetrySource(t *testing.T) {
	for _, value := range []string{"", "V2", "Legacy", "both", "v1", " v2"} {
		if _, err := LoadRunConfig(nil, env(map[string]string{"ALERT_EVALUATOR_TELEMETRY_SOURCE": value})); err == nil {
			t.Errorf("env ALERT_EVALUATOR_TELEMETRY_SOURCE=%q accepted", value)
		}
		if _, err := LoadRunConfig([]string{"--telemetry-source=" + value}, env(nil)); err == nil {
			t.Errorf("flag --telemetry-source=%q accepted", value)
		}
	}
}

func TestStagingValidationIsUnchangedByTelemetrySource(t *testing.T) {
	hosted := map[string]string{
		"APP_ENV": "prod",
		"ALERT_EVALUATOR_EXPECTED_SAMPLE_INTERVAL": "20s",
		"ALERT_EVALUATOR_MIN_COVERAGE_RATIO":       "0.9",
		"ALERT_EVALUATOR_MIN_POINTS":               "4",
		"ALERT_EVALUATOR_ALLOWED_LATENESS":         "30s",
		"ALERT_EVALUATOR_MAX_SAMPLE_AGE":           "1m",
		"TELEGRAM_DELIVERY_ENABLED":                "false",
	}
	config, err := LoadRunConfig(nil, env(hosted))
	if err != nil {
		t.Fatalf("prod config without an explicit telemetry source: %v", err)
	}
	if config.TelemetrySource != "legacy" {
		t.Fatalf("prod telemetry source = %q, want the legacy default", config.TelemetrySource)
	}
	hosted["ALERT_EVALUATOR_TELEMETRY_SOURCE"] = "v2"
	if config, err = LoadRunConfig(nil, env(hosted)); err != nil || config.TelemetrySource != "v2" {
		t.Fatalf("prod v2 config = %q, %v", config.TelemetrySource, err)
	}
	delete(hosted, "TELEGRAM_DELIVERY_ENABLED")
	if _, err := LoadRunConfig([]string{"--telemetry-source=v2"}, env(hosted)); err == nil {
		t.Fatal("telemetry source relaxed the explicit Telegram flag rule")
	}
}
