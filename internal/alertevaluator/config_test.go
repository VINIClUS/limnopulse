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
	config, err := LoadRunConfig(nil, env(map[string]string{
		"APP_ENV": "staging",
		"ALERT_EVALUATOR_EXPECTED_SAMPLE_INTERVAL": "20s",
		"ALERT_EVALUATOR_MIN_COVERAGE_RATIO":       "0.9",
		"ALERT_EVALUATOR_MIN_POINTS":               "4",
		"ALERT_EVALUATOR_ALLOWED_LATENESS":         "30s",
		"ALERT_EVALUATOR_MAX_SAMPLE_AGE":           "1m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.ExpectedSampleInterval != 20*time.Second || config.MinimumCoverageRatio != 0.9 {
		t.Fatalf("staging quality = %#v", config)
	}
}
