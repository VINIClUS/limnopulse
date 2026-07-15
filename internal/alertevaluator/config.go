package alertevaluator

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"
)

type LookupEnv func(string) (string, bool)

type RunConfig struct {
	AppEnv                 string
	EvaluationTime         *time.Time
	Shard                  int
	ShardCount             int
	GlobalDeadline         time.Duration
	DrainGrace             time.Duration
	PerRuleTimeout         time.Duration
	LeaseTTL               time.Duration
	EvaluationParallelism  int
	QueryParallelism       int
	MaxRules               int
	PageSize               int
	ExpectedSampleInterval time.Duration
	MinimumCoverageRatio   float64
	MinimumPoints          int
	AllowedLateness        time.Duration
	MaximumSampleAge       time.Duration
	SystemicErrorThreshold int
	OTLPFlushTimeout       time.Duration
	AWSRegion              string
	DynamoDBTable          string
	DynamoDBEndpoint       string
	InfluxDBURL            string
	InfluxDBToken          string
	InfluxDBOrg            string
	InfluxDBBucket         string
	RedisURL               string
	OTLPEndpoint           string
}

func defaultRunConfig() RunConfig {
	return RunConfig{
		AppEnv:                 "local",
		ShardCount:             1,
		GlobalDeadline:         45 * time.Second,
		DrainGrace:             5 * time.Second,
		PerRuleTimeout:         8 * time.Second,
		LeaseTTL:               15 * time.Second,
		EvaluationParallelism:  8,
		QueryParallelism:       4,
		MaxRules:               250,
		PageSize:               25,
		ExpectedSampleInterval: 10 * time.Second,
		MinimumCoverageRatio:   0.8,
		MinimumPoints:          3,
		AllowedLateness:        15 * time.Second,
		MaximumSampleAge:       30 * time.Second,
		SystemicErrorThreshold: 3,
		OTLPFlushTimeout:       2 * time.Second,
		AWSRegion:              "us-east-1",
		DynamoDBTable:          "LimnopulseDomain",
		InfluxDBURL:            "http://localhost:8086",
		InfluxDBToken:          "local-dev-token",
		InfluxDBOrg:            "limnopulse",
		InfluxDBBucket:         "limnopulse_raw",
		RedisURL:               "redis://localhost:6379/0",
	}
}

func LoadRunConfig(args []string, lookup LookupEnv) (RunConfig, error) {
	config := defaultRunConfig()
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	qualityExplicit := map[string]bool{}
	if err := applyEnvironment(&config, lookup, qualityExplicit); err != nil {
		return RunConfig{}, err
	}

	fs := flag.NewFlagSet("alert-evaluator run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	evaluationTime := ""
	fs.StringVar(&evaluationTime, "evaluation-time", "", "logical RFC3339 evaluation time")
	fs.IntVar(&config.Shard, "shard", config.Shard, "logical shard number")
	fs.IntVar(&config.ShardCount, "shard-count", config.ShardCount, "logical shard count")
	fs.DurationVar(&config.GlobalDeadline, "global-deadline", config.GlobalDeadline, "run deadline")
	fs.DurationVar(&config.DrainGrace, "drain-grace", config.DrainGrace, "shutdown grace")
	fs.DurationVar(&config.PerRuleTimeout, "per-rule-timeout", config.PerRuleTimeout, "rule timeout")
	fs.DurationVar(&config.LeaseTTL, "lease-ttl", config.LeaseTTL, "DynamoDB lease TTL")
	fs.IntVar(&config.EvaluationParallelism, "evaluation-parallelism", config.EvaluationParallelism, "parallel evaluations")
	fs.IntVar(&config.QueryParallelism, "query-parallelism", config.QueryParallelism, "parallel GSI queries")
	fs.IntVar(&config.MaxRules, "max-rules", config.MaxRules, "maximum rules per run")
	fs.IntVar(&config.PageSize, "page-size", config.PageSize, "DynamoDB query page size")
	fs.DurationVar(&config.ExpectedSampleInterval, "expected-sample-interval", config.ExpectedSampleInterval, "quality bucket width")
	fs.Float64Var(&config.MinimumCoverageRatio, "min-coverage-ratio", config.MinimumCoverageRatio, "minimum quality coverage")
	fs.IntVar(&config.MinimumPoints, "min-points", config.MinimumPoints, "minimum occupied buckets")
	fs.DurationVar(&config.AllowedLateness, "allowed-lateness", config.AllowedLateness, "telemetry lateness allowance")
	fs.DurationVar(&config.MaximumSampleAge, "max-sample-age", config.MaximumSampleAge, "maximum sample age")
	if err := fs.Parse(args); err != nil {
		return RunConfig{}, err
	}
	fs.Visit(func(item *flag.Flag) {
		if _, ok := qualityFlagToEnv[item.Name]; ok {
			qualityExplicit[item.Name] = true
		}
	})
	if evaluationTime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, evaluationTime)
		if err != nil {
			return RunConfig{}, fmt.Errorf("evaluation-time: %w", err)
		}
		parsed = parsed.UTC()
		config.EvaluationTime = &parsed
	}
	if err := validateRunConfig(config, qualityExplicit); err != nil {
		return RunConfig{}, err
	}
	return config, nil
}

var qualityFlagToEnv = map[string]string{
	"expected-sample-interval": "ALERT_EVALUATOR_EXPECTED_SAMPLE_INTERVAL",
	"min-coverage-ratio":       "ALERT_EVALUATOR_MIN_COVERAGE_RATIO",
	"min-points":               "ALERT_EVALUATOR_MIN_POINTS",
	"allowed-lateness":         "ALERT_EVALUATOR_ALLOWED_LATENESS",
	"max-sample-age":           "ALERT_EVALUATOR_MAX_SAMPLE_AGE",
}

func applyEnvironment(config *RunConfig, lookup LookupEnv, explicit map[string]bool) error {
	stringValues := map[string]*string{
		"APP_ENV":                     &config.AppEnv,
		"AWS_REGION":                  &config.AWSRegion,
		"DYNAMODB_DOMAIN_TABLE":       &config.DynamoDBTable,
		"DYNAMODB_ENDPOINT_URL":       &config.DynamoDBEndpoint,
		"INFLUXDB_URL":                &config.InfluxDBURL,
		"INFLUXDB_TOKEN":              &config.InfluxDBToken,
		"INFLUXDB_ORG":                &config.InfluxDBOrg,
		"INFLUXDB_BUCKET_RAW":         &config.InfluxDBBucket,
		"REDIS_URL":                   &config.RedisURL,
		"OTEL_EXPORTER_OTLP_ENDPOINT": &config.OTLPEndpoint,
	}
	for key, target := range stringValues {
		if value, ok := lookup(key); ok {
			*target = value
		}
	}
	intValues := map[string]*int{
		"ALERT_EVALUATOR_SHARD":                  &config.Shard,
		"ALERT_EVALUATOR_SHARD_COUNT":            &config.ShardCount,
		"ALERT_EVALUATOR_EVALUATION_PARALLELISM": &config.EvaluationParallelism,
		"ALERT_EVALUATOR_QUERY_PARALLELISM":      &config.QueryParallelism,
		"ALERT_EVALUATOR_MAX_RULES":              &config.MaxRules,
		"ALERT_EVALUATOR_PAGE_SIZE":              &config.PageSize,
		"ALERT_EVALUATOR_MIN_POINTS":             &config.MinimumPoints,
	}
	for key, target := range intValues {
		if value, ok := lookup(key); ok {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*target = parsed
		}
	}
	durations := map[string]*time.Duration{
		"ALERT_EVALUATOR_GLOBAL_DEADLINE":          &config.GlobalDeadline,
		"ALERT_EVALUATOR_DRAIN_GRACE":              &config.DrainGrace,
		"ALERT_EVALUATOR_PER_RULE_TIMEOUT":         &config.PerRuleTimeout,
		"ALERT_EVALUATOR_LEASE_TTL":                &config.LeaseTTL,
		"ALERT_EVALUATOR_EXPECTED_SAMPLE_INTERVAL": &config.ExpectedSampleInterval,
		"ALERT_EVALUATOR_ALLOWED_LATENESS":         &config.AllowedLateness,
		"ALERT_EVALUATOR_MAX_SAMPLE_AGE":           &config.MaximumSampleAge,
		"ALERT_EVALUATOR_OTLP_FLUSH_TIMEOUT":       &config.OTLPFlushTimeout,
	}
	for key, target := range durations {
		if value, ok := lookup(key); ok {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*target = parsed
		}
	}
	if value, ok := lookup("ALERT_EVALUATOR_MIN_COVERAGE_RATIO"); ok {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("ALERT_EVALUATOR_MIN_COVERAGE_RATIO: %w", err)
		}
		config.MinimumCoverageRatio = parsed
	}
	for flagName, envName := range qualityFlagToEnv {
		_, explicit[flagName] = lookup(envName)
	}
	return nil
}

func validateRunConfig(config RunConfig, qualityExplicit map[string]bool) error {
	if _, err := ValidateShard(config.Shard, config.ShardCount); err != nil {
		return err
	}
	if config.GlobalDeadline <= 0 || config.GlobalDeadline >= time.Minute {
		return fmt.Errorf("global-deadline must be positive and less than the cadence")
	}
	if config.DrainGrace <= 0 || config.DrainGrace >= config.GlobalDeadline {
		return fmt.Errorf("drain-grace must be positive and less than global-deadline")
	}
	if config.PerRuleTimeout <= 0 || config.LeaseTTL <= config.PerRuleTimeout {
		return fmt.Errorf("lease-ttl must be greater than per-rule-timeout")
	}
	if config.EvaluationParallelism < 1 || config.QueryParallelism < 1 || config.MaxRules < 1 || config.PageSize < 1 {
		return fmt.Errorf("parallelism and work limits must be positive")
	}
	if config.ExpectedSampleInterval <= 0 || config.MinimumCoverageRatio <= 0 || config.MinimumCoverageRatio > 1 || config.MinimumPoints < 1 || config.AllowedLateness < 0 || config.MaximumSampleAge <= 0 {
		return fmt.Errorf("quality settings are invalid")
	}
	if config.AppEnv != "local" && config.AppEnv != "test" && config.AppEnv != "staging" && config.AppEnv != "prod" {
		return fmt.Errorf("APP_ENV must be local, test, staging or prod")
	}
	if config.AppEnv == "staging" || config.AppEnv == "prod" {
		for flagName := range qualityFlagToEnv {
			if !qualityExplicit[flagName] {
				return fmt.Errorf("%s must be explicit in %s", flagName, config.AppEnv)
			}
		}
	}
	return nil
}
