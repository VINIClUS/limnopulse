package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

// fluxLiteralFixture is testdata/flux_literals/d19_cases.json (P2 plan D19, #84).
// tests/unit/test_telemetry_v2_schema.py reads the same file, so Python and Go
// emit identical literals.
type fluxLiteralFixture struct {
	Literals []struct {
		Value   string `json:"value"`
		Literal string `json:"literal"`
	} `json:"literals"`
	RoundTrip []string `json:"round_trip"`
}

func loadFluxLiteralFixture(t *testing.T) fluxLiteralFixture {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "testdata", "flux_literals", "d19_cases.json"))
	if err != nil {
		t.Fatalf("read D19 cases: %v", err)
	}
	var fixture fluxLiteralFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode D19 cases: %v", err)
	}
	if len(fixture.Literals) < 20 || len(fixture.RoundTrip) < 20 {
		t.Fatalf("D19 cases are incomplete: %d literals, %d round-trip values", len(fixture.Literals), len(fixture.RoundTrip))
	}
	return fixture
}

func literalSpec() alertevaluator.WindowSpec {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	return alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)}
}

// between returns the text of query between the first prefix and the next suffix.
func between(query, prefix, suffix string) (string, bool) {
	begin := strings.Index(query, prefix)
	if begin < 0 {
		return "", false
	}
	begin += len(prefix)
	end := strings.Index(query[begin:], suffix)
	if end < 0 {
		return "", false
	}
	return query[begin : begin+end], true
}

// v2Literal returns the literal BuildV2Query writes for value as the bucket,
// the tenant and (when not empty) the device, so these tests drive the real call
// sites, and fails when the three differ.
func v2Literal(value string) (string, error) {
	query, err := BuildV2Query(value, alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: value}, PondID: "pond_1", DeviceID: value, Metric: "ph",
	}, literalSpec())
	if err != nil {
		return "", err
	}
	sites := []struct{ name, prefix, suffix string }{
		{"bucket", "from(bucket: ", ")\n  |> range(start: "},
		{"tenant", "\n  |> filter(fn: (r) => r[\"tenant_id\"] == ", ")\n  |> filter(fn: (r) => r[\"asset_id\"] == "},
		{"device", "\n  |> filter(fn: (r) => r[\"device_id\"] == ", ")\n  |> filter(fn: (r) => r[\"metric_key\"] == "},
	}
	if value == "" {
		sites = sites[:2] // an empty DeviceID is a pond-wide rule without a device filter
	}
	var literal string
	for index, site := range sites {
		found, ok := between(query, site.prefix, site.suffix)
		if !ok {
			return "", fmt.Errorf("query has no %s literal:\n%s", site.name, query)
		}
		if index > 0 && found != literal {
			return "", fmt.Errorf("%s literal %+q differs from the bucket literal %+q", site.name, found, literal)
		}
		literal = found
	}
	return literal, nil
}

var fluxSimpleEscapes = map[byte]byte{'n': '\n', 'r': '\r', 't': '\t', '"': '"', '\\': '\\'}

// decodeFluxLiteral decodes a literal with the Flux string escapes D19 emits,
// and nothing else. It is stricter than Flux: an unescaped `$`, a raw C0 control
// or any `\x` escape other than `\x24` is an error.
func decodeFluxLiteral(literal string) (string, error) {
	if len(literal) < 2 || literal[0] != '"' || literal[len(literal)-1] != '"' {
		return "", fmt.Errorf("not a quoted literal: %q", literal)
	}
	body := literal[1 : len(literal)-1]
	var out strings.Builder
	for index := 0; index < len(body); {
		char := body[index]
		switch {
		case strings.HasPrefix(body[index:], `\x24`):
			out.WriteByte('$')
			index += 4
		case char == '\\':
			if index+1 == len(body) {
				return "", fmt.Errorf("dangling backslash in %q", literal)
			}
			decoded, ok := fluxSimpleEscapes[body[index+1]]
			if !ok {
				return "", fmt.Errorf("escape \\%c is not one D19 emits, in %q", body[index+1], literal)
			}
			out.WriteByte(decoded)
			index += 2
		case char == '"':
			return "", fmt.Errorf("unescaped quote ends %q early", literal)
		case char == '$':
			return "", fmt.Errorf("unescaped $ in %q", literal)
		case char < 0x20:
			return "", fmt.Errorf("raw control character in %q", literal)
		default:
			out.WriteByte(char)
			index++
		}
	}
	return out.String(), nil
}

func TestDecodeFluxLiteralRejectsWhatD19NeverEmits(t *testing.T) {
	for _, literal := range []string{
		"plain", `"`, `"$"`, `"\/"`, `"\b"`, `"\u2028"`, `"a"b"`, "\"\n\"", `"\"`, `"\xZZ"`, `"\x2"`,
		`"\x7b"`, `"\xc3\xa9"`,
	} {
		if decoded, err := decodeFluxLiteral(literal); err == nil {
			t.Errorf("decodeFluxLiteral(%+q) = %+q, want an error", literal, decoded)
		}
	}
}

func assertLiteralRoundTrips(t *testing.T, value string) {
	t.Helper()
	literal, err := v2Literal(value)
	if err != nil {
		t.Fatalf("literal for %+q: %v", value, err)
	}
	if strings.Contains(literal, "$") || strings.Contains(strings.ReplaceAll(literal, `\\`, ""), `\u`) {
		t.Fatalf("literal %+q for %+q keeps a $ or a \\u escape", literal, value)
	}
	decoded, err := decodeFluxLiteral(literal)
	if err != nil || decoded != value {
		t.Fatalf("literal %+q decodes to (%+q, %v), want %+q", literal, decoded, err, value)
	}
}

func TestBuildV2QueryLiteralsFollowTheLosslessRule(t *testing.T) {
	for _, test := range loadFluxLiteralFixture(t).Literals {
		literal, err := v2Literal(test.Value)
		if err != nil || literal != test.Literal {
			t.Errorf("literal for %+q = (%+q, %v), want %+q", test.Value, literal, err, test.Literal)
		}
	}
}

func TestBuildV2QueryLiteralsDecodeToTheirInput(t *testing.T) {
	for _, value := range loadFluxLiteralFixture(t).RoundTrip {
		assertLiteralRoundTrips(t, value)
	}
}

func TestBuildV2QueryLiteralsRoundTripRandomText(t *testing.T) {
	// Multi-character tokens make the escape neighbours (`\` before U+2028,
	// `\u2028` text, `$` before `{`) frequent.
	tokens := []string{
		"$", "{", "}", "${", `\`, `\\`, `"`, "\n", "\r", "\t", " ", "x", "\\u2028", `\x24`, `\b`,
		"\u2028", "\u2029", "é", "<&>", "😀", "\x7f", "\u0085", "\ufffd",
	}
	random := rand.New(rand.NewPCG(84, 84))
	for range 2000 {
		var value strings.Builder
		for range random.IntN(9) {
			value.WriteString(tokens[random.IntN(len(tokens))])
		}
		assertLiteralRoundTrips(t, value.String())
	}
}

func TestBuildV2QueryRejectsC0ControlsAndInvalidUTF8(t *testing.T) {
	var rejected []string
	for code := rune(0); code < 0x20; code++ {
		if code == '\n' || code == '\r' || code == '\t' {
			continue
		}
		rejected = append(rejected, string(code), "a"+string(code)+"b", "$${"+string(code)+"}")
	}
	// Invalid UTF-8, an encoded surrogate included: encoding/json would turn it into U+FFFD.
	rejected = append(rejected, "\xff", "a\xc3", "\xed\xa0\x80")
	spec := literalSpec()
	builders := map[string]func(value string) (string, error){
		"tenant": func(value string) (string, error) {
			return BuildV2Query("raw", alertevaluator.EvaluationRule{
				Rule: alertevaluator.Rule{TenantID: value}, PondID: "pond_1", Metric: "ph",
			}, spec)
		},
		"device": func(value string) (string, error) { return BuildV2Query("raw", vectorRule(value, "ph"), spec) },
		"bucket": func(value string) (string, error) { return BuildV2Query(value, vectorRule("", "ph"), spec) },
	}
	for name, build := range builders {
		// The same builder accepts a clean value, so only the literal can fail.
		if query, err := build("clean_1"); err != nil || query == "" {
			t.Fatalf("%s: clean value gives (%q, %v)", name, query, err)
		}
		for _, value := range rejected {
			query, err := build(value)
			if err == nil || query != "" {
				t.Errorf("%s %+q: BuildV2Query = (%q, %v), want an error", name, value, query, err)
				continue
			}
			if !strings.Contains(err.Error(), "control character") && !strings.Contains(err.Error(), "UTF-8") {
				t.Errorf("%s %+q: error %q names neither the control character nor UTF-8", name, value, err)
			}
		}
	}
}

func TestBuildV2QueryAcceptsTheAllowedControlsAndTheC0Boundary(t *testing.T) {
	for _, value := range []string{
		"\n", "\r", "\t", " ", "\x7f", "\u0085", "\u009f", "\ufffd", "\\u0001", `\b`, `\f`,
	} {
		assertLiteralRoundTrips(t, value)
	}
}

func TestV2WindowReaderRejectsControlCharactersWithoutQuerying(t *testing.T) {
	for _, test := range []struct {
		bucket string
		rule   alertevaluator.EvaluationRule
	}{
		{bucket: "raw", rule: vectorRule("dev\x1b[0m", "ph")},
		{bucket: "raw", rule: alertevaluator.EvaluationRule{Rule: alertevaluator.Rule{TenantID: "tnt\x00"}, PondID: vectorPond, Metric: "ph"}},
		{bucket: "raw\x01", rule: vectorRule("", "ph")},
	} {
		querier := &fakeV2Querier{}
		result, err := V2WindowReader{Bucket: test.bucket, Querier: querier}.Evaluate(context.Background(), test.rule, literalSpec())
		if err == nil || querier.calls != 0 || result != (alertevaluator.WindowResult{}) {
			t.Fatalf("Evaluate(%+q, %+q/%+q) = (%#v, %v) after %d queries; want an error before any query",
				test.bucket, test.rule.TenantID, test.rule.DeviceID, result, err, querier.calls)
		}
	}
}

func TestLegacyBuildQueryKeepsJSONMarshalLiterals(t *testing.T) {
	// D19 leaves the legacy builder byte-identical: json.Marshal, HTML escapes included.
	query, err := BuildQuery("raw$archive", alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: "é<&>\u2028$${a}"}, PondID: "p$", DeviceID: "d\x01", Metric: "ph",
	}, literalSpec())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`from(bucket: "raw$archive")`,
		"r[\"tenant_id\"] == \"é\\u003c\\u0026\\u003e\\u2028$${a}\")",
		`r["pond_id"] == "p$")`,
		"r[\"device_id\"] == \"d\\u0001\")",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("legacy query misses %q (as on main):\n%s", want, query)
		}
	}
}
