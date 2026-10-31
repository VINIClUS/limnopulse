package influx

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

// fluxLiteralCases is the P2 plan D19 table (#84). tests/unit/test_telemetry_v2_schema.py
// holds the same table, so Python and Go emit identical literals.
var fluxLiteralCases = []struct{ value, want string }{
	{"plain", `"plain"`},
	{"", `""`},
	{`say "hi"`, `"say \"hi\""`},
	{`back\slash`, `"back\\slash"`},
	{"line\nbreak\r\tx", `"line\nbreak\r\tx"`},
	{"${x}", `"\x24{x}"`},
	{"a${b}c${d}", `"a\x24{b}c\x24{d}"`},
	{"raw$archive", `"raw\x24archive"`},
	{"$", `"\x24"`},
	{"{x}", `"{x}"`},
	{"$${a}", `"\x24\x24{a}"`},
	{`\${z}`, `"\\\x24{z}"`},
	{`$"`, `"\x24\""`},
	{`\x24`, `"\\x24"`},
	{`\u2028`, `"\\u2028"`},
	{"é", `"é"`},
	{"<&>", `"<&>"`},
	{"\u2028\u2029", "\"\u2028\u2029\""},
	{"😀", `"😀"`},
	{"a\x7fb\u0085", "\"a\x7fb\u0085\""},
}

// fluxRoundTripValues is the #84 list, the characters encoding/json escapes even without
// HTML escaping, and neighbours of the escapes the rule emits. The Python live test
// (tests/influx/test_flux_literal_round_trip_influx.py) uses the same values.
var fluxRoundTripValues = []string{
	"$${a}", "${a}", "$", "t$\n", `\${z}`, "$$$${{a}}", `a$\${b}`, "é", "<&>", `a"b\c`,
	"\u2028", "\u2029", `$"`, "raw$archive", "\r\t", "a\x7fb\u0085", "😀", `\u2028`, `\x24`,
	`\`, "}{$", "",
}

func literalSpec() alertevaluator.WindowSpec {
	start := time.Date(2026, 7, 9, 11, 55, 0, 0, time.UTC)
	return alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)}
}

// v2TenantLiteral returns the literal BuildV2Query writes for tenantID, so these tests
// drive the real call site rather than a helper signature.
func v2TenantLiteral(tenantID string) (string, error) {
	query, err := BuildV2Query("raw", alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: tenantID}, PondID: "pond_1", Metric: "ph",
	}, literalSpec())
	if err != nil {
		return "", err
	}
	const prefix = "\n  |> filter(fn: (r) => r[\"tenant_id\"] == "
	const suffix = ")\n  |> filter(fn: (r) => r[\"asset_id\"] == "
	begin := strings.Index(query, prefix)
	if begin < 0 {
		return "", fmt.Errorf("query has no tenant filter:\n%s", query)
	}
	begin += len(prefix)
	end := strings.Index(query[begin:], suffix)
	if end < 0 {
		return "", fmt.Errorf("query has no asset filter after the tenant:\n%s", query)
	}
	return query[begin : begin+end], nil
}

// decodeFluxLiteral decodes a literal with the Flux string escapes. It is stricter than
// Flux: an unescaped `$`, a raw C0 control or any escape D19 never emits is an error.
func decodeFluxLiteral(literal string) (string, error) {
	if len(literal) < 2 || literal[0] != '"' || literal[len(literal)-1] != '"' {
		return "", fmt.Errorf("not a quoted literal: %q", literal)
	}
	body := literal[1 : len(literal)-1]
	var out []byte
	for index := 0; index < len(body); {
		char := body[index]
		switch {
		case char == '\\':
			if index+1 == len(body) {
				return "", fmt.Errorf("dangling backslash in %q", literal)
			}
			escape := body[index+1]
			switch escape {
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case '"', '\\':
				out = append(out, escape)
			case 'x':
				if index+4 > len(body) {
					return "", fmt.Errorf("short \\x escape in %q", literal)
				}
				value, err := strconv.ParseUint(body[index+2:index+4], 16, 8)
				if err != nil {
					return "", fmt.Errorf("bad \\x escape in %q: %w", literal, err)
				}
				out = append(out, byte(value))
				index += 4
				continue
			default:
				return "", fmt.Errorf("invalid Flux escape \\%c in %q", escape, literal)
			}
			index += 2
		case char == '"':
			return "", fmt.Errorf("unescaped quote ends %q early", literal)
		case char == '$':
			return "", fmt.Errorf("unescaped $ in %q", literal)
		case char < 0x20:
			return "", fmt.Errorf("raw control character in %q", literal)
		default:
			out = append(out, char)
			index++
		}
	}
	if !utf8.Valid(out) {
		return "", fmt.Errorf("%q decodes to invalid UTF-8", literal)
	}
	return string(out), nil
}

func assertLiteralRoundTrips(t *testing.T, value string) {
	t.Helper()
	literal, err := v2TenantLiteral(value)
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
	for _, test := range fluxLiteralCases {
		literal, err := v2TenantLiteral(test.value)
		if err != nil || literal != test.want {
			t.Errorf("literal for %+q = (%+q, %v), want %+q", test.value, literal, err, test.want)
		}
	}
}

func TestBuildV2QueryLiteralsDecodeToTheirInput(t *testing.T) {
	for _, value := range fluxRoundTripValues {
		assertLiteralRoundTrips(t, value)
	}
	for _, test := range fluxLiteralCases {
		assertLiteralRoundTrips(t, test.value)
	}
}

func TestBuildV2QueryLiteralsRoundTripRandomText(t *testing.T) {
	alphabet := []rune("${}\\\"\n\r\t xu2028é<&>\u2028\u2029😀\x7f\u0085")
	random := rand.New(rand.NewPCG(84, 84))
	for range 2000 {
		runes := make([]rune, random.IntN(13))
		for index := range runes {
			runes[index] = alphabet[random.IntN(len(alphabet))]
		}
		assertLiteralRoundTrips(t, string(runes))
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
	for _, value := range rejected {
		for name, build := range map[string]func() (string, error){
			"tenant": func() (string, error) {
				return BuildV2Query("raw", alertevaluator.EvaluationRule{
					Rule: alertevaluator.Rule{TenantID: value}, PondID: "pond_1", Metric: "ph",
				}, spec)
			},
			"device": func() (string, error) { return BuildV2Query("raw", vectorRule(value, "ph"), spec) },
			"bucket": func() (string, error) { return BuildV2Query(value, vectorRule("", "ph"), spec) },
		} {
			if query, err := build(); err == nil || query != "" {
				t.Errorf("%s %+q: BuildV2Query = (%q, %v), want an error", name, value, query, err)
			}
		}
	}
}

func TestBuildV2QueryAcceptsTheAllowedControlsAndTheC0Boundary(t *testing.T) {
	for _, value := range []string{"\n", "\r", "\t", " ", "\x7f", "\u0085", "\u009f"} {
		assertLiteralRoundTrips(t, value)
	}
}

func TestV2WindowReaderRejectsControlCharactersWithoutQuerying(t *testing.T) {
	for _, rule := range []alertevaluator.EvaluationRule{
		vectorRule("dev\x1b[0m", "ph"),
		{Rule: alertevaluator.Rule{TenantID: "tnt\x00"}, PondID: vectorPond, Metric: "ph"},
	} {
		querier := &fakeV2Querier{}
		result, err := V2WindowReader{Bucket: "raw", Querier: querier}.Evaluate(context.Background(), rule, literalSpec())
		if err == nil || querier.calls != 0 || result != (alertevaluator.WindowResult{}) {
			t.Fatalf("Evaluate(%+q) = (%#v, %v) after %d queries; want an error before any query",
				rule.TenantID+"/"+rule.DeviceID, result, err, querier.calls)
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
		`r["tenant_id"] == "é\u003c\u0026\u003e\u2028$${a}")`,
		`r["pond_id"] == "p$")`,
		`r["device_id"] == "d\u0001")`,
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("legacy query misses %q (as on main):\n%s", want, query)
		}
	}
}
