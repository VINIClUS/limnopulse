package notifications

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestTelegramTemplateIsPlainPTBRAndPreservesRequiredFacts(t *testing.T) {
	value := 31.25
	content, err := RenderTelegramAlert(
		NotificationKindOpening,
		EmailTemplateData{
			RuleName: "Oxigênio baixo", Severity: "critical", TenantID: "tnt 1",
			PondID: "viveiro/sul", Metric: "temperature", Unit: "°C",
			Operator: ">", Threshold: 30, ObservedValue: &value,
			EvaluationWindow: 5 * time.Minute,
			WindowStart:      time.Date(2026, 8, 13, 11, 55, 0, 0, time.UTC),
			WindowEnd:        time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
			EvaluatedAt:      time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
			EventID:          "alert_1",
		},
		"https://app.example.com/base",
	)
	if err != nil {
		t.Fatalf("RenderTelegramAlert() error = %v", err)
	}
	body := content.BodyText()
	for _, want := range []string{
		"🚨 Alerta crítico", "Tanque: viveiro/sul", "Regra: Oxigênio baixo",
		"Valor: 31.25 °C (> 30 °C)", "Evento: alert_1",
		"https://app.example.com/base/tenants/tnt%201/alert-events/alert_1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body does not contain %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"<b>", "parse_mode", "[Oxigênio"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("plain text body contains %q: %s", forbidden, body)
		}
	}
}

func TestTelegramTemplateTruncatesByRuneAndKeepsEventAndFinalURL(t *testing.T) {
	content, err := RenderTelegramAlert(
		NotificationKindRecovery,
		EmailTemplateData{
			RuleName: strings.Repeat("á", 5_000), Severity: "warning", TenantID: "tnt_1",
			PondID: "pond_1", Metric: "ph", Unit: "pH", Operator: "<", Threshold: 6,
			EvaluationWindow: 5 * time.Minute,
			WindowStart:      time.Date(2026, 8, 13, 11, 55, 0, 0, time.UTC),
			WindowEnd:        time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
			EvaluatedAt:      time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), EventID: "alert_1",
		},
		"https://app.example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	body := content.BodyText()
	if !utf8.ValidString(body) || utf8.RuneCountInString(body) > MaxTelegramBodyRunes {
		t.Fatalf("invalid truncated body: runes=%d", utf8.RuneCountInString(body))
	}
	if !strings.Contains(body, "Evento: alert_1") ||
		!strings.HasSuffix(body, "https://app.example.com/tenants/tnt_1/alert-events/alert_1") {
		t.Fatalf("truncation lost immutable tail:\n%s", body)
	}
}

func TestTelegramAlertURLRejectsUnsafeHostedBase(t *testing.T) {
	for _, base := range []string{
		"http://app.example.com", "https://user:pass@app.example.com", "https://app.example.com/#fragment",
	} {
		if _, err := BuildAlertEventURL(base, "tnt_1", "alert_1", false); err == nil {
			t.Fatalf("unsafe hosted base %q accepted", base)
		}
	}
	got, err := BuildAlertEventURL("http://localhost:3000", "tnt_1", "alert_1", true)
	if err != nil || got != "http://localhost:3000/tenants/tnt_1/alert-events/alert_1" {
		t.Fatalf("local URL = %q, %v", got, err)
	}
}

func TestTelegramTemplateRejectsIncompleteDomainData(t *testing.T) {
	_, err := RenderTelegramAlert(
		NotificationKindOpening,
		EmailTemplateData{
			Severity: "critical", TenantID: "tnt_1", PondID: "pond_1", Metric: "ph", Unit: "pH",
			Operator: "<", Threshold: 6, EvaluationWindow: time.Minute,
			WindowStart: time.Date(2026, 8, 13, 11, 59, 0, 0, time.UTC),
			WindowEnd:   time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
			EvaluatedAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), EventID: "alert_1",
		},
		"https://app.example.com",
	)
	if err == nil {
		t.Fatal("Telegram renderer accepted an empty rule name")
	}
}
