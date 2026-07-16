package notifications

import (
	"io/fs"
	"math"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestTemplateRendererLoadsImmutableEmbeddedTemplates(t *testing.T) {
	renderer, err := NewTemplateRenderer()
	if err != nil {
		t.Fatalf("NewTemplateRenderer() error = %v", err)
	}

	for _, templateID := range []TemplateID{TemplateAlertOpeningV1, TemplateAlertRecoveryV1} {
		content, err := renderer.Render(templateID, LocalePTBR, validEmailTemplateData())
		if err != nil {
			t.Fatalf("Render(%q) error = %v", templateID, err)
		}
		if content.TemplateID() != templateID || content.TemplateVersion() != 1 || content.Locale() != LocalePTBR {
			t.Fatalf("rendered provenance = id=%q version=%d locale=%q", content.TemplateID(), content.TemplateVersion(), content.Locale())
		}
		if len(content.ContentHash()) != 64 {
			t.Fatalf("content hash = %q", content.ContentHash())
		}
	}
}

func TestTemplateRendererRejectsUnsupportedLocaleAndTemplate(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	if _, err := renderer.Render(TemplateAlertOpeningV1, "en-US", validEmailTemplateData()); err == nil {
		t.Fatal("unsupported locale rendered")
	}
	if _, err := renderer.Render("other/v1", LocalePTBR, validEmailTemplateData()); err == nil {
		t.Fatal("unknown template rendered")
	}
}

func TestTemplateRendererFormatsPtBRDataAndUTCTimesDeterministically(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	data := validEmailTemplateData()
	observed := 1234.567
	data.Threshold = 1234.5
	data.ObservedValue = &observed
	data.WindowStart = time.Date(2026, 7, 16, 9, 4, 5, 6, time.FixedZone("BRT", -3*60*60))
	data.WindowEnd = data.WindowStart.Add(5 * time.Minute)
	data.EvaluatedAt = data.WindowEnd.Add(15 * time.Second)

	first, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	second, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data)
	if err != nil {
		t.Fatalf("second Render() error = %v", err)
	}

	if first != second {
		t.Fatal("same typed data produced different rendered content")
	}
	if first.Subject() != "[critical] Alerta aberto: Oxigênio baixo" {
		t.Fatalf("subject = %q", first.Subject())
	}
	for _, want := range []string{
		"Regra: Oxigênio baixo",
		"Severidade: critical",
		"Tenant: tnt_1",
		"Viveiro: pond_1",
		"Dispositivo: dev_1",
		"Métrica: dissolved_oxygen (mg/L)",
		"Condição: < 1.234,5 mg/L",
		"Valor observado: 1.234,567 mg/L",
		"Janela de avaliação: 5 min",
		"Início da janela: 2026-07-16T12:04:05.000000006Z",
		"Fim da janela: 2026-07-16T12:09:05.000000006Z",
		"Avaliado em: 2026-07-16T12:09:20.000000006Z",
		"ID do evento: alert_1",
	} {
		if !strings.Contains(first.Text(), want) {
			t.Errorf("plain text missing %q:\n%s", want, first.Text())
		}
	}
}

func TestTemplateRendererEscapesHTMLFields(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	data := validEmailTemplateData()
	data.RuleName = `<script>alert("x")</script>`
	data.Unit = `mg/L & "unsafe"`

	content, err := renderer.Render(TemplateAlertRecoveryV1, LocalePTBR, data)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if strings.Contains(content.HTML(), "<script>") || strings.Contains(content.HTML(), `"unsafe"`) {
		t.Fatalf("HTML contains unescaped data: %s", content.HTML())
	}
	if !strings.Contains(content.HTML(), "&lt;script&gt;") || !strings.Contains(content.HTML(), "&amp;") {
		t.Fatalf("HTML does not contain escaped data: %s", content.HTML())
	}
}

func TestTemplateRendererOmitsAbsentObservedValue(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	data := validEmailTemplateData()
	data.ObservedValue = nil

	content, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	if strings.Contains(content.Text(), "Valor observado") || strings.Contains(content.HTML(), "Valor observado") {
		t.Fatalf("absent observed value was rendered: %s", content.Text())
	}
}

func TestTemplateRendererEnforcesOutputLimits(t *testing.T) {
	renderer := mustTemplateRenderer(t)

	t.Run("subject CRLF", func(t *testing.T) {
		data := validEmailTemplateData()
		data.RuleName = "regra\r\nBcc: victim@example.com"
		if _, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
			t.Fatal("CRLF subject rendered")
		}
	})

	t.Run("subject runes", func(t *testing.T) {
		data := validEmailTemplateData()
		data.RuleName = strings.Repeat("á", MaxEmailSubjectRunes)
		if _, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
			t.Fatal("oversized subject rendered")
		}
	})

	t.Run("plain text bytes", func(t *testing.T) {
		data := validEmailTemplateData()
		data.TenantID = strings.Repeat("t", MaxEmailTextBytes)
		if _, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
			t.Fatal("oversized plain text rendered")
		}
	})

	t.Run("HTML bytes", func(t *testing.T) {
		custom, err := newTemplateRenderer(repeatedHTMLTemplateFS())
		if err != nil {
			t.Fatalf("newTemplateRenderer() error = %v", err)
		}
		data := validEmailTemplateData()
		data.TenantID = strings.Repeat("t", 60*1024)
		if _, err := custom.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
			t.Fatal("oversized HTML rendered")
		}
	})
}

func TestTemplateRendererRejectsInvalidNumbers(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		data := validEmailTemplateData()
		data.Threshold = value
		if _, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
			t.Fatalf("invalid number %v rendered", value)
		}
	}
}

func TestTemplateRendererRejectsMissingOrNULRequiredData(t *testing.T) {
	renderer := mustTemplateRenderer(t)
	tests := []struct {
		name   string
		mutate func(*EmailTemplateData)
	}{
		{name: "rule name", mutate: func(data *EmailTemplateData) { data.RuleName = "" }},
		{name: "severity", mutate: func(data *EmailTemplateData) { data.Severity = "" }},
		{name: "tenant ID", mutate: func(data *EmailTemplateData) { data.TenantID = "tnt\x001" }},
		{name: "pond ID", mutate: func(data *EmailTemplateData) { data.PondID = "" }},
		{name: "device ID", mutate: func(data *EmailTemplateData) { data.DeviceID = "" }},
		{name: "metric", mutate: func(data *EmailTemplateData) { data.Metric = "" }},
		{name: "unit", mutate: func(data *EmailTemplateData) { data.Unit = "" }},
		{name: "operator", mutate: func(data *EmailTemplateData) { data.Operator = "" }},
		{name: "event ID", mutate: func(data *EmailTemplateData) { data.EventID = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := validEmailTemplateData()
			tt.mutate(&data)
			if _, err := renderer.Render(TemplateAlertOpeningV1, LocalePTBR, data); err == nil {
				t.Fatal("invalid template data rendered")
			}
		})
	}
}

func TestTemplateRendererValidatesBundleAtStartup(t *testing.T) {
	if _, err := newTemplateRenderer(fstest.MapFS{}); err == nil {
		t.Fatal("missing template bundle accepted")
	}

	invalid := validTemplateFS("{{.FieldThatDoesNotExist}}", "text", "<p>html</p>")
	if _, err := newTemplateRenderer(invalid); err == nil {
		t.Fatal("invalid typed template accepted")
	}
}

func TestTemplateRendererStartupRejectsInvalidRenderedOutput(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		text    string
		html    string
	}{
		{
			name:    "subject CRLF",
			subject: "static subject\r\nBcc: victim@example.com",
			text:    "text",
			html:    "<p>html</p>",
		},
		{
			name:    "subject runes",
			subject: strings.Repeat("á", MaxEmailSubjectRunes+1),
			text:    "text",
			html:    "<p>html</p>",
		},
		{
			name:    "plain text bytes",
			subject: "subject",
			text:    strings.Repeat("t", MaxEmailTextBytes+1),
			html:    "<p>html</p>",
		},
		{
			name:    "HTML bytes",
			subject: "subject",
			text:    "text",
			html:    strings.Repeat("h", MaxEmailHTMLBytes+1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newTemplateRenderer(validTemplateFS(tt.subject, tt.text, tt.html)); err == nil {
				t.Fatal("invalid rendered startup output accepted")
			}
		})
	}
}

func validEmailTemplateData() EmailTemplateData {
	observed := 4.75
	return EmailTemplateData{
		RuleName:         "Oxigênio baixo",
		Severity:         "critical",
		TenantID:         "tnt_1",
		PondID:           "pond_1",
		DeviceID:         "dev_1",
		Metric:           "dissolved_oxygen",
		Unit:             "mg/L",
		Operator:         "<",
		Threshold:        5.5,
		ObservedValue:    &observed,
		EvaluationWindow: 5 * time.Minute,
		WindowStart:      time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC),
		WindowEnd:        time.Date(2026, 7, 16, 12, 5, 0, 0, time.UTC),
		EvaluatedAt:      time.Date(2026, 7, 16, 12, 5, 15, 0, time.UTC),
		EventID:          "alert_1",
	}
}

func mustTemplateRenderer(t *testing.T) *TemplateRenderer {
	t.Helper()
	renderer, err := NewTemplateRenderer()
	if err != nil {
		t.Fatalf("NewTemplateRenderer() error = %v", err)
	}
	return renderer
}

func validTemplateFS(subject, text, html string) fs.FS {
	files := fstest.MapFS{}
	for _, name := range []string{"alert-opening", "alert-recovery"} {
		root := "templates/email/pt-BR/" + name + "/v1/"
		files[root+"subject.tmpl"] = &fstest.MapFile{Data: []byte(subject)}
		files[root+"text.tmpl"] = &fstest.MapFile{Data: []byte(text)}
		files[root+"html.tmpl"] = &fstest.MapFile{Data: []byte(html)}
	}
	return files
}

func repeatedHTMLTemplateFS() fs.FS {
	html := strings.Repeat("{{.TenantID}}", 5)
	return validTemplateFS("short", "{{.TenantID}}", html)
}
