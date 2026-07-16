package notifications

import (
	"bytes"
	"embed"
	"fmt"
	htmltemplate "html/template"
	"io/fs"
	"math"
	"strconv"
	"strings"
	texttemplate "text/template"
	"time"
	"unicode/utf8"
)

const (
	TemplateAlertOpeningV1  TemplateID = "alert-opening/v1"
	TemplateAlertRecoveryV1 TemplateID = "alert-recovery/v1"
	LocalePTBR              Locale     = "pt-BR"

	MaxEmailSubjectRunes = 180
	MaxEmailTextBytes    = 64 * 1024
	MaxEmailHTMLBytes    = 256 * 1024
)

//go:embed templates/email/pt-BR/alert-opening/v1/*.tmpl templates/email/pt-BR/alert-recovery/v1/*.tmpl
var embeddedEmailTemplates embed.FS

type TemplateID string

type Locale string

func (templateID TemplateID) Version() (int, error) {
	switch templateID {
	case TemplateAlertOpeningV1, TemplateAlertRecoveryV1:
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown template ID %q", templateID)
	}
}

func (locale Locale) Validate() error {
	if locale != LocalePTBR {
		return fmt.Errorf("unsupported locale %q", locale)
	}
	return nil
}

type EmailTemplateData struct {
	RuleName         string
	Severity         string
	TenantID         string
	PondID           string
	DeviceID         string
	Metric           string
	Unit             string
	Operator         string
	Threshold        float64
	ObservedValue    *float64
	EvaluationWindow time.Duration
	WindowStart      time.Time
	WindowEnd        time.Time
	EvaluatedAt      time.Time
	EventID          string
}

type TemplateRenderer struct {
	templates map[TemplateID]compiledEmailTemplate
}

type compiledEmailTemplate struct {
	version int
	subject *texttemplate.Template
	text    *texttemplate.Template
	html    *htmltemplate.Template
}

type templateDefinition struct {
	id      TemplateID
	version int
	path    string
}

var templateDefinitions = []templateDefinition{
	{id: TemplateAlertOpeningV1, version: 1, path: "alert-opening/v1"},
	{id: TemplateAlertRecoveryV1, version: 1, path: "alert-recovery/v1"},
}

func NewTemplateRenderer() (*TemplateRenderer, error) {
	return newTemplateRenderer(embeddedEmailTemplates)
}

func newTemplateRenderer(templateFS fs.FS) (*TemplateRenderer, error) {
	renderer := &TemplateRenderer{
		templates: make(map[TemplateID]compiledEmailTemplate, len(templateDefinitions)),
	}
	for _, definition := range templateDefinitions {
		compiled, err := parseEmailTemplate(templateFS, definition)
		if err != nil {
			return nil, fmt.Errorf("parse template %q: %w", definition.id, err)
		}
		if err := validateCompiledTemplate(compiled); err != nil {
			return nil, fmt.Errorf("validate template %q: %w", definition.id, err)
		}
		renderer.templates[definition.id] = compiled
	}
	return renderer, nil
}

func (renderer *TemplateRenderer) Render(
	templateID TemplateID,
	locale Locale,
	data EmailTemplateData,
) (RenderedContent, error) {
	if err := locale.Validate(); err != nil {
		return RenderedContent{}, err
	}
	compiled, exists := renderer.templates[templateID]
	if !exists {
		return RenderedContent{}, fmt.Errorf("unknown template ID %q", templateID)
	}
	if err := data.Validate(); err != nil {
		return RenderedContent{}, err
	}

	subject, err := executeTextTemplate(compiled.subject, data)
	if err != nil {
		return RenderedContent{}, fmt.Errorf("render subject: %w", err)
	}
	text, err := executeTextTemplate(compiled.text, data)
	if err != nil {
		return RenderedContent{}, fmt.Errorf("render plain text: %w", err)
	}
	html, err := executeHTMLTemplate(compiled.html, data)
	if err != nil {
		return RenderedContent{}, fmt.Errorf("render HTML: %w", err)
	}
	if err := validateRenderedEmail(subject, text, html); err != nil {
		return RenderedContent{}, err
	}
	return newRenderedContent(templateID, compiled.version, locale, subject, text, html), nil
}

func (data EmailTemplateData) Validate() error {
	for name, value := range map[string]string{
		"rule name": data.RuleName,
		"severity":  data.Severity,
		"tenant ID": data.TenantID,
		"pond ID":   data.PondID,
		"device ID": data.DeviceID,
		"metric":    data.Metric,
		"unit":      data.Unit,
		"operator":  data.Operator,
		"event ID":  data.EventID,
	} {
		if err := validateIdentityField(name, value); err != nil {
			return err
		}
	}
	if data.EvaluationWindow <= 0 {
		return fmt.Errorf("evaluation window must be positive")
	}
	if data.WindowStart.IsZero() || data.WindowEnd.IsZero() || data.EvaluatedAt.IsZero() {
		return fmt.Errorf("evaluation times must not be zero")
	}
	return nil
}

func parseEmailTemplate(templateFS fs.FS, definition templateDefinition) (compiledEmailTemplate, error) {
	root := "templates/email/pt-BR/" + definition.path + "/"
	subject, err := parseTextTemplate(templateFS, root+"subject.tmpl", true)
	if err != nil {
		return compiledEmailTemplate{}, err
	}
	plainText, err := parseTextTemplate(templateFS, root+"text.tmpl", false)
	if err != nil {
		return compiledEmailTemplate{}, err
	}
	html, err := parseHTMLTemplate(templateFS, root+"html.tmpl")
	if err != nil {
		return compiledEmailTemplate{}, err
	}
	return compiledEmailTemplate{
		version: definition.version,
		subject: subject,
		text:    plainText,
		html:    html,
	}, nil
}

func parseTextTemplate(templateFS fs.FS, path string, trimFinalNewline bool) (*texttemplate.Template, error) {
	source, err := fs.ReadFile(templateFS, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if trimFinalNewline {
		source = bytes.TrimSuffix(source, []byte("\n"))
		source = bytes.TrimSuffix(source, []byte("\r"))
	}
	return texttemplate.New(path).
		Funcs(templateFunctions()).
		Option("missingkey=error").
		Parse(string(source))
}

func parseHTMLTemplate(templateFS fs.FS, path string) (*htmltemplate.Template, error) {
	source, err := fs.ReadFile(templateFS, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return htmltemplate.New(path).
		Funcs(htmltemplate.FuncMap(templateFunctions())).
		Option("missingkey=error").
		Parse(string(source))
}

func templateFunctions() texttemplate.FuncMap {
	return texttemplate.FuncMap{
		"duration":  formatDurationPTBR,
		"number":    formatNumberPTBR,
		"numberPtr": formatNumberPointerPTBR,
		"utc":       fixedUTCTimestamp,
	}
}

func validateCompiledTemplate(compiled compiledEmailTemplate) error {
	observed := 1.5
	data := EmailTemplateData{
		ObservedValue:    &observed,
		EvaluationWindow: time.Minute,
	}
	subject, err := executeTextTemplate(compiled.subject, data)
	if err != nil {
		return fmt.Errorf("execute subject: %w", err)
	}
	text, err := executeTextTemplate(compiled.text, data)
	if err != nil {
		return fmt.Errorf("execute plain text: %w", err)
	}
	html, err := executeHTMLTemplate(compiled.html, data)
	if err != nil {
		return fmt.Errorf("execute HTML: %w", err)
	}
	return validateRenderedEmail(subject, text, html)
}

func executeTextTemplate(template *texttemplate.Template, data EmailTemplateData) (string, error) {
	var output bytes.Buffer
	if err := template.Execute(&output, data); err != nil {
		return "", err
	}
	return output.String(), nil
}

func executeHTMLTemplate(template *htmltemplate.Template, data EmailTemplateData) (string, error) {
	var output bytes.Buffer
	if err := template.Execute(&output, data); err != nil {
		return "", err
	}
	return output.String(), nil
}

func validateRenderedEmail(subject, text, html string) error {
	if strings.ContainsAny(subject, "\r\n") {
		return fmt.Errorf("email subject must not contain CR or LF")
	}
	if utf8.RuneCountInString(subject) > MaxEmailSubjectRunes {
		return fmt.Errorf("email subject must not exceed %d runes", MaxEmailSubjectRunes)
	}
	if len(text) > MaxEmailTextBytes {
		return fmt.Errorf("email plain text must not exceed %d bytes", MaxEmailTextBytes)
	}
	if len(html) > MaxEmailHTMLBytes {
		return fmt.Errorf("email HTML must not exceed %d bytes", MaxEmailHTMLBytes)
	}
	return nil
}

func formatNumberPointerPTBR(value *float64) (string, error) {
	if value == nil {
		return "", nil
	}
	return formatNumberPTBR(*value)
}

func formatNumberPTBR(value float64) (string, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", fmt.Errorf("number must be finite")
	}
	if value == 0 {
		value = 0
	}
	raw := strconv.FormatFloat(value, 'f', -1, 64)
	sign := ""
	if strings.HasPrefix(raw, "-") {
		sign = "-"
		raw = strings.TrimPrefix(raw, "-")
	}
	parts := strings.SplitN(raw, ".", 2)
	integer := parts[0]
	for index := len(integer) - 3; index > 0; index -= 3 {
		integer = integer[:index] + "." + integer[index:]
	}
	if len(parts) == 1 {
		return sign + integer, nil
	}
	return sign + integer + "," + parts[1], nil
}

func formatDurationPTBR(value time.Duration) (string, error) {
	if value <= 0 {
		return "", fmt.Errorf("evaluation window must be positive")
	}
	if value%time.Hour == 0 {
		return strconv.FormatInt(int64(value/time.Hour), 10) + " h", nil
	}
	if value%time.Minute == 0 {
		return strconv.FormatInt(int64(value/time.Minute), 10) + " min", nil
	}
	if value%time.Second == 0 {
		return strconv.FormatInt(int64(value/time.Second), 10) + " s", nil
	}
	seconds, err := formatNumberPTBR(value.Seconds())
	if err != nil {
		return "", err
	}
	return seconds + " s", nil
}
