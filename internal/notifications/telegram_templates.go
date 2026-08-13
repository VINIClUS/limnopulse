package notifications

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

func RenderTelegramAlert(
	kind NotificationKind,
	data EmailTemplateData,
	webURL string,
) (TelegramRenderedContent, error) {
	return RenderTelegramAlertForEnvironment(kind, data, webURL, false)
}

func RenderTelegramAlertForEnvironment(
	kind NotificationKind,
	data EmailTemplateData,
	webURL string,
	allowInsecure bool,
) (TelegramRenderedContent, error) {
	if err := kind.Validate(); err != nil {
		return TelegramRenderedContent{}, err
	}
	if err := data.Validate(); err != nil {
		return TelegramRenderedContent{}, err
	}
	link, err := BuildAlertEventURL(webURL, data.TenantID, data.EventID, allowInsecure)
	if err != nil {
		return TelegramRenderedContent{}, err
	}
	title := "🚨 Alerta " + telegramSeverityPTBR(data.Severity)
	templateID := TemplateTelegramAlertOpeningV1
	if kind == NotificationKindRecovery {
		title = "✅ Alerta recuperado"
		templateID = TemplateTelegramAlertRecoveryV1
	}
	value := "indisponível"
	if data.ObservedValue != nil {
		value = formatTelegramNumber(*data.ObservedValue)
	}
	unitSuffix := ""
	if strings.TrimSpace(data.Unit) != "" {
		unitSuffix = " " + strings.TrimSpace(data.Unit)
	}
	condition := fmt.Sprintf(
		"%s %s%s",
		strings.TrimSpace(data.Operator),
		formatTelegramNumber(data.Threshold),
		unitSuffix,
	)
	fixedBeforeRule := []string{
		title,
		"Severidade: " + strings.TrimSpace(data.Severity),
		"Tanque: " + strings.TrimSpace(data.PondID),
	}
	if strings.TrimSpace(data.DeviceID) != "" {
		fixedBeforeRule = append(fixedBeforeRule, "Dispositivo: "+strings.TrimSpace(data.DeviceID))
	}
	fixedAfterRule := []string{
		fmt.Sprintf("Valor: %s%s (%s)", value, unitSuffix, condition),
		fmt.Sprintf(
			"Janela: %s – %s",
			data.WindowStart.UTC().Format("2006-01-02 15:04:05Z"),
			data.WindowEnd.UTC().Format("2006-01-02 15:04:05Z"),
		),
		"Evento: " + strings.TrimSpace(data.EventID),
		link,
	}
	fixedRunes := utf8.RuneCountInString(strings.Join(fixedBeforeRule, "\n")) +
		utf8.RuneCountInString(strings.Join(fixedAfterRule, "\n")) +
		2 // newlines around the rule line
	rulePrefix := "Regra: "
	availableRuleRunes := MaxTelegramBodyRunes - fixedRunes - utf8.RuneCountInString(rulePrefix)
	if availableRuleRunes < 1 {
		return TelegramRenderedContent{}, fmt.Errorf("Telegram required content exceeds internal limit")
	}
	ruleName := truncateRunes(strings.TrimSpace(data.RuleName), availableRuleRunes)
	lines := append(fixedBeforeRule, rulePrefix+ruleName)
	lines = append(lines, fixedAfterRule...)
	return NewTelegramRenderedContent(templateID, LocalePTBR, strings.Join(lines, "\n"))
}

func BuildAlertEventURL(
	base string,
	tenantID string,
	eventID string,
	allowInsecure bool,
) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(base))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return "", fmt.Errorf("invalid Limnopulse web URL")
	}
	if parsed.Scheme != "https" && !(allowInsecure && parsed.Scheme == "http") {
		return "", fmt.Errorf("Limnopulse web URL must use HTTPS")
	}
	if err := validateIdentityField("tenant ID", tenantID); err != nil {
		return "", err
	}
	if err := validateIdentityField("event ID", eventID); err != nil {
		return "", err
	}
	basePath := strings.TrimSuffix(parsed.EscapedPath(), "/")
	parsed.RawPath = ""
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimSuffix(parsed.String(), "/") + basePath +
		"/tenants/" + url.PathEscape(tenantID) +
		"/alert-events/" + url.PathEscape(eventID), nil
}

func telegramSeverityPTBR(severity string) string {
	if strings.TrimSpace(severity) == "critical" {
		return "crítico"
	}
	return "de aviso"
}

func formatTelegramNumber(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func truncateRunes(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	if limit <= 1 {
		return "…"
	}
	runes := []rune(value)
	return string(runes[:limit-1]) + "…"
}
