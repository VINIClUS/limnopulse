package notifications

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"unicode/utf8"
)

const MaxTelegramBodyRunes = 3800

type TelegramRenderedContentSnapshot struct {
	TemplateID      TemplateID `json:"template_id"`
	TemplateVersion int        `json:"template_version"`
	Locale          Locale     `json:"locale"`
	BodyText        string     `json:"body_text"`
	ContentHash     string     `json:"content_hash"`
}

type TelegramRenderedContent struct {
	templateID      TemplateID
	templateVersion int
	locale          Locale
	bodyText        string
	contentHash     string
}

func NewTelegramRenderedContent(
	templateID TemplateID,
	locale Locale,
	bodyText string,
) (TelegramRenderedContent, error) {
	version, err := templateID.Version()
	if err != nil {
		return TelegramRenderedContent{}, err
	}
	snapshot := TelegramRenderedContentSnapshot{
		TemplateID: templateID, TemplateVersion: version, Locale: locale,
		BodyText: bodyText,
	}
	snapshot.ContentHash = telegramRenderedContentHash(snapshot)
	return RestoreTelegramRenderedContent(snapshot)
}

func (content TelegramRenderedContent) TemplateID() TemplateID { return content.templateID }
func (content TelegramRenderedContent) TemplateVersion() int   { return content.templateVersion }
func (content TelegramRenderedContent) Locale() Locale         { return content.locale }
func (content TelegramRenderedContent) BodyText() string       { return content.bodyText }
func (content TelegramRenderedContent) ContentHash() string    { return content.contentHash }

func (content TelegramRenderedContent) Snapshot() TelegramRenderedContentSnapshot {
	return TelegramRenderedContentSnapshot{
		TemplateID: content.templateID, TemplateVersion: content.templateVersion,
		Locale: content.locale, BodyText: content.bodyText, ContentHash: content.contentHash,
	}
}

func (content TelegramRenderedContent) Validate() error {
	_, err := RestoreTelegramRenderedContent(content.Snapshot())
	return err
}

func RestoreTelegramRenderedContent(
	snapshot TelegramRenderedContentSnapshot,
) (TelegramRenderedContent, error) {
	version, err := snapshot.TemplateID.Version()
	if err != nil {
		return TelegramRenderedContent{}, err
	}
	if snapshot.TemplateID != TemplateTelegramAlertOpeningV1 &&
		snapshot.TemplateID != TemplateTelegramAlertRecoveryV1 {
		return TelegramRenderedContent{}, fmt.Errorf("template %q is not a Telegram template", snapshot.TemplateID)
	}
	if snapshot.TemplateVersion != version {
		return TelegramRenderedContent{}, fmt.Errorf("template version must be %d", version)
	}
	if err := snapshot.Locale.Validate(); err != nil {
		return TelegramRenderedContent{}, err
	}
	if snapshot.BodyText == "" || !utf8.ValidString(snapshot.BodyText) {
		return TelegramRenderedContent{}, fmt.Errorf("Telegram body must be non-empty valid UTF-8")
	}
	if utf8.RuneCountInString(snapshot.BodyText) > MaxTelegramBodyRunes {
		return TelegramRenderedContent{}, fmt.Errorf("Telegram body exceeds %d runes", MaxTelegramBodyRunes)
	}
	if snapshot.ContentHash != telegramRenderedContentHash(snapshot) {
		return TelegramRenderedContent{}, fmt.Errorf("rendered Telegram content hash mismatch")
	}
	return TelegramRenderedContent{
		templateID: snapshot.TemplateID, templateVersion: snapshot.TemplateVersion,
		locale: snapshot.Locale, bodyText: snapshot.BodyText, contentHash: snapshot.ContentHash,
	}, nil
}

func telegramRenderedContentHash(snapshot TelegramRenderedContentSnapshot) string {
	hash := sha256.New()
	for _, field := range []string{
		"limnopulse:rendered-telegram:v1",
		string(snapshot.TemplateID),
		strconv.Itoa(snapshot.TemplateVersion),
		string(snapshot.Locale),
		snapshot.BodyText,
	} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(field))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
