package notifications

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
)

type RenderedContentSnapshot struct {
	TemplateID      TemplateID `json:"template_id"`
	TemplateVersion int        `json:"template_version"`
	Locale          Locale     `json:"locale"`
	Subject         string     `json:"subject"`
	Text            string     `json:"text"`
	HTML            string     `json:"html"`
	ContentHash     string     `json:"content_hash"`
}

type RenderedContent struct {
	templateID      TemplateID
	templateVersion int
	locale          Locale
	subject         string
	text            string
	html            string
	contentHash     string
}

func newRenderedContent(
	templateID TemplateID,
	templateVersion int,
	locale Locale,
	subject string,
	text string,
	html string,
) RenderedContent {
	return RenderedContent{
		templateID:      templateID,
		templateVersion: templateVersion,
		locale:          locale,
		subject:         subject,
		text:            text,
		html:            html,
		contentHash: renderedContentHash(
			templateID,
			templateVersion,
			locale,
			subject,
			text,
			html,
		),
	}
}

func (content RenderedContent) TemplateID() TemplateID {
	return content.templateID
}

func (content RenderedContent) TemplateVersion() int {
	return content.templateVersion
}

func (content RenderedContent) Locale() Locale {
	return content.locale
}

func (content RenderedContent) Subject() string {
	return content.subject
}

func (content RenderedContent) Text() string {
	return content.text
}

func (content RenderedContent) HTML() string {
	return content.html
}

func (content RenderedContent) ContentHash() string {
	return content.contentHash
}

func (content RenderedContent) Snapshot() RenderedContentSnapshot {
	return RenderedContentSnapshot{
		TemplateID:      content.templateID,
		TemplateVersion: content.templateVersion,
		Locale:          content.locale,
		Subject:         content.subject,
		Text:            content.text,
		HTML:            content.html,
		ContentHash:     content.contentHash,
	}
}

func (content RenderedContent) Validate() error {
	_, err := RestoreRenderedContent(content.Snapshot())
	return err
}

func RestoreRenderedContent(snapshot RenderedContentSnapshot) (RenderedContent, error) {
	version, err := snapshot.TemplateID.Version()
	if err != nil {
		return RenderedContent{}, err
	}
	if snapshot.TemplateVersion != version {
		return RenderedContent{}, fmt.Errorf(
			"template version must be %d for %q",
			version,
			snapshot.TemplateID,
		)
	}
	if err := snapshot.Locale.Validate(); err != nil {
		return RenderedContent{}, err
	}
	if snapshot.Subject == "" || snapshot.Text == "" || snapshot.HTML == "" {
		return RenderedContent{}, fmt.Errorf("rendered email subject, text, and HTML must not be empty")
	}
	if err := validateRenderedEmail(snapshot.Subject, snapshot.Text, snapshot.HTML); err != nil {
		return RenderedContent{}, err
	}
	wantHash := renderedContentHash(
		snapshot.TemplateID,
		snapshot.TemplateVersion,
		snapshot.Locale,
		snapshot.Subject,
		snapshot.Text,
		snapshot.HTML,
	)
	if snapshot.ContentHash != wantHash {
		return RenderedContent{}, fmt.Errorf("rendered email content hash mismatch")
	}
	return RenderedContent{
		templateID:      snapshot.TemplateID,
		templateVersion: snapshot.TemplateVersion,
		locale:          snapshot.Locale,
		subject:         snapshot.Subject,
		text:            snapshot.Text,
		html:            snapshot.HTML,
		contentHash:     snapshot.ContentHash,
	}, nil
}

func renderedContentHash(
	templateID TemplateID,
	templateVersion int,
	locale Locale,
	subject string,
	text string,
	html string,
) string {
	hash := sha256.New()
	for _, field := range []string{
		"limnopulse:rendered-email:v1",
		string(templateID),
		strconv.Itoa(templateVersion),
		string(locale),
		subject,
		text,
		html,
	} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(field))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
