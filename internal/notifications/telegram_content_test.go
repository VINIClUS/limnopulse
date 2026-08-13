package notifications

import (
	"strings"
	"testing"
	"time"
)

func TestTelegramRenderedContentRoundTripsAndDetectsTampering(t *testing.T) {
	content, err := NewTelegramRenderedContent(
		TemplateTelegramAlertOpeningV1,
		LocalePTBR,
		"🚨 Alerta crítico\nTanque: Viveiro Sul\nEvento: alert_1",
	)
	if err != nil {
		t.Fatalf("NewTelegramRenderedContent() error = %v", err)
	}
	snapshot := content.Snapshot()
	restored, err := RestoreTelegramRenderedContent(snapshot)
	if err != nil || restored != content {
		t.Fatalf("RestoreTelegramRenderedContent() = %#v, %v", restored, err)
	}
	snapshot.BodyText += "tampered"
	if _, err := RestoreTelegramRenderedContent(snapshot); err == nil {
		t.Fatal("tampered Telegram content accepted")
	}
}

func TestTelegramRenderedContentUsesRuneLimit(t *testing.T) {
	body := strings.Repeat("á", MaxTelegramBodyRunes)
	if _, err := NewTelegramRenderedContent(TemplateTelegramAlertOpeningV1, LocalePTBR, body); err != nil {
		t.Fatalf("rune-safe body rejected: %v", err)
	}
	if _, err := NewTelegramRenderedContent(TemplateTelegramAlertOpeningV1, LocalePTBR, body+"x"); err == nil {
		t.Fatal("body beyond internal Telegram limit accepted")
	}
}

func TestTelegramDeliverySnapshotCarriesOnlyImmutableDestinationAndContent(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	deliveryID, err := NewDeliveryID(
		"alert_1",
		NotificationKindOpening,
		ChannelTelegram,
		"sub_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	content, err := NewTelegramRenderedContent(
		TemplateTelegramAlertOpeningV1,
		LocalePTBR,
		"Alerta do viveiro\nEvento: alert_1",
	)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := NewPendingDelivery(DeliveryParams{
		TenantID: "tnt_1", OutboxID: "outbox_1", DeliveryID: deliveryID,
		EventID: "alert_1", RuleID: "rule_1", Kind: NotificationKindOpening,
		Channel: ChannelTelegram, RecipientID: "sub_1",
		DestinationID: "destination_hash", TelegramChatID: 123,
		MembershipSnapshot: MembershipSnapshot{Role: "member", Status: "active", Version: 1},
		TelegramContent:    content, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("NewPendingDelivery() error = %v", err)
	}
	snapshot := delivery.Snapshot()
	if snapshot.Channel != ChannelTelegram || snapshot.DestinationID != "destination_hash" ||
		snapshot.TelegramChatID != 123 || snapshot.TelegramContent.BodyText == "" {
		t.Fatalf("Telegram delivery snapshot = %#v", snapshot)
	}
	if snapshot.NormalizedEmail != "" || snapshot.Content != (RenderedContentSnapshot{}) {
		t.Fatalf("Telegram delivery leaked email variant = %#v", snapshot)
	}
	if _, err := RestoreDelivery(snapshot); err != nil {
		t.Fatalf("RestoreDelivery() error = %v", err)
	}
}
