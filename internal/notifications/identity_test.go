package notifications

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type identityFixture struct {
	SchemaVersion   int              `json:"schema_version"`
	RelayVectors    []relayVector    `json:"relay_vectors"`
	DeliveryVectors []deliveryVector `json:"delivery_vectors"`
}

type relayVector struct {
	WorkKind  WorkKind `json:"work_kind"`
	TenantID  string   `json:"tenant_id"`
	ItemID    string   `json:"item_id"`
	Scheduled string   `json:"scheduled_at"`
	Bucket    int      `json:"bucket"`
	GSIPK     string   `json:"gsi_pk"`
	GSISK     string   `json:"gsi_sk"`
}

type deliveryVector struct {
	EventID     string           `json:"event_id"`
	Kind        NotificationKind `json:"kind"`
	Channel     Channel          `json:"channel"`
	RecipientID string           `json:"recipient_id"`
	DeliveryID  string           `json:"delivery_id"`
}

func TestIdentityGoldenVectors(t *testing.T) {
	fixture := loadIdentityFixture(t)
	if fixture.SchemaVersion != 1 {
		t.Fatalf("fixture schema version = %d, want 1", fixture.SchemaVersion)
	}

	for _, vector := range fixture.RelayVectors {
		vector := vector
		t.Run("relay/"+string(vector.WorkKind), func(t *testing.T) {
			scheduledAt, err := time.Parse(time.RFC3339Nano, vector.Scheduled)
			if err != nil {
				t.Fatalf("parse fixture timestamp: %v", err)
			}

			bucket, err := RelayBucket(vector.WorkKind, vector.TenantID, vector.ItemID)
			if err != nil {
				t.Fatalf("RelayBucket() error = %v", err)
			}
			if bucket != vector.Bucket {
				t.Fatalf("RelayBucket() = %d, want %d", bucket, vector.Bucket)
			}

			key, err := BuildRelayIndexKey(vector.WorkKind, vector.TenantID, vector.ItemID, scheduledAt)
			if err != nil {
				t.Fatalf("BuildRelayIndexKey() error = %v", err)
			}
			if key.Bucket != vector.Bucket || key.PartitionKey != vector.GSIPK || key.SortKey != vector.GSISK {
				t.Fatalf("BuildRelayIndexKey() = %#v, want bucket=%d pk=%q sk=%q", key, vector.Bucket, vector.GSIPK, vector.GSISK)
			}
		})
	}

	for _, vector := range fixture.DeliveryVectors {
		vector := vector
		t.Run("delivery/"+string(vector.Kind), func(t *testing.T) {
			got, err := NewDeliveryID(vector.EventID, vector.Kind, vector.Channel, vector.RecipientID)
			if err != nil {
				t.Fatalf("NewDeliveryID() error = %v", err)
			}
			if got != vector.DeliveryID {
				t.Fatalf("NewDeliveryID() = %q, want %q", got, vector.DeliveryID)
			}
		})
	}
}

func TestRelayIdentityRejectsEmptyUnknownAndNULInputs(t *testing.T) {
	tests := []struct {
		name     string
		workKind WorkKind
		tenantID string
		itemID   string
	}{
		{name: "empty work kind", workKind: "", tenantID: "tnt_1", itemID: "item_1"},
		{name: "unknown work kind", workKind: "OTHER", tenantID: "tnt_1", itemID: "item_1"},
		{name: "NUL work kind", workKind: "INTENT\x00", tenantID: "tnt_1", itemID: "item_1"},
		{name: "empty tenant", workKind: WorkKindIntent, tenantID: "", itemID: "item_1"},
		{name: "NUL tenant", workKind: WorkKindIntent, tenantID: "tnt\x00one", itemID: "item_1"},
		{name: "empty item", workKind: WorkKindIntent, tenantID: "tnt_1", itemID: ""},
		{name: "NUL item", workKind: WorkKindIntent, tenantID: "tnt_1", itemID: "item\x00one"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := RelayBucket(tt.workKind, tt.tenantID, tt.itemID); err == nil {
				t.Fatal("RelayBucket() succeeded")
			}
		})
	}
}

func TestDeliveryIdentityRejectsEmptyUnknownAndNULInputs(t *testing.T) {
	tests := []struct {
		name        string
		eventID     string
		kind        NotificationKind
		channel     Channel
		recipientID string
	}{
		{name: "empty event", eventID: "", kind: NotificationKindOpening, channel: ChannelEmail, recipientID: "user_1"},
		{name: "NUL event", eventID: "alert\x001", kind: NotificationKindOpening, channel: ChannelEmail, recipientID: "user_1"},
		{name: "unknown kind", eventID: "alert_1", kind: "other", channel: ChannelEmail, recipientID: "user_1"},
		{name: "NUL kind", eventID: "alert_1", kind: "opening\x00", channel: ChannelEmail, recipientID: "user_1"},
		{name: "unknown channel", eventID: "alert_1", kind: NotificationKindOpening, channel: "sms", recipientID: "user_1"},
		{name: "NUL channel", eventID: "alert_1", kind: NotificationKindOpening, channel: "email\x00", recipientID: "user_1"},
		{name: "empty recipient", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail, recipientID: ""},
		{name: "NUL recipient", eventID: "alert_1", kind: NotificationKindOpening, channel: ChannelEmail, recipientID: "user\x001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewDeliveryID(tt.eventID, tt.kind, tt.channel, tt.recipientID); err == nil {
				t.Fatal("NewDeliveryID() succeeded")
			}
		})
	}
}

func TestDurableStorageKeys(t *testing.T) {
	tests := []struct {
		name  string
		build func() (StorageKey, error)
		want  StorageKey
	}{
		{
			name: "delivery",
			build: func() (StorageKey, error) {
				return DeliveryStorageKey("outbox_1", "del_1")
			},
			want: StorageKey{PartitionKey: "NOTIFICATION_OUTBOX#outbox_1", SortKey: "DELIVERY#del_1"},
		},
		{
			name: "attempt",
			build: func() (StorageKey, error) {
				return AttemptStorageKey("del_1", "attempt_1")
			},
			want: StorageKey{PartitionKey: "NOTIFICATION_DELIVERY#del_1", SortKey: "ATTEMPT#attempt_1"},
		},
		{
			name: "deliverability",
			build: func() (StorageKey, error) {
				return DeliverabilityStorageKey("owner@example.com")
			},
			want: StorageKey{
				PartitionKey: "EMAIL_IDENTITY#c8cd3c6427301eaf6665bccacd65ddb614527acc843a15463e3faba57124c351",
				SortKey:      "DELIVERABILITY",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.build()
			if err != nil {
				t.Fatalf("build key: %v", err)
			}
			if got != tt.want {
				t.Fatalf("key = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestDurableStorageKeysRejectEmptyAndNULInputs(t *testing.T) {
	builders := []func() error{
		func() error { _, err := DeliveryStorageKey("", "del_1"); return err },
		func() error { _, err := DeliveryStorageKey("outbox_1", "del\x001"); return err },
		func() error { _, err := AttemptStorageKey("", "attempt_1"); return err },
		func() error { _, err := AttemptStorageKey("del_1", "attempt\x001"); return err },
		func() error { _, err := DeliverabilityStorageKey(""); return err },
		func() error { _, err := DeliverabilityStorageKey("a\x00@example.com"); return err },
	}
	for index, build := range builders {
		if err := build(); err == nil || !strings.Contains(err.Error(), "must") {
			t.Fatalf("builder %d error = %v", index, err)
		}
	}
}

func loadIdentityFixture(t *testing.T) identityFixture {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "notification_identity_vectors.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read identity fixture: %v", err)
	}
	var fixture identityFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode identity fixture: %v", err)
	}
	return fixture
}
