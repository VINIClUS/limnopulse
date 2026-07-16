package notifications

import "testing"

func TestRenderedContentSnapshotRoundTripsWithoutRerender(t *testing.T) {
	original := mustRenderedContent(t)
	snapshot := original.Snapshot()
	restored, err := RestoreRenderedContent(snapshot)
	if err != nil {
		t.Fatalf("RestoreRenderedContent() error = %v", err)
	}
	if restored != original {
		t.Fatalf("restored content differs:\n got %#v\nwant %#v", restored, original)
	}

	snapshot.Subject = "mutated outside the immutable value"
	if original.Subject() == snapshot.Subject {
		t.Fatal("mutating persistence snapshot changed immutable content")
	}
}

func TestRenderedContentRestoreRejectsTampering(t *testing.T) {
	snapshot := mustRenderedContent(t).Snapshot()
	snapshot.HTML += "<p>tampered</p>"
	if _, err := RestoreRenderedContent(snapshot); err == nil {
		t.Fatal("tampered content hash accepted")
	}
}

func TestDeliveryStateChangesDoNotRerenderContent(t *testing.T) {
	deliveryID, err := NewDeliveryID("alert_1", NotificationKindOpening, ChannelEmail, "user_1")
	if err != nil {
		t.Fatalf("NewDeliveryID() error = %v", err)
	}
	delivery := validDelivery(t, deliveryID)
	original := delivery.Content

	delivery.State = DeliveryStateQueued
	delivery.UpdatedAt = delivery.UpdatedAt.Add(1)
	if delivery.Content != original {
		t.Fatal("delivery transition changed persisted rendered content")
	}
}
