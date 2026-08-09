package notifications

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	RelayBucketCount = 64
	fixedUTCLayout   = "2006-01-02T15:04:05.000000000Z"
)

type RelayIndexKey struct {
	Bucket       int
	PartitionKey string
	SortKey      string
}

type StorageKey struct {
	PartitionKey string
	SortKey      string
}

func RelayBucket(workKind WorkKind, tenantID, itemID string) (int, error) {
	if err := workKind.Validate(); err != nil {
		return 0, err
	}
	if err := validateIdentityField("tenant ID", tenantID); err != nil {
		return 0, err
	}
	if err := validateIdentityField("item ID", itemID); err != nil {
		return 0, err
	}
	digest := sha256.Sum256([]byte(string(workKind) + "\x00" + tenantID + "\x00" + itemID))
	return int(binary.BigEndian.Uint64(digest[:8]) % RelayBucketCount), nil
}

func BuildRelayIndexKey(
	workKind WorkKind,
	tenantID string,
	itemID string,
	scheduledAt time.Time,
) (RelayIndexKey, error) {
	bucket, err := RelayBucket(workKind, tenantID, itemID)
	if err != nil {
		return RelayIndexKey{}, err
	}
	return RelayIndexKey{
		Bucket:       bucket,
		PartitionKey: fmt.Sprintf("NOTIFICATION_RELAY#V1#BUCKET#%02d", bucket),
		SortKey: fmt.Sprintf(
			"%s#%s#%s#%s",
			fixedUTCTimestamp(scheduledAt),
			workKind,
			base64.RawURLEncoding.EncodeToString([]byte(tenantID)),
			base64.RawURLEncoding.EncodeToString([]byte(itemID)),
		),
	}, nil
}

func NewDeliveryID(
	eventID string,
	kind NotificationKind,
	channel Channel,
	recipientID string,
) (string, error) {
	if err := validateIdentityField("event ID", eventID); err != nil {
		return "", err
	}
	if err := kind.Validate(); err != nil {
		return "", err
	}
	if err := channel.Validate(); err != nil {
		return "", err
	}
	if err := validateIdentityField("recipient ID", recipientID); err != nil {
		return "", err
	}
	canonical := "limnopulse:delivery:v1\x00" + eventID + "\x00" + string(kind) +
		"\x00" + string(channel) + "\x00" + recipientID
	digest := sha256.Sum256([]byte(canonical))
	return "del_" + hex.EncodeToString(digest[:]), nil
}

func OutboxID(eventID string, channel Channel, kind NotificationKind) string {
	canonical := eventID + "\x00" + string(channel) + "\x00" + string(kind)
	digest := sha256.Sum256([]byte(canonical))
	return "outbox_" + hex.EncodeToString(digest[:])
}

func DeliveryStorageKey(outboxID, deliveryID string) (StorageKey, error) {
	if err := validateIdentityField("outbox ID", outboxID); err != nil {
		return StorageKey{}, err
	}
	if err := validateIdentityField("delivery ID", deliveryID); err != nil {
		return StorageKey{}, err
	}
	return StorageKey{
		PartitionKey: "NOTIFICATION_OUTBOX#" + outboxID,
		SortKey:      "DELIVERY#" + deliveryID,
	}, nil
}

func AttemptStorageKey(deliveryID, attemptID string) (StorageKey, error) {
	if err := validateIdentityField("delivery ID", deliveryID); err != nil {
		return StorageKey{}, err
	}
	if err := validateIdentityField("attempt ID", attemptID); err != nil {
		return StorageKey{}, err
	}
	return StorageKey{
		PartitionKey: "NOTIFICATION_DELIVERY#" + deliveryID,
		SortKey:      "ATTEMPT#" + attemptID,
	}, nil
}

func DeliverabilityStorageKey(normalizedEmail string) (StorageKey, error) {
	if err := validateIdentityField("normalized email", normalizedEmail); err != nil {
		return StorageKey{}, err
	}
	digest := sha256.Sum256([]byte(normalizedEmail))
	return StorageKey{
		PartitionKey: "EMAIL_IDENTITY#" + hex.EncodeToString(digest[:]),
		SortKey:      "DELIVERABILITY",
	}, nil
}

func validateIdentityField(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must not contain NUL", name)
	}
	return nil
}

func fixedUTCTimestamp(value time.Time) string {
	return value.UTC().Format(fixedUTCLayout)
}
