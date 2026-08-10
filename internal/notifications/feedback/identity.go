package feedback

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/notifications"
)

func TransportDedupeKey(eventBridgeID string) (notifications.StorageKey, error) {
	if eventBridgeID == "" || strings.ContainsRune(eventBridgeID, '\x00') {
		return notifications.StorageKey{}, fmt.Errorf("EventBridge event ID is invalid")
	}
	digest := sha256.Sum256([]byte("limnopulse:ses-feedback-transport:v1\x00" + eventBridgeID))
	return notifications.StorageKey{
		PartitionKey: "SES_FEEDBACK_TRANSPORT#" + hex.EncodeToString(digest[:]),
		SortKey:      "EVENT",
	}, nil
}

func SemanticDedupeKey(
	providerMessageID string,
	semanticType SemanticType,
) (notifications.StorageKey, error) {
	if providerMessageID == "" || strings.ContainsRune(providerMessageID, '\x00') {
		return notifications.StorageKey{}, fmt.Errorf("provider message ID is invalid")
	}
	if err := semanticType.Validate(); err != nil {
		return notifications.StorageKey{}, err
	}
	digest := sha256.Sum256([]byte("limnopulse:ses-feedback-semantic:v1\x00" + providerMessageID + "\x00" + string(semanticType)))
	return notifications.StorageKey{
		PartitionKey: "SES_PROVIDER_EVENT#" + hex.EncodeToString(digest[:]),
		SortKey:      "EVENT",
	}, nil
}
