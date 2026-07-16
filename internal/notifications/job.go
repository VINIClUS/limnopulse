package notifications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const (
	DeliveryJobSchemaVersion    = 1
	DeliveryJobMessageType      = "notification.delivery"
	TraceparentMessageAttribute = "traceparent"
)

type JobEnvelope struct {
	SchemaVersion int              `json:"schema_version"`
	MessageType   string           `json:"message_type"`
	TenantID      string           `json:"tenant_id"`
	OutboxID      string           `json:"outbox_id"`
	DeliveryID    string           `json:"delivery_id"`
	EventID       string           `json:"event_id"`
	Kind          NotificationKind `json:"kind"`
	Channel       Channel          `json:"channel"`
}

func NewDeliveryJob(
	tenantID string,
	outboxID string,
	deliveryID string,
	eventID string,
	kind NotificationKind,
	channel Channel,
) (JobEnvelope, error) {
	job := JobEnvelope{
		SchemaVersion: DeliveryJobSchemaVersion,
		MessageType:   DeliveryJobMessageType,
		TenantID:      tenantID,
		OutboxID:      outboxID,
		DeliveryID:    deliveryID,
		EventID:       eventID,
		Kind:          kind,
		Channel:       channel,
	}
	if err := job.Validate(); err != nil {
		return JobEnvelope{}, err
	}
	return job, nil
}

func (job JobEnvelope) Validate() error {
	if job.SchemaVersion != DeliveryJobSchemaVersion {
		return fmt.Errorf("job schema version must be %d", DeliveryJobSchemaVersion)
	}
	if job.MessageType != DeliveryJobMessageType {
		return fmt.Errorf("job message type must be %q", DeliveryJobMessageType)
	}
	for name, value := range map[string]string{
		"tenant ID":   job.TenantID,
		"outbox ID":   job.OutboxID,
		"delivery ID": job.DeliveryID,
		"event ID":    job.EventID,
	} {
		if err := validateIdentityField(name, value); err != nil {
			return err
		}
	}
	if err := job.Kind.Validate(); err != nil {
		return err
	}
	return job.Channel.Validate()
}

func (job JobEnvelope) MarshalJSON() ([]byte, error) {
	if err := job.Validate(); err != nil {
		return nil, err
	}
	type plain JobEnvelope
	return json.Marshal(plain(job))
}

func (job *JobEnvelope) UnmarshalJSON(data []byte) error {
	type plain JobEnvelope
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("job JSON must contain exactly one value")
		}
		return fmt.Errorf("job JSON has trailing data: %w", err)
	}
	candidate := JobEnvelope(decoded)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*job = candidate
	return nil
}
