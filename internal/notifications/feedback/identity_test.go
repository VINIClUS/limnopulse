package feedback

import "testing"

func TestFeedbackDedupeKeysAreHashedStableAndDistinguishBounceSemantics(t *testing.T) {
	transport, err := TransportDedupeKey("evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if transport.PartitionKey != "SES_FEEDBACK_TRANSPORT#f2ff07b53e63c121122d62f54a1192127056b465279d2c76b0e2a7a237b2f04c" ||
		transport.SortKey != "EVENT" {
		t.Fatalf("transport key = %#v", transport)
	}
	soft, err := SemanticDedupeKey("ses_message_1", SemanticSoftBounce)
	if err != nil {
		t.Fatal(err)
	}
	hard, err := SemanticDedupeKey("ses_message_1", SemanticHardBounce)
	if err != nil {
		t.Fatal(err)
	}
	if soft == hard || soft.PartitionKey == "" || hard.PartitionKey == "" || soft.SortKey != "EVENT" || hard.SortKey != "EVENT" {
		t.Fatalf("soft=%#v hard=%#v", soft, hard)
	}
}

func TestFeedbackDedupeKeysRejectInvalidInputs(t *testing.T) {
	for _, build := range []func() error{
		func() error { _, err := TransportDedupeKey(""); return err },
		func() error { _, err := TransportDedupeKey("evt\x001"); return err },
		func() error { _, err := SemanticDedupeKey("", SemanticSend); return err },
		func() error { _, err := SemanticDedupeKey("ses_1", "invalid"); return err },
	} {
		if err := build(); err == nil {
			t.Fatal("invalid feedback identity accepted")
		}
	}
}
