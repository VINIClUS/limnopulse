package main

import (
	"testing"
)

func TestActiveAlertIndexBackfillRequiresExplicitTenant(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	if got := runMain([]string{"backfill-active-alert-index"}); got == 0 {
		t.Fatal("expected missing tenant scope to fail")
	}
}

func TestActiveAlertIndexBackfillRejectsInvalidFlag(t *testing.T) {
	if got := runMain([]string{"backfill-active-alert-index", "--limit=-1"}); got == 0 {
		t.Fatal("expected invalid limit to fail")
	}
}

func TestActiveAlertIndexBackfillAcceptsTenantIDAlias(t *testing.T) {
	config, err := parseActiveAlertIndexBackfillArgs([]string{"--tenant-id", "tnt_123"})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.tenants) != 1 || config.tenants[0] != "tnt_123" {
		t.Fatalf("tenants = %#v", config.tenants)
	}
}
