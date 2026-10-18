package influx

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// contractVectors mirrors the parts of testdata/telemetry_v2/contract_vectors.json
// (frozen by P2-00) that the Go evaluator consumes.
type contractVectors struct {
	MetricCatalog []struct {
		MetricKey   string  `json:"metric_key"`
		LegacyField *string `json:"legacy_field"`
	} `json:"metric_catalog"`
	IDVectors struct {
		AssetIDForLegacyPond []struct {
			TenantID string `json:"tenant_id"`
			PondID   string `json:"pond_id"`
			AssetID  string `json:"asset_id"`
		} `json:"asset_id_for_legacy_pond"`
		DefaultComponentIDForLegacyDevice []struct {
			TenantID    string `json:"tenant_id"`
			DeviceID    string `json:"device_id"`
			ComponentID string `json:"component_id"`
		} `json:"default_component_id_for_legacy_device"`
	} `json:"id_vectors"`
	Scenarios map[string]vectorScenario `json:"scenarios"`
}

type vectorScenario struct {
	TenantID          string              `json:"tenant_id"`
	Devices           []string            `json:"devices"`
	Components        []string            `json:"components"`
	LegacyPondByAsset map[string]string   `json:"legacy_pond_by_asset"`
	Observations      []vectorObservation `json:"observations"`
	V2Lines           []string            `json:"v2_lines"`
	LegacyLines       []string            `json:"legacy_lines"`
}

type vectorObservation struct {
	Resolved struct {
		TenantID    string `json:"tenant_id"`
		AssetID     string `json:"asset_id"`
		DeviceID    string `json:"device_id"`
		ComponentID string `json:"component_id"`
	} `json:"resolved"`
	Observation struct {
		MetricKey  string  `json:"metric_key"`
		Value      float64 `json:"value"`
		ObservedAt string  `json:"observed_at"`
		Sequence   *int64  `json:"sequence"`
	} `json:"observation"`
	Quality struct {
		State string `json:"state"`
	} `json:"quality"`
}

// loadContractVectors reads the cross-language vectors from the repository
// root, located from this file's path so the test is independent of the
// working directory.
func loadContractVectors(t *testing.T) contractVectors {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "testdata", "telemetry_v2", "contract_vectors.json"))
	if err != nil {
		t.Fatalf("read contract vectors: %v", err)
	}
	var vectors contractVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode contract vectors: %v", err)
	}
	if len(vectors.MetricCatalog) == 0 || len(vectors.Scenarios) == 0 {
		t.Fatal("contract vectors are empty")
	}
	return vectors
}

func TestAssetIDForLegacyPondMatchesPythonVectors(t *testing.T) {
	vectors := loadContractVectors(t).IDVectors.AssetIDForLegacyPond
	if len(vectors) < 5 {
		t.Fatalf("got %d asset vectors, want at least 5", len(vectors))
	}
	for _, vector := range vectors {
		if got := AssetIDForLegacyPond(vector.TenantID, vector.PondID); got != vector.AssetID {
			t.Errorf("AssetIDForLegacyPond(%q, %q) = %q, want %q", vector.TenantID, vector.PondID, got, vector.AssetID)
		}
	}
}

func TestDefaultComponentIDForLegacyDeviceMatchesPythonVectors(t *testing.T) {
	vectors := loadContractVectors(t).IDVectors.DefaultComponentIDForLegacyDevice
	if len(vectors) < 5 {
		t.Fatalf("got %d component vectors, want at least 5", len(vectors))
	}
	for _, vector := range vectors {
		if got := DefaultComponentIDForLegacyDevice(vector.TenantID, vector.DeviceID); got != vector.ComponentID {
			t.Errorf("DefaultComponentIDForLegacyDevice(%q, %q) = %q, want %q", vector.TenantID, vector.DeviceID, got, vector.ComponentID)
		}
	}
}

func TestLegacyIDsAreDistinctAcrossKinds(t *testing.T) {
	// The same tenant/entity pair must not collide between an Asset and a
	// Component: each kind has its own discriminator.
	asset := AssetIDForLegacyPond("tnt_local_001", "same-id")
	component := DefaultComponentIDForLegacyDevice("tnt_local_001", "same-id")
	if asset[len("ast_"):] == component[len("cmp_"):] {
		t.Fatalf("asset %q and component %q share a uuid", asset, component)
	}
}

func TestMetricKeyForLegacyMetricMatchesCatalog(t *testing.T) {
	catalog := loadContractVectors(t).MetricCatalog
	mapped := 0
	for _, row := range catalog {
		if row.LegacyField == nil {
			continue
		}
		mapped++
		got, ok := MetricKeyForLegacyMetric(*row.LegacyField)
		if !ok || got != row.MetricKey {
			t.Errorf("MetricKeyForLegacyMetric(%q) = (%q, %v), want (%q, true)", *row.LegacyField, got, ok, row.MetricKey)
		}
		if _, allowed := allowedMetrics[*row.LegacyField]; !allowed {
			t.Errorf("catalog legacy field %q is not a legacy alert metric", *row.LegacyField)
		}
	}
	if mapped != len(allowedMetrics) {
		t.Fatalf("catalog maps %d legacy fields, legacy alert metrics are %d", mapped, len(allowedMetrics))
	}
}

func TestMetricKeyForLegacyMetricRejectsUnknownNames(t *testing.T) {
	for _, metric := range []string{"", "seq", "water.temperature", "PH", "Temp_C", "ph ", "do_mg_l\x00"} {
		if got, ok := MetricKeyForLegacyMetric(metric); ok || got != "" {
			t.Errorf("MetricKeyForLegacyMetric(%q) = (%q, %v), want (\"\", false)", metric, got, ok)
		}
	}
}
