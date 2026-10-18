package influx

import (
	"encoding/hex"

	"github.com/google/uuid"
)

// metricKeyByLegacyMetric maps each legacy alert metric (allowedMetrics) to
// its metric-catalog@1 key.
var metricKeyByLegacyMetric = map[string]string{
	"temp_c":        "water.temperature",
	"ph":            "water.ph",
	"do_mg_l":       "water.dissolved_oxygen",
	"turbidity_ntu": "water.turbidity",
	"salinity_ppt":  "water.salinity",
	"battery_v":     "device.battery_voltage",
	"rssi":          "device.rssi",
}

// legacyV2ID mirrors the Python domain.ids._legacy_v2_id: a uuid5 over
// NAMESPACE_URL, rendered as "<prefix>_<32 hex>".
func legacyV2ID(prefix, discriminator string) string {
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("limnopulse/v4/"+discriminator))
	return prefix + "_" + hex.EncodeToString(id[:])
}

// AssetIDForLegacyPond returns the Asset ID of a legacy pond.
func AssetIDForLegacyPond(tenantID, pondID string) string {
	return legacyV2ID("ast", "legacy-pond/"+tenantID+"/"+pondID)
}

// DefaultComponentIDForLegacyDevice returns the default Component ID of a
// legacy device.
func DefaultComponentIDForLegacyDevice(tenantID, deviceID string) string {
	return legacyV2ID("cmp", "legacy-device-component/"+tenantID+"/"+deviceID)
}

// MetricKeyForLegacyMetric maps a legacy alert metric to its catalog key.
func MetricKeyForLegacyMetric(metric string) (string, bool) {
	key, ok := metricKeyByLegacyMetric[metric]
	return key, ok
}
