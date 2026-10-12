// Output 1: GET /v1 device request. Output 2: verdict (reading rejected).
const EXPECTED_KEYS = [
    "battery_v", "do_mg_l", "ph", "rssi", "salinity_ppt",
    "seq", "temp_c", "ts", "turbidity_ntu",
];
// Strip trailing slashes without a regex (linear, no backtracking).
let base = String(env.get("LIMNOPULSE_API_URL") || "http://127.0.0.1:8000");
while (base.endsWith("/")) {
    base = base.slice(0, -1);
}
const tenant = String(env.get("LIMNOPULSE_TENANT") || "tnt_local_001");
const parts = String(msg.topic || "").split("/");
// The switch node only lets objects through; never trust that here.
const reading = msg.payload !== null && typeof msg.payload === "object" && !Array.isArray(msg.payload)
    ? msg.payload
    : {};

const ctx = {
    base: base,
    tenant: tenant,
    device_id: parts.length === 5 && parts[4] === "readings" ? parts[3] : null,
    seq: Object.hasOwn(reading, "seq") ? reading.seq : null,
    ts: typeof reading.ts === "string" ? reading.ts : null,
    v1_pond_id: null,
    component_id: null,
    native: false,
    resolved_asset_id: null,
    expected_asset_id: null,
};
msg.e2e = ctx;

function verdict(ok, reason) {
    msg.payload = {
        device_id: ctx.device_id,
        seq: ctx.seq,
        ts: ctx.ts,
        v1_pond_id: ctx.v1_pond_id,
        resolved_asset_id: ctx.resolved_asset_id,
        expected_asset_id: ctx.expected_asset_id,
        ok: ok,
        reason: reason,
    };
    return [null, msg];
}

if (!ctx.device_id) {
    return verdict(false, "bad-topic");
}
// Code-point order, the same order as EXPECTED_KEYS (all keys are ASCII).
function byCodePoint(a, b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}
const keys = Object.keys(reading).sort(byCodePoint);
if (keys.join(",") !== EXPECTED_KEYS.join(",")) {
    return verdict(false, "payload-keys-mismatch");
}
if (!ctx.ts || !Number.isFinite(Date.parse(ctx.ts))) {
    return verdict(false, "bad-ts");
}

msg.method = "GET";
msg.url = base + "/v1/tenants/" + encodeURIComponent(tenant) + "/devices/" + encodeURIComponent(ctx.device_id);
msg.headers = {
    "Accept": "application/json",
    "X-Dev-User-Sub": String(env.get("LIMNOPULSE_DEV_USER_SUB") || "local-user-001"),
    "X-Dev-User-Email": String(env.get("LIMNOPULSE_DEV_USER_EMAIL") || "local@example.test"),
};
msg.payload = undefined;
return [msg, null];
