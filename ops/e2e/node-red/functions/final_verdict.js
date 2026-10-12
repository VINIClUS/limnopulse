// Single output: verdict. Legacy devices: cross-checks the resolved asset
// against the deterministic legacy-pond asset id and the pond profile's
// legacy_pond_id. Native v2 devices: the resolved asset must exist; there is
// no /v1 pond to compare with, so expected_asset_id and v1_pond_id stay null.
const ctx = msg.e2e;

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
    return msg;
}

// HTTP nodes return text; undefined = body is not JSON.
function parseBody(raw) {
    if (raw !== null && typeof raw === "object" && !Buffer.isBuffer(raw)) return raw;
    if (raw === undefined || raw === null) return undefined;
    try {
        return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw));
    } catch (e) {
        return undefined;
    }
}

// AssetResponse: {"asset_id": ..., "pond_profile": {"legacy_pond_id": ...}}.
function legacyPondId(asset) {
    const profile = asset.pond_profile;
    if (!profile || typeof profile !== "object" || !Object.hasOwn(profile, "legacy_pond_id")) {
        return undefined;
    }
    return profile.legacy_pond_id;
}

const body = parseBody(msg.payload);
if (ctx.native) {
    if (msg.statusCode !== 200 || !body || typeof body !== "object") {
        return verdict(false, "v2-native-asset-lookup-error-" + String(msg.statusCode));
    }
    const assetId = body.asset_id;
    if (assetId !== ctx.resolved_asset_id) {
        return verdict(false, "v2-native-asset-mismatch");
    }
    return verdict(true, "v2-native");
}

if (msg.statusCode !== 200 || !body || typeof body !== "object") {
    return verdict(false, "asset-lookup-error-" + String(msg.statusCode));
}
const legacyAssetId = body.asset_id;
if (legacyAssetId !== ctx.resolved_asset_id) {
    return verdict(false, "asset-response-mismatch");
}
if (ctx.resolved_asset_id !== ctx.expected_asset_id) {
    // An ended deployment means the reading predates a relocation: the
    // expected asset (from the device's current /v1 pond) no longer applies.
    return verdict(false, ctx.deployment_ended ? "asset-mismatch-ended-deployment" : "asset-mismatch");
}
const legacy = legacyPondId(body);
if (legacy === undefined) {
    return verdict(false, "asset-without-legacy-pond-id");
}
if (legacy !== ctx.v1_pond_id) {
    return verdict(false, "legacy-pond-mismatch");
}
return verdict(true, "match");
