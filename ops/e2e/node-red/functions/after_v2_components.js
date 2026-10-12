// Output 1: GET /v2 deployments request. Output 2: verdict.
// Legacy devices: the deterministic default component must be listed.
// Native v2 devices (ctx.native): every component is a candidate; their
// deployments are walked one by one by resolve_deployment.js.
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
    return [null, msg];
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

// /v2 is "unavailable" when the route is not mounted (Starlette's default
// 404 body), the flag rejects it (501/503), or the API answers 404 with an
// explicit disabled marker. Any other non-200 is a real failure.
function v2Unavailable(status, body) {
    if (status === 501 || status === 503) return true;
    if (status !== 404) return false;
    const detail = body && typeof body === "object" ? body.detail : body;
    if (detail === "Not Found") return true;
    return typeof detail === "string" && /v2|disabled/i.test(detail);
}

function items(body, key) {
    if (Array.isArray(body)) return body;
    if (body && Array.isArray(body.items)) return body.items;
    if (body && Array.isArray(body[key])) return body[key];
    return null;
}

const body = parseBody(msg.payload);
if (v2Unavailable(msg.statusCode, body)) {
    return verdict(null, "v2-unavailable");
}
if (ctx.native && msg.statusCode === 404) {
    return verdict(false, "v2-native-device-not-found");
}
if (msg.statusCode !== 200) {
    return verdict(false, "v2-components-error-" + String(msg.statusCode));
}
const components = items(body, "components");
if (components === null) {
    return verdict(false, "v2-components-bad-body");
}
const ids = components
    .map(function (c) { return c && (c.component_id || c.id); })
    .filter(function (id) { return typeof id === "string" && id.length > 0; });

if (ctx.native) {
    if (ids.length === 0) {
        return verdict(false, "v2-native-no-components");
    }
    ctx.component_id = ids[0];
    ctx.pending_component_ids = ids.slice(1);
    ctx.native_matches = [];
} else if (ids.indexOf(ctx.component_id) === -1) {
    return verdict(false, "default-component-missing");
}

msg.method = "GET";
msg.url = ctx.base + "/v2/tenants/" + encodeURIComponent(ctx.tenant) +
    "/deployments?component_id=" + encodeURIComponent(ctx.component_id);
msg.headers = {
    "Accept": "application/json",
    "X-Dev-User-Sub": String(env.get("LIMNOPULSE_DEV_USER_SUB") || "local-user-001"),
    "X-Dev-User-Email": String(env.get("LIMNOPULSE_DEV_USER_EMAIL") || "local@example.test"),
};
msg.payload = undefined;
return [msg, null];
