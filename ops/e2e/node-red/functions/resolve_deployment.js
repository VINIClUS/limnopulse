// Output 1: GET /v2 asset request. Output 2: verdict.
// Output 3: GET /v2 deployments for the next component (native devices only).
// Resolves the deployment whose half-open interval [started_at, ended_at)
// contains the reading ts (ended_at null = still open).
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
    return [null, msg, null];
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

function headers() {
    return {
        "Accept": "application/json",
        "X-Dev-User-Sub": String(env.get("LIMNOPULSE_DEV_USER_SUB") || "local-user-001"),
        "X-Dev-User-Email": String(env.get("LIMNOPULSE_DEV_USER_EMAIL") || "local@example.test"),
    };
}

const prefix = ctx.native ? "v2-native-" : "";
const body = parseBody(msg.payload);
if (v2Unavailable(msg.statusCode, body)) {
    return verdict(null, "v2-unavailable");
}
if (msg.statusCode !== 200) {
    return verdict(false, "v2-deployments-error-" + String(msg.statusCode));
}
const deployments = items(body, "deployments");
if (deployments === null) {
    return verdict(false, "v2-deployments-bad-body");
}

const tsMs = Date.parse(ctx.ts);
const matches = deployments.filter(function (d) {
    if (!d || d.component_id !== ctx.component_id) return false;
    const start = Date.parse(d.started_at);
    if (!Number.isFinite(start)) return false;
    const end = d.ended_at === null || d.ended_at === undefined ? Infinity : Date.parse(d.ended_at);
    return start <= tsMs && tsMs < end;
});

if (matches.length > 1) {
    return verdict(false, prefix + "overlapping-deployments");
}

let match = matches[0];
if (ctx.native) {
    if (match) {
        ctx.native_matches.push({
            component_id: ctx.component_id,
            deployment_id: match.deployment_id || null,
            asset_id: match.asset_id || null,
            ended: match.ended_at !== null && match.ended_at !== undefined,
        });
    }
    if (ctx.pending_component_ids.length > 0) {
        ctx.component_id = ctx.pending_component_ids.shift();
        msg.method = "GET";
        msg.url = ctx.base + "/v2/tenants/" + encodeURIComponent(ctx.tenant) +
            "/deployments?component_id=" + encodeURIComponent(ctx.component_id);
        msg.headers = headers();
        msg.payload = undefined;
        return [null, null, msg];
    }
    const assets = ctx.native_matches
        .map(function (m) { return m.asset_id; })
        .filter(function (a, i, all) { return all.indexOf(a) === i; });
    if (assets.length === 0) {
        return verdict(false, "v2-native-no-deployment-for-ts");
    }
    if (assets.length > 1) {
        return verdict(false, "v2-native-ambiguous-assets");
    }
    match = ctx.native_matches[0];
    ctx.component_id = match.component_id;
} else if (!match) {
    return verdict(false, "no-deployment-for-ts");
}

ctx.resolved_asset_id = match.asset_id || null;
ctx.deployment_id = match.deployment_id || null;
ctx.deployment_ended = ctx.native ? match.ended :
    match.ended_at !== null && match.ended_at !== undefined;
if (!ctx.resolved_asset_id) {
    return verdict(false, prefix + "deployment-without-asset");
}

msg.method = "GET";
msg.url = ctx.base + "/v2/tenants/" + encodeURIComponent(ctx.tenant) + "/assets/" +
    encodeURIComponent(ctx.resolved_asset_id);
msg.headers = headers();
msg.payload = undefined;
return [msg, null, null];
