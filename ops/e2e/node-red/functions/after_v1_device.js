// Output 1: GET /v2 device components request. Output 2: verdict.
// Legacy devices: /v1 pond -> deterministic default component + expected asset.
// Native v2 devices (canonical dev_<32hex>, /v1 404): ctx.native = true.
// Deterministic IDs mirror the Python helpers:
//   asset_id_for_legacy_pond(t, p) = "ast_" + uuid5(NAMESPACE_URL, "limnopulse/v4/legacy-pond/{t}/{p}").hex
//   default_component_id_for_legacy_device(t, d) =
//       "cmp_" + uuid5(NAMESPACE_URL, "limnopulse/v4/legacy-device-component/{t}/{d}").hex
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

// SHA-1 needs 32-bit wrap-around arithmetic: `>>> 0` reduces each sum modulo
// 2^32 (as an unsigned value). The bitwise operators then work on the same 32
// bits, so the digest is identical to the signed `| 0` form.
function sha1(bytes) {
    const ml = bytes.length;
    const withPad = ((ml + 9 + 63) >> 6) << 6;
    const buf = new Uint8Array(withPad);
    buf.set(bytes);
    buf[ml] = 0x80;
    const bitLen = ml * 8;
    buf[withPad - 4] = (bitLen >>> 24) & 0xff;
    buf[withPad - 3] = (bitLen >>> 16) & 0xff;
    buf[withPad - 2] = (bitLen >>> 8) & 0xff;
    buf[withPad - 1] = bitLen & 0xff;
    let h0 = 0x67452301, h1 = 0xefcdab89, h2 = 0x98badcfe, h3 = 0x10325476, h4 = 0xc3d2e1f0;
    const w = new Array(80);
    for (let off = 0; off < withPad; off += 64) {
        for (let i = 0; i < 16; i++) {
            const j = off + i * 4;
            w[i] = ((buf[j] << 24) | (buf[j + 1] << 16) | (buf[j + 2] << 8) | buf[j + 3]) >>> 0;
        }
        for (let i = 16; i < 80; i++) {
            const x = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
            w[i] = (x << 1) | (x >>> 31);
        }
        let a = h0, b = h1, c = h2, d = h3, e = h4;
        for (let i = 0; i < 80; i++) {
            let f, k;
            if (i < 20) { f = (b & c) | (~b & d); k = 0x5a827999; }
            else if (i < 40) { f = b ^ c ^ d; k = 0x6ed9eba1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8f1bbcdc; }
            else { f = b ^ c ^ d; k = 0xca62c1d6; }
            const t = (((a << 5) | (a >>> 27)) + f + e + k + w[i]) >>> 0;
            e = d; d = c; c = (b << 30) | (b >>> 2); b = a; a = t;
        }
        h0 = (h0 + a) >>> 0; h1 = (h1 + b) >>> 0; h2 = (h2 + c) >>> 0; h3 = (h3 + d) >>> 0; h4 = (h4 + e) >>> 0;
    }
    const out = new Uint8Array(20);
    [h0, h1, h2, h3, h4].forEach(function (h, i) {
        out[i * 4] = (h >>> 24) & 0xff;
        out[i * 4 + 1] = (h >>> 16) & 0xff;
        out[i * 4 + 2] = (h >>> 8) & 0xff;
        out[i * 4 + 3] = h & 0xff;
    });
    return out;
}

function uuid5Hex(name) {
    // RFC 4122 NAMESPACE_URL 6ba7b811-9dad-11d1-80b4-00c04fd430c8
    const ns = Buffer.from("6ba7b8119dad11d180b400c04fd430c8", "hex");
    const digest = sha1(Buffer.concat([ns, Buffer.from(name, "utf8")]));
    const b = Array.from(digest.slice(0, 16));
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    return b.map(function (x) { return (x < 16 ? "0" : "") + x.toString(16); }).join("");
}

// HTTP nodes return text (ret "txt") so a non-JSON body (e.g. a text/plain
// 500) never triggers Node-RED's JSON parse warning; parse it here instead.
// undefined = not JSON.
function parseBody(raw) {
    if (raw !== null && typeof raw === "object" && !Buffer.isBuffer(raw)) return raw;
    if (raw === undefined || raw === null) return undefined;
    try {
        return JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw));
    } catch (e) {
        return undefined;
    }
}

function v2Request(path) {
    msg.method = "GET";
    msg.url = ctx.base + "/v2/tenants/" + encodeURIComponent(ctx.tenant) + path;
    msg.headers = {
        "Accept": "application/json",
        "X-Dev-User-Sub": String(env.get("LIMNOPULSE_DEV_USER_SUB") || "local-user-001"),
        "X-Dev-User-Email": String(env.get("LIMNOPULSE_DEV_USER_EMAIL") || "local@example.test"),
    };
    msg.payload = undefined;
    return [msg, null];
}

// Canonical v2 device ids; native v2 devices have no /v1 row (404 after M1-05).
const CANONICAL_DEVICE_ID = /^dev_[0-9a-f]{32}$/;
const body = parseBody(msg.payload);

if (msg.statusCode === 404 && CANONICAL_DEVICE_ID.test(ctx.device_id)) {
    // Native v2 device: resolve through all of its components instead of the
    // legacy default component; there is no /v1 pond to cross-check.
    ctx.native = true;
    return v2Request("/devices/" + encodeURIComponent(ctx.device_id) + "/components");
}
if (msg.statusCode === 404) {
    return verdict(false, "v1-device-not-found");
}
if (msg.statusCode !== 200 || !body || typeof body !== "object" || Array.isArray(body)) {
    return verdict(false, "v1-error-" + String(msg.statusCode));
}
if (!body.pond_id) {
    return verdict(false, "v1-device-without-pond");
}

ctx.native = false;
ctx.v1_pond_id = body.pond_id;
ctx.expected_asset_id = "ast_" + uuid5Hex("limnopulse/v4/legacy-pond/" + ctx.tenant + "/" + ctx.v1_pond_id);
ctx.component_id = "cmp_" + uuid5Hex("limnopulse/v4/legacy-device-component/" + ctx.tenant + "/" + ctx.device_id);

return v2Request("/devices/" + encodeURIComponent(ctx.device_id) + "/components");
