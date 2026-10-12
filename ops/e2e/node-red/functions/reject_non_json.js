// Single output: verdict for a reading whose payload is not a JSON object.
const parts = String(msg.topic || "").split("/");
msg.payload = {
    device_id: parts.length === 5 ? parts[3] : null,
    seq: null,
    ts: null,
    v1_pond_id: null,
    resolved_asset_id: null,
    expected_asset_id: null,
    ok: false,
    reason: "payload-not-json-object",
};
return msg;
