// Minimal headless Node-RED settings for the LimnoPulse M1 e2e harness.
//
//   node-red --settings ops/e2e/node-red/settings.js \
//            --userDir "${NODE_RED_USER_DIR:-/tmp/limnopulse-e2e-node-red}"
//
// The flow file is loaded from this directory (ops/e2e/node-red/flows.json).
// The editor/admin API binds to loopback only and has no adminAuth: it is a
// local test harness, never an exposed service.
const os = require("os");
const path = require("path");

const defaults = {
    MQTT_HOST: "127.0.0.1",
    MQTT_PORT: "1883",
    LIMNOPULSE_API_URL: "http://127.0.0.1:8000",
    LIMNOPULSE_TENANT: "tnt_local_001",
};
for (const [key, value] of Object.entries(defaults)) {
    if (!process.env[key]) {
        process.env[key] = value;
    }
}

module.exports = {
    uiHost: "127.0.0.1",
    uiPort: Number(process.env.NODE_RED_PORT || 1880),
    userDir: process.env.NODE_RED_USER_DIR || path.join(os.tmpdir(), "limnopulse-e2e-node-red"),
    flowFile: path.join(__dirname, "flows.json"),
    flowFilePretty: true,
    // No credentials are stored by this flow; do not write a key file.
    credentialSecret: false,
    httpRequestTimeout: 10000,
    mqttReconnectTime: 2000,
    functionExternalModules: false,
    functionTimeout: 0,
    externalModules: {
        autoInstall: false,
        palette: { allowInstall: false },
        modules: { allowInstall: false },
    },
    diagnostics: { enabled: false, ui: false },
    telemetry: { enabled: false, updateNotification: false },
    runtimeState: { enabled: false, ui: false },
    logging: {
        console: { level: process.env.NODE_RED_LOG_LEVEL || "info", metrics: false, audit: false },
    },
    editorTheme: {
        projects: { enabled: false },
        tours: false,
    },
};
