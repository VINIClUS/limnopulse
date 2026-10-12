// LimnoPulse simulated water-quality probe (Wokwi ESP32 DevKit).
//
// Every LP_PUBLISH_INTERVAL_MS it builds the legacy reading payload, with
// exactly the keys of examples/telemetry/reading.local.json, publishes it to
//   limnopulse/v1/devices/<LP_DEVICE_ID>/readings
// and prints one capture line to Serial:
//   READING {"ts":...,"seq":...,...}
// ops/e2e/wokwi/replay.py turns those lines back into MQTT publishes.
//
// The device never sends tenant or pond identifiers; enrichment happens in
// Telegraf / the API. Override settings without editing this file:
//   * numeric values via arduino-cli, e.g.
//       --build-property 'build.defines=-DLP_MQTT_PORT=1884 -DLP_PUBLISH_INTERVAL_MS=2000UL'
//   * strings via an untracked lp_local_config.h next to this sketch, e.g.
//       #define LP_MQTT_HOST "broker.example"
//     (arduino-cli build properties cannot carry quoted string macros.)

#include <WiFi.h>
#include <time.h>
#include <math.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>

#if __has_include("lp_local_config.h")
#include "lp_local_config.h"
#endif

#ifndef LP_DEVICE_ID
#define LP_DEVICE_ID "local-device-001"
#endif
#ifndef LP_WIFI_SSID
#define LP_WIFI_SSID "Wokwi-GUEST"
#endif
#ifndef LP_WIFI_PASSWORD
#define LP_WIFI_PASSWORD ""
#endif
// host.wokwi.internal reaches the developer machine through the Wokwi
// private IoT gateway. Without it MQTT fails and the serial capture + replay
// path is used instead; READING lines are printed either way.
#ifndef LP_MQTT_HOST
#define LP_MQTT_HOST "host.wokwi.internal"
#endif
#ifndef LP_MQTT_PORT
#define LP_MQTT_PORT 1883
#endif
#ifndef LP_PUBLISH_INTERVAL_MS
#define LP_PUBLISH_INTERVAL_MS 5000UL
#endif
#ifndef LP_NTP_SERVER
#define LP_NTP_SERVER "pool.ntp.org"
#endif
// Used when NTP is unreachable: 2026-07-09T12:00:00Z + uptime.
#ifndef LP_FALLBACK_EPOCH
#define LP_FALLBACK_EPOCH 1783598400UL
#endif

#define LP_TOPIC_PREFIX "limnopulse/v1/devices/"
#define LP_TOPIC_SUFFIX "/readings"

static const int PIN_DS18B20 = 4;
static const int PIN_PH = 34;
static const int PIN_DO = 35;
static const int PIN_TURBIDITY = 32;
static const int PIN_SALINITY = 33;
static const int PIN_BATTERY = 36;  // VP

static const char *const TOPIC = LP_TOPIC_PREFIX LP_DEVICE_ID LP_TOPIC_SUFFIX;

OneWire oneWire(PIN_DS18B20);
DallasTemperature tempSensor(&oneWire);
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

static uint32_t seq = 0;
static unsigned long lastPublish = 0;
static bool ntpSynced = false;

static double round2(double value) {
  return round(value * 100.0) / 100.0;
}

static double scaledAdc(int pin, double lo, double hi) {
  const int raw = analogRead(pin);  // 0..4095 (12-bit)
  return lo + (hi - lo) * (raw / 4095.0);
}

static void isoTimestamp(char *out, size_t len) {
  time_t now = time(nullptr);
  if (!ntpSynced || now < 1700000000) {
    now = (time_t)(LP_FALLBACK_EPOCH + millis() / 1000UL);
  }
  struct tm utc;
  gmtime_r(&now, &utc);
  strftime(out, len, "%Y-%m-%dT%H:%M:%SZ", &utc);
}

static void connectWifi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(LP_WIFI_SSID, LP_WIFI_PASSWORD, 6);
  const unsigned long started = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - started < 15000UL) {
    delay(100);
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("WIFI connected ip=%s\n", WiFi.localIP().toString().c_str());
  } else {
    Serial.println("WIFI not connected; readings will be serial-only");
  }
}

static void syncClock() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }
  configTime(0, 0, LP_NTP_SERVER);
  const unsigned long started = millis();
  while (time(nullptr) < 1700000000 && millis() - started < 8000UL) {
    delay(200);
  }
  ntpSynced = time(nullptr) >= 1700000000;
  Serial.println(ntpSynced ? "NTP synced" : "NTP unavailable; using fallback epoch");
}

static void ensureMqtt() {
  if (mqtt.connected() || WiFi.status() != WL_CONNECTED) {
    return;
  }
  String clientId = String("limnopulse-probe-") + LP_DEVICE_ID;
  if (mqtt.connect(clientId.c_str())) {
    Serial.printf("MQTT connected %s:%d\n", LP_MQTT_HOST, LP_MQTT_PORT);
  } else {
    Serial.printf("MQTT connect %s:%d failed rc=%d\n", LP_MQTT_HOST, LP_MQTT_PORT, mqtt.state());
  }
}

static void publishReading() {
  tempSensor.requestTemperatures();
  double tempC = tempSensor.getTempCByIndex(0);
  if (tempC == DEVICE_DISCONNECTED_C) {
    tempC = NAN;
  }

  char ts[25];
  isoTimestamp(ts, sizeof(ts));
  seq += 1;

  // Key set and order mirror examples/telemetry/reading.local.json.
  JsonDocument doc;
  doc["ts"] = ts;
  doc["seq"] = seq;
  doc["temp_c"] = round2(tempC);
  doc["ph"] = round2(scaledAdc(PIN_PH, 0.0, 14.0));
  doc["do_mg_l"] = round2(scaledAdc(PIN_DO, 0.0, 15.0));
  doc["turbidity_ntu"] = round2(scaledAdc(PIN_TURBIDITY, 0.0, 100.0));
  doc["salinity_ppt"] = round2(scaledAdc(PIN_SALINITY, 0.0, 40.0));
  doc["battery_v"] = round2(scaledAdc(PIN_BATTERY, 3.0, 4.2));
  doc["rssi"] = WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0;

  char payload[256];
  const size_t len = serializeJson(doc, payload, sizeof(payload));

  Serial.print("READING ");
  Serial.println(payload);

  ensureMqtt();
  if (mqtt.connected()) {
    if (!mqtt.publish(TOPIC, (const uint8_t *)payload, len, false)) {
      Serial.println("MQTT publish failed");
    }
  }
}

void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.printf("LimnoPulse probe %s booting topic=%s\n", LP_DEVICE_ID, TOPIC);
  analogReadResolution(12);
  tempSensor.begin();
  mqtt.setServer(LP_MQTT_HOST, LP_MQTT_PORT);
  mqtt.setBufferSize(512);
  connectWifi();
  syncClock();
  lastPublish = millis() - LP_PUBLISH_INTERVAL_MS;
}

void loop() {
  mqtt.loop();
  if (millis() - lastPublish >= LP_PUBLISH_INTERVAL_MS) {
    lastPublish = millis();
    publishReading();
  }
  delay(10);
}
