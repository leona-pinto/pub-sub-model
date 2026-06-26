/**
 * mock-gps-publisher.js
 *
 * Simulates a GPS tracker publishing SAREF messages over MQTT.
 * The car starts 10 km away and drives toward home (52.2180, 6.8900),
 * crossing the HVAC (3 km), TV (2 km), and BBQ (2 km) thresholds.
 *
 * Run in a separate terminal: node mock-gps-publisher.js
 */

const mqtt = require('mqtt');

const MQTT_BROKER = 'mqtt://localhost:1883';
const TOPIC_GPS   = 'trackers/HTIT_51/gps';

// House location
const HOUSE_LAT = 52.2180;
const HOUSE_LON  = 6.8900;

const client = mqtt.connect(MQTT_BROKER);

function buildSarefMessage(lat, lon, temperature) {
  return {
    "@context": {
      "saref": "https://saref.etsi.org/core/",
      "geo":   "http://www.w3.org/2003/01/geo/wgs84_pos#"
    },
    "@type": "saref:Measurement",
    "saref:hasMeasurement": [
      {
        "saref:hasProperty": {
          "@type": "saref:TemperatureProperty",
          "saref:hasValue": { "@value": temperature }
        }
      },
      {
        "saref:hasProperty": {
          "@type": "saref:LocationProperty",
          "geo:lat":  { "@value": lat },
          "geo:long": { "@value": lon }
        }
      }
    ]
  };
}

function interpolate(lat1, lon1, lat2, lon2, steps) {
  const points = [];
  for (let i = 0; i <= steps; i++) {
    const t = i / steps;
    points.push({ lat: lat1 + (lat2 - lat1) * t, lon: lon1 + (lon2 - lon1) * t });
  }
  return points;
}

function publish(lat, lon, temperature, label) {
  return new Promise((resolve, reject) => {
    const msg = buildSarefMessage(lat, lon, temperature);
    client.publish(TOPIC_GPS, JSON.stringify(msg), { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log(`[MOCK] ${label} | lat: ${lat.toFixed(4)}, lon: ${lon.toFixed(4)} | temp: ${temperature}C`);
      resolve();
    });
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runScenario() {
  console.log('[MOCK] Starting drive-home scenario...\n');

  const START_LAT = HOUSE_LAT + 0.09; // ~10 km north
  const START_LON = HOUSE_LON;
  const temperature = 20; // Change to >25 to test COOLING mode

  // Phase 1: Far away (>3 km) — all devices OFF
  console.log('--- Phase 1: Far from home (>3 km) ---');
  for (const p of interpolate(START_LAT, START_LON, HOUSE_LAT + 0.04, HOUSE_LON, 3)) {
    await publish(p.lat, p.lon, temperature, 'FAR');
    await sleep(2000);
  }

  // Phase 2: Entering HVAC zone (<3 km) — HVAC turns ON
  console.log('\n--- Phase 2: Entering HVAC zone (<3 km) ---');
  for (const p of interpolate(HOUSE_LAT + 0.04, HOUSE_LON, HOUSE_LAT + 0.02, HOUSE_LON, 4)) {
    await publish(p.lat, p.lon, temperature, 'HVAC ZONE');
    await sleep(2000);
  }

  // Phase 3: Entering TV + BBQ zone (<2 km) — TV and BBQ turn ON
  console.log('\n--- Phase 3: Entering TV + BBQ zone (<2 km) ---');
  for (const p of interpolate(HOUSE_LAT + 0.02, HOUSE_LON, HOUSE_LAT + 0.005, HOUSE_LON, 4)) {
    await publish(p.lat, p.lon, temperature, 'TV/BBQ ZONE');
    await sleep(2000);
  }

  // Phase 4: At home — all ON
  console.log('\n--- Phase 4: Arrived home ---');
  for (let i = 0; i < 3; i++) {
    await publish(HOUSE_LAT, HOUSE_LON, temperature, 'HOME');
    await sleep(2000);
  }

  // Phase 5: Leaving — devices turn OFF in reverse order
  console.log('\n--- Phase 5: Leaving home ---');
  for (const p of interpolate(HOUSE_LAT, HOUSE_LON, START_LAT, START_LON, 5)) {
    await publish(p.lat, p.lon, temperature, 'LEAVING');
    await sleep(2000);
  }

  console.log('\n[MOCK] Scenario complete.');
  client.end();
}

client.on('connect', () => {
  console.log('[MOCK] Connected to MQTT broker');
  runScenario().catch(console.error);
});

client.on('error', (err) => {
  console.error('[MOCK] MQTT error:', err.message);
});