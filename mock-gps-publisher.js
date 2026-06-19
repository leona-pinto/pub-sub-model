/**
 * mock-gps-publisher.js
 *
 * Simulates a GPS tracker publishing SAREF messages to Kafka.
 * The car starts 10 km away and drives toward home (52.2180, 6.8900),
 * crossing the HVAC (3 km), TV (2 km), and BBQ (2 km) thresholds.
 *
 * Run in a separate terminal: node mock-gps-publisher.js
 * Comment out the scenario steps you don't need.
 */

const { Kafka } = require('kafkajs');

const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'trackers.HTIT_51.gps';

// House location
const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

const kafka = new Kafka({
  clientId: 'mock-gps-publisher',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

// Build a SAREF-formatted GPS + temperature message
function buildSarefMessage(lat, lon, temperature) {
  return {
    "@context": {
      "saref": "https://saref.etsi.org/core/",
      "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#"
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

// Interpolate between two lat/lon points in `steps` steps
function interpolate(lat1, lon1, lat2, lon2, steps) {
  const points = [];
  for (let i = 0; i <= steps; i++) {
    const t = i / steps;
    points.push({
      lat: lat1 + (lat2 - lat1) * t,
      lon: lon1 + (lon2 - lon1) * t
    });
  }
  return points;
}

// Publish one message and log it
async function publish(lat, lon, temperature, label) {
  const msg = buildSarefMessage(lat, lon, temperature);
  await producer.send({
    topic: KAFKA_TOPIC,
    messages: [{ value: JSON.stringify(msg) }]
  });
  console.log(`[MOCK] ${label} | lat: ${lat.toFixed(4)}, lon: ${lon.toFixed(4)} | temp: ${temperature}°C`);
}

// Sleep helper
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function runScenario() {
  await producer.connect();
  console.log('[MOCK] Producer connected');
  console.log('[MOCK] Starting drive-home scenario...\n');

  // ── Starting point: ~10 km away from home ──────────────────────────────────
  // Offset of ~0.09 degrees latitude ≈ 10 km
  const START_LAT = HOUSE_LAT + 0.09;
  const START_LON = HOUSE_LON;

  const temperature = 20; // °C — change to >25 to test COOLING mode

  // ── Phase 1: Far away (outside all thresholds) — 3 messages ───────────────
  console.log('--- Phase 1: Far from home (>3 km) ---');
  const farPoints = interpolate(START_LAT, START_LON, HOUSE_LAT + 0.04, HOUSE_LON, 3);
  for (const p of farPoints) {
    await publish(p.lat, p.lon, temperature, 'FAR');
    await sleep(2000);
  }

  // ── Phase 2: Approaching — crosses HVAC threshold at ~3 km ────────────────
  console.log('\n--- Phase 2: Entering HVAC zone (<3 km) ---');
  // 0.027 degrees lat ≈ 3 km
  const hvacPoints = interpolate(HOUSE_LAT + 0.04, HOUSE_LON, HOUSE_LAT + 0.02, HOUSE_LON, 4);
  for (const p of hvacPoints) {
    await publish(p.lat, p.lon, temperature, 'HVAC ZONE');
    await sleep(2000);
  }

  // ── Phase 3: Closer — crosses TV/BBQ threshold at ~2 km ──────────────────
  console.log('\n--- Phase 3: Entering TV + BBQ zone (<2 km) ---');
  // 0.018 degrees lat ≈ 2 km
  const closePoints = interpolate(HOUSE_LAT + 0.02, HOUSE_LON, HOUSE_LAT + 0.005, HOUSE_LON, 4);
  for (const p of closePoints) {
    await publish(p.lat, p.lon, temperature, 'TV/BBQ ZONE');
    await sleep(2000);
  }

  // ── Phase 4: Arrived home ─────────────────────────────────────────────────
  console.log('\n--- Phase 4: Arrived home ---');
  for (let i = 0; i < 3; i++) {
    await publish(HOUSE_LAT, HOUSE_LON, temperature, 'HOME');
    await sleep(2000);
  }

  // ── Phase 5: Leaving home again ───────────────────────────────────────────
  console.log('\n--- Phase 5: Leaving home (devices should turn OFF) ---');
  const leavePoints = interpolate(HOUSE_LAT, HOUSE_LON, START_LAT, START_LON, 5);
  for (const p of leavePoints) {
    await publish(p.lat, p.lon, temperature, 'LEAVING');
    await sleep(2000);
  }

  console.log('\n[MOCK] Scenario complete.');
  await producer.disconnect();
}

runScenario().catch(console.error);