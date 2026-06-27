/**
 * mock-gps-publisher.js
 *
 * Simulates GPS + acceleration sensor messages over MQTT.
 * Run in a separate terminal: node mock-gps-publisher.js
 */

const mqtt = require('mqtt');

const MQTT_BROKER    = 'mqtt://localhost:1883';
const TOPIC_GPS      = 'sensor/gps';
const TOPIC_ACC      = 'sensor/acceleration';

const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

const client = mqtt.connect(MQTT_BROKER);

// ── Message builders ──────────────────────────────────────────────────────────

function buildGpsMessage(lat, lon, temperature) {
  return {
    "@context": {
      "saref":   "https://saref.etsi.org/core/",
      "geo":     "http://www.w3.org/2003/01/geo/wgs84_pos#",
      "xsd":     "http://www.w3.org/2001/XMLSchema#",
      "dcterms": "http://purl.org/dc/terms/"
    },
    "@type": "saref:Measurement",
    "saref:hasMeasurement": [
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": {
          "@type":    "saref:Temperature",
          "geo:lat":  lat,
          "geo:long": lon
        },
        "saref:hasValue": { "@type": "xsd:float", "@value": temperature }
      },
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": {
          "@type":    "geo:Point",
          "geo:lat":  lat,
          "geo:long": lon
        }
      }
    ]
  };
}

function buildAccelerationMessage(x, y, z) {
  const timestamp = new Date().toISOString();
  return {
    "@context": {
      "saref":   "https://saref.etsi.org/core/",
      "ex":      "http://example.org/",
      "xsd":     "http://www.w3.org/2001/XMLSchema#",
      "dcterms": "http://purl.org/dc/terms/"
    },
    "@id":   `urn:message:acceleration:${timestamp}`,
    "@type": "ex:Message",
    "dcterms:issued": timestamp,
    "saref:hasMeasurement": [
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": { "@type": "ex:AccelerationX" },
        "saref:hasValue": { "@type": "xsd:float", "@value": x }
      },
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": { "@type": "ex:AccelerationY" },
        "saref:hasValue": { "@type": "xsd:float", "@value": y }
      },
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": { "@type": "ex:AccelerationZ" },
        "saref:hasValue": { "@type": "xsd:float", "@value": z }
      }
    ]
  };
}
function buildHumidityMessage(humidity) {
  const timestamp = new Date().toISOString();
  return {
    "@context": {
      "saref":   "https://saref.etsi.org/core/",
      "xsd":     "http://www.w3.org/2001/XMLSchema#",
      "dcterms": "http://purl.org/dc/terms/"
    },
    "@id":   `urn:message:humidity:${timestamp}`,
    "@type": "ex:Message",
    "dcterms:issued": timestamp,
    "saref:hasMeasurement": [
      {
        "@type": "saref:Measurement",
        "saref:relatesToProperty": { "@type": "saref:Humidity" },
        "saref:hasValue": { "@type": "xsd:float", "@value": humidity }
      }
    ]
  };
}
// ── Publish helpers ───────────────────────────────────────────────────────────

const TOPIC_HUMID = 'sensor/humidity';

function publishHumidity(humidity, label) {
  return new Promise((resolve, reject) => {
    const msg = buildHumidityMessage(humidity);
    client.publish(TOPIC_HUMID, JSON.stringify(msg), { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log(`[HUM]  ${label} | humidity: ${humidity}%`);
      resolve();
    });
  });
}

function publishGps(lat, lon, temperature, label) {
  return new Promise((resolve, reject) => {
    const msg = buildGpsMessage(lat, lon, temperature);
    client.publish(TOPIC_GPS, JSON.stringify(msg), { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log(`[GPS]  ${label} | lat: ${lat.toFixed(4)} lon: ${lon.toFixed(4)} | temp: ${temperature}°C`);
      resolve();
    });
  });
}

function publishAcceleration(x, y, z, label) {
  return new Promise((resolve, reject) => {
    const magnitude = Math.sqrt(x ** 2 + y ** 2 + z ** 2);
    const msg = buildAccelerationMessage(x, y, z);
    client.publish(TOPIC_ACC, JSON.stringify(msg), { qos: 1 }, (err) => {
      if (err) return reject(err);
      console.log(`[ACC]  ${label} | x:${x} y:${y} z:${z} | magnitude: ${magnitude.toFixed(2)} m/s²`);
      resolve();
    });
  });
}

function interpolate(lat1, lon1, lat2, lon2, steps) {
  const points = [];
  for (let i = 0; i <= steps; i++) {
    const t = i / steps;
    points.push({ lat: lat1 + (lat2 - lat1) * t, lon: lon1 + (lon2 - lon1) * t });
  }
  return points;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Scenario ──────────────────────────────────────────────────────────────────

async function runScenario() {
  console.log('[MOCK] Starting scenario...\n');

  const START_LAT   = HOUSE_LAT + 0.09;
  const START_LON   = HOUSE_LON;
  const temperature = 20; // change to >25 to test COOLING mode

  // Phase 1: Far away — all devices OFF
  console.log('--- Phase 1: Far from home (>3 km) ---');
  for (const p of interpolate(START_LAT, START_LON, HOUSE_LAT + 0.04, HOUSE_LON, 3)) {
    await publishGps(p.lat, p.lon, temperature, 'FAR');
    await sleep(2000);
  }

  // Phase 2: HVAC zone
  console.log('\n--- Phase 2: Entering HVAC zone (<3 km) ---');
  for (const p of interpolate(HOUSE_LAT + 0.04, HOUSE_LON, HOUSE_LAT + 0.02, HOUSE_LON, 4)) {
    await publishGps(p.lat, p.lon, temperature, 'HVAC ZONE');
    await sleep(2000);
  }

  // Phase 3: TV + BBQ zone
  console.log('\n--- Phase 3: Entering TV + BBQ zone (<2 km) ---');
  for (const p of interpolate(HOUSE_LAT + 0.02, HOUSE_LON, HOUSE_LAT + 0.005, HOUSE_LON, 4)) {
    await publishGps(p.lat, p.lon, temperature, 'TV/BBQ ZONE');
    await sleep(2000);
  }

  // Phase 4: At home
  console.log('\n--- Phase 4: Arrived home ---');
  for (let i = 0; i < 3; i++) {
    await publishGps(HOUSE_LAT, HOUSE_LON, temperature, 'HOME');
    await sleep(2000);
  }

  // Phase 5: Acceleration test — low then high then back to low
  console.log('\n--- Phase 5: Acceleration test ---');

  console.log('[ACC]  Sending low acceleration (below threshold)...');
  for (let i = 0; i < 3; i++) {
    await publishAcceleration(0.5, 0.3, 9.8, 'IDLE');   // ~9.8 m/s² = resting on table
    await sleep(1000);
  }

  console.log('[ACC]  Sending HIGH acceleration (above 15 m/s² threshold) — LED should go rainbow, TV ON...');
  for (let i = 0; i < 4; i++) {
    await publishAcceleration(8, 10, 9, 'SHAKE');        // magnitude ~15.6 m/s²
    await sleep(1000);
  }

  console.log('[ACC]  Sending low acceleration again — LED should turn off, TV OFF...');
  for (let i = 0; i < 3; i++) {
    await publishAcceleration(0.5, 0.3, 9.8, 'IDLE');
    await sleep(1000);
  }
// Phase 5b: Temperature test — trigger COOLING mode
console.log('\n--- Phase 5b: High temperature — HVAC should switch to COOLING ---');
for (let i = 0; i < 3; i++) {
  await publishGps(HOUSE_LAT, HOUSE_LON, 28, 'HOT');   // 28°C > 25°C threshold
  await sleep(2000);
}

console.log('[TEMP] Sending cool temperature — HVAC should switch back to HEATING...');
for (let i = 0; i < 3; i++) {
  await publishGps(HOUSE_LAT, HOUSE_LON, 20, 'COOL');  // back to 20°C
  await sleep(2000);
}

// Phase 5c: Humidity test — below threshold activates humidifier, above deactivates
console.log('\n--- Phase 5c: Humidity test ---');

console.log('[HUM]  Sending HIGH humidity (above 65%) — humidifier should be OFF...');
for (let i = 0; i < 3; i++) {
  await publishHumidity(80, 'HIGH');
  await sleep(1500);
}

console.log('[HUM]  Sending LOW humidity (below 65%) — humidifier should turn ON...');
for (let i = 0; i < 3; i++) {
  await publishHumidity(40, 'LOW');
  await sleep(1500);
}

console.log('[HUM]  Back to normal humidity — humidifier should turn OFF...');
for (let i = 0; i < 3; i++) {
  await publishHumidity(70, 'NORMAL');
  await sleep(1500);
}
  // Phase 6: Leaving home
  console.log('\n--- Phase 6: Leaving home ---');
  for (const p of interpolate(HOUSE_LAT, HOUSE_LON, START_LAT, START_LON, 5)) {
    await publishGps(p.lat, p.lon, temperature, 'LEAVING');
    await sleep(2000);
  }

  console.log('\n[MOCK] Scenario complete.');
  client.end();
}

client.on('connect', () => {
  console.log('[MOCK] Connected to MQTT broker\n');
  runScenario().catch(console.error);
});

client.on('error', (err) => {
  console.error('[MOCK] MQTT error:', err.message);
});