const express = require('express');
const path    = require('path');
const http    = require('http');
const { Server } = require('socket.io');
const mqtt   = require('mqtt');
const axios  = require('axios');
const { config } = require('dotenv');

config();

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: '*' } });

const PORT = 4000;
app.use(express.json());

app.use('/ui', express.static(path.join(__dirname, 'dt-ui')));
app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'dt-ui', 'index.html'));
});

// ── MQTT topics ───────────────────────────────────────────────────────────────

const MQTT_BROKER = 'mqtt://localhost:1883';

const EXTERNAL_MQTT_BROKER   = process.env.MQTT_BROKER;
const EXTERNAL_MQTT_PORT     = process.env.MQTT_PORT || 1883;
const EXTERNAL_MQTT_USERNAME = process.env.MQTT_USERNAME;
const EXTERNAL_MQTT_PASSWORD = process.env.MQTT_PASSWORD;

const TOPIC_HVAC_STATE       = 'home/hvac/state';
const TOPIC_TV_STATE         = 'home/tv/state';
const TOPIC_BBQ_STATE        = 'home/bbq/state';
const TOPIC_HUMIDIFIER_STATE = 'home/humidifier/state';
const TOPIC_LED_SEMANTIC     = 'home/led/command';

const TOPIC_HVAC_COMMAND = 'home/hvac/command';
const TOPIC_TV_COMMAND   = 'home/tv/command';
const TOPIC_BBQ_COMMAND  = 'home/bbq/command';

const TOPIC_LED_HARDWARE = `trackers/${process.env.MQTT_USER || 'HTIT_51'}/leds/set`;

// ── Digital Twin state ────────────────────────────────────────────────────────

const latestState = { tv: null, hvac: null, bbq: null, led: null, humidifier: null };
const history     = { tv: [],   hvac: [],   bbq: [],   led: [],   humidifier: []   };

const sensorReadings = {
  gps:          { latitude: null, longitude: null, distance: null, timestamp: null },
  temperature:  { value: null, timestamp: null },
  acceleration: { x: null, y: null, z: null, magnitude: null, timestamp: null },
  humidity:     { value: null, timestamp: null }
};

// ── GraphDB ───────────────────────────────────────────────────────────────────

async function storeStateTriple(device, state, timestamp) {
  const rdf = `
INSERT DATA {
  GRAPH <http://example.org/dt> {
    <http://example.org/${device}>
      <http://example.org/hasState>     "${state}" ;
      <http://example.org/hasTimestamp> "${timestamp}" .
  }
}`;
  try {
    await axios.post('http://localhost:7200/repositories/smart-home/statements', rdf,
      { headers: { 'Content-Type': 'application/sparql-update' } });
  } catch (err) {
    console.error('[GraphDB] State store failed:', err.message);
  }
}

async function storeCommandTriple(device, command, timestamp) {
  const rdf = `
INSERT DATA {
  GRAPH <http://example.org/dt> {
    <http://example.org/command/${device}/${timestamp}>
      a <https://saref.etsi.org/core/Command> ;
      <https://saref.etsi.org/core/actsUpon>        <urn:device:home:${device}> ;
      <https://saref.etsi.org/core/hasCommandKind>  "${command}" ;
      <http://purl.org/dc/terms/issued>             "${timestamp}" .
  }
}`;
  try {
    await axios.post('http://localhost:7200/repositories/smart-home/statements', rdf,
      { headers: { 'Content-Type': 'application/sparql-update' } });
  } catch (err) {
    console.error('[GraphDB] Command store failed:', err.message);
  }
}

// ── MQTT clients ──────────────────────────────────────────────────────────────

const mqttClient = mqtt.connect(MQTT_BROKER);

let trackerClient = null;
if (EXTERNAL_MQTT_BROKER && EXTERNAL_MQTT_USERNAME && EXTERNAL_MQTT_PASSWORD) {
  trackerClient = mqtt.connect(`mqtt://${EXTERNAL_MQTT_BROKER}:${EXTERNAL_MQTT_PORT}`, {
    username: EXTERNAL_MQTT_USERNAME,
    password: EXTERNAL_MQTT_PASSWORD,
    clientId: `dt-server-${Date.now()}`
  });
  trackerClient.on('connect', () => console.log(`[DT] Connected to external broker`));
  trackerClient.on('error',   (err) => console.error('[DT] External broker error:', err.message));
}

// ── Send device command (DT → PT) ────────────────────────────────────────────

function sendCommand(device, command) {
  const topicMap = { hvac: TOPIC_HVAC_COMMAND, tv: TOPIC_TV_COMMAND, bbq: TOPIC_BBQ_COMMAND };
  const topic    = topicMap[device];
  if (!topic) { console.warn(`[DT] Unknown device: ${device}`); return; }

  const deviceMap = {
    hvac: { urn: 'urn:device:home:smart-hvac',     label: 'Smart HVAC',     propertyType: 'saref:OperatingMode' },
    tv:   { urn: 'urn:device:home:smart-tv',       label: 'Smart TV',       propertyType: 'saref:OnOffState' },
    bbq:  { urn: 'urn:device:home:smart-barbecue', label: 'Smart Barbecue', propertyType: 'saref:OnOffState' }
  };
  const deviceInfo = deviceMap[device] || { urn: `urn:device:home:${device}`, label: device, propertyType: 'saref:Property' };
  const timestamp  = new Date().toISOString();

  const message = {
    "@context": { "saref": "https://saref.etsi.org/core/", "dcterms": "http://purl.org/dc/terms/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#" },
    "@id":   `urn:command:dt:${device}:${Date.now()}`,
    "@type": "saref:Command",
    "dcterms:issued":          timestamp,
    "saref:actsUpon":          { "@id": deviceInfo.urn, "@type": "saref:Device", "rdfs:label": deviceInfo.label },
    "saref:relatesToProperty": { "@type": deviceInfo.propertyType },
    "saref:hasCommandKind":    command
  };

  mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
    if (err) { console.error(`[DT] Command publish failed:`, err.message); return; }
    console.log(`[DT COMMAND] ${device.toUpperCase()} <- ${command} @ ${timestamp}`);
    storeCommandTriple(device, command, timestamp);
  });
}

// ── Send LED command ──────────────────────────────────────────────────────────

function sendLedCommand(animation) {
  const timestamp = new Date().toISOString();

  // SAREF JSON-LD to local broker (semantic record + DT picks it up via subscription)
  const sarefCommand = {
    "@context": { "saref": "https://saref.etsi.org/core/", "dcterms": "http://purl.org/dc/terms/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#" },
    "@id":   `urn:command:dt:led:${Date.now()}`,
    "@type": "saref:Command",
    "dcterms:issued":       timestamp,
    "saref:hasCommandKind": animation,
    "saref:actsUpon":       { "@id": "urn:device:home:smart-led", "@type": "saref:Device", "rdfs:label": "Smart LED Strip" }
  };

  mqttClient.publish(TOPIC_LED_SEMANTIC, JSON.stringify(sarefCommand), { qos: 1 }, (err) => {
    if (err) console.error('[DT] LED SAREF publish failed:', err.message);
    else     console.log(`[DT] LED SAREF command: ${animation}`);
  });

  // Plain JSON to external broker (hardware format the LED module understands)
  if (trackerClient) {
    trackerClient.publish(TOPIC_LED_HARDWARE, JSON.stringify({ animation }), { qos: 1 }, (err) => {
      if (err) console.error('[DT] LED hardware publish failed:', err.message);
      else     console.log(`[DT] LED hardware command sent: ${animation}`);
    });
  }

  storeCommandTriple('led', animation, timestamp);
}

// ── Extract state value from SAREF state message ──────────────────────────────

function extractState(payload) {
  // Handles saref:hasValue (current format from publishDeviceState)
  // and saref:hasState (legacy format) 
  return payload?.['saref:hasValue']?.['@value']
      || payload?.['saref:hasState']?.['@value']
      || null;
}

// ── MQTT ──────────────────────────────────────────────────────────────────────

mqttClient.on('connect', () => {
  console.log('[DT] Connected to local MQTT broker');

  mqttClient.subscribe(TOPIC_HVAC_STATE,       { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_STATE,         { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_STATE,        { qos: 1 });
  mqttClient.subscribe(TOPIC_HUMIDIFIER_STATE, { qos: 1 });
  mqttClient.subscribe(TOPIC_LED_SEMANTIC,     { qos: 1 });

  // Sensor reading topics — DT subscribes to show live readings on dashboard
  mqttClient.subscribe('sensor/gps',          { qos: 1 });
  mqttClient.subscribe('sensor/acceleration', { qos: 1 });
  mqttClient.subscribe('sensor/humidity',     { qos: 1 });
  console.log('[DT] Subscribed to state + sensor + LED topics');
});

mqttClient.on('error', (err) => console.error('[DT] MQTT error:', err.message));

mqttClient.on('message', (topic, payload) => {
  try {
    const msg       = JSON.parse(payload.toString());
    const timestamp = new Date().toISOString();

    // ── Device state updates (home-app → DT) ─────────────────────────────────
    const stateTopicMap = {
      [TOPIC_TV_STATE]:         'tv',
      [TOPIC_HVAC_STATE]:       'hvac',
      [TOPIC_BBQ_STATE]:        'bbq',
      [TOPIC_HUMIDIFIER_STATE]: 'humidifier'
    };

    if (stateTopicMap[topic]) {
      const device = stateTopicMap[topic];
      const state  = extractState(msg);
      if (!state) { console.warn(`[DT] No state in ${topic}`); return; }

      // Only push to history when state actually changes (avoids log spam
      // since home-app now publishes on every GPS message)
      const prev = latestState[device]?.state;
      latestState[device] = { state, timestamp };

      if (prev !== state) {
        history[device].push({ state, timestamp });
        console.log(`[DT] ${device.toUpperCase()} state changed: ${prev || 'none'} → ${state}`);
        storeStateTriple(device, state, timestamp);
      }

      // Always push to UI so dashboard is always current
      io.emit('device-state-update', { latestState, history });
      return;
    }

    // ── LED tracking ──────────────────────────────────────────────────────────
    if (topic === TOPIC_LED_SEMANTIC) {
      const animation = msg?.['saref:hasCommandKind'] || null;
      if (!animation) return;

      const prev = latestState.led?.state;
      latestState.led = { state: animation, timestamp };

      if (prev !== animation) {
        history.led.push({ state: animation, timestamp });
        console.log(`[DT] LED changed: ${prev || 'none'} → ${animation}`);
        storeStateTriple('led', animation, timestamp);
      }

      io.emit('device-state-update', { latestState, history });
      return;
    }

    // ── Live sensor readings (for dashboard display only) ─────────────────────
    if (topic === 'sensor/gps') {
      const measurements = msg['saref:hasMeasurement'] || [];
      for (const m of measurements) {
        const types    = [].concat(m['saref:relatesToProperty']?.['@type'] || []);
        const value    = m['saref:hasValue']?.['@value'];
        const property = m['saref:relatesToProperty'];

        if (types.includes('geo:Point') || types.includes('saref:Location') || types.includes('saref:LocationProperty')) {
          sensorReadings.gps.latitude  = property?.['geo:lat'];
          sensorReadings.gps.longitude = property?.['geo:long'];
          sensorReadings.gps.timestamp = timestamp;
        }
        if (types.includes('saref:Temperature') || types.includes('saref:TemperatureProperty')) {
          sensorReadings.temperature.value     = value;
          sensorReadings.temperature.timestamp = timestamp;
        }
      }
      // Calculate distance from home (same formula as home-app)
      if (sensorReadings.gps.latitude != null && sensorReadings.gps.longitude != null) {
        const R    = 6371;
        const lat1 = 52.2176 * Math.PI / 180;
        const lat2 = Number(sensorReadings.gps.latitude)  * Math.PI / 180;
        const dLat = lat2 - lat1;
        const dLon = (Number(sensorReadings.gps.longitude) - 6.8904) * Math.PI / 180;
        const a    = Math.sin(dLat/2)**2 + Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLon/2)**2;
        sensorReadings.gps.distance = R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
      }
      io.emit('sensor-update', sensorReadings);
      return;
    }

    if (topic === 'sensor/acceleration') {
      const measurements = msg['saref:hasMeasurement'] || [];
      for (const m of measurements) {
        const types = [].concat(m['saref:relatesToProperty']?.['@type'] || []);
        const value = parseFloat(m['saref:hasValue']?.['@value']) || 0;
        if (types.includes('ex:AccelerationX')) sensorReadings.acceleration.x = value;
        if (types.includes('ex:AccelerationY')) sensorReadings.acceleration.y = value;
        if (types.includes('ex:AccelerationZ')) sensorReadings.acceleration.z = value;
      }
      const { x, y, z } = sensorReadings.acceleration;
      sensorReadings.acceleration.magnitude = Math.sqrt(x**2 + y**2 + z**2);
      sensorReadings.acceleration.timestamp = timestamp;
      io.emit('sensor-update', sensorReadings);
      return;
    }

    if (topic === 'sensor/humidity') {
      const measurements = msg['saref:hasMeasurement'] || [];
      for (const m of measurements) {
        const types = [].concat(m['saref:relatesToProperty']?.['@type'] || []);
        if (types.includes('saref:Humidity')) {
          sensorReadings.humidity.value     = m['saref:hasValue']?.['@value'];
          sensorReadings.humidity.timestamp = timestamp;
        }
      }
      io.emit('sensor-update', sensorReadings);
      return;
    }

  } catch (err) {
    console.error('[DT] Error processing message:', err.message);
  }
});

// ── REST API ──────────────────────────────────────────────────────────────────

app.post('/api/command', (req, res) => {
  const { device, command } = req.body;
  try {
    sendCommand(device, command);
    res.json({ success: true });
  } catch (err) {
    console.error('[API] Command failed:', err.message);
    res.status(500).json({ success: false });
  }
});

app.post('/api/led-command', (req, res) => {
  const { animation } = req.body;
  if (!animation) return res.status(400).json({ success: false, error: 'animation required' });
  try {
    sendLedCommand(animation);
    res.json({ success: true });
  } catch (err) {
    console.error('[API] LED command failed:', err.message);
    res.status(500).json({ success: false });
  }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log('[DT] UI client connected');
  socket.emit('device-state-update', { latestState, history });
  socket.emit('sensor-update', sensorReadings);
});

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`[DT] UI running on http://localhost:${PORT}/ui`);
});

process.on('SIGINT', () => {
  console.log('\n[DT] Shutting down...');
  mqttClient.end();
  if (trackerClient) trackerClient.end();
  server.close();
  process.exit(0);
});