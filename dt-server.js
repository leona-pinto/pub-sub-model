const express = require('express');
const path    = require('path');
const http    = require('http');
const { Server } = require('socket.io');
const mqtt   = require('mqtt');
const axios  = require('axios');

// ── Express + Socket.io ───────────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

const PORT = 4000;
app.use(express.json());

// UI
app.use('/ui', express.static(path.join(__dirname, 'dt-ui')));
app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'dt-ui', 'index.html'));
});

// Command endpoint — called by the UI buttons
app.post('/api/command', async (req, res) => {
  const { device, command } = req.body;
  try {
    sendCommand(device, command);
    res.json({ success: true });
  } catch (err) {
    console.error('[API] Command failed:', err.message);
    res.status(500).json({ success: false });
  }
});

// ── MQTT topics ───────────────────────────────────────────────────────────────

const MQTT_BROKER = 'mqtt://localhost:1883';

// DT listens to these (PT → DT state updates)
const TOPIC_HVAC_STATE = 'home/hvac/state';
const TOPIC_TV_STATE   = 'home/tv/state';
const TOPIC_BBQ_STATE  = 'home/bbq/state';

// DT publishes to these (DT → PT commands)
const TOPIC_HVAC_COMMAND = 'home/hvac/command';
const TOPIC_TV_COMMAND   = 'home/tv/command';
const TOPIC_BBQ_COMMAND  = 'home/bbq/command';

// ── Digital Twin state ────────────────────────────────────────────────────────

const latestState = { tv: null, hvac: null, bbq: null };
const history     = { tv: [],   hvac: [],   bbq: []   };

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
  await axios.post(
    'http://localhost:7200/repositories/smart-home/statements',
    rdf,
    { headers: { 'Content-Type': 'application/sparql-update' } }
  );
}

async function storeCommandTriple(device, command, timestamp) {
  const rdf = `
INSERT DATA {
  GRAPH <http://example.org/dt> {
    <http://example.org/${device}/command/${timestamp}>
      <http://example.org/hasCommand>   "${command}" ;
      <http://example.org/hasTimestamp> "${timestamp}" .
  }
}`;
  await axios.post(
    'http://localhost:7200/repositories/smart-home/statements',
    rdf,
    { headers: { 'Content-Type': 'application/sparql-update' } }
  );
}

// ── Send command (DT → PT) ────────────────────────────────────────────────────

function sendCommand(device, command) {
  const topicMap = {
    hvac: TOPIC_HVAC_COMMAND,
    tv:   TOPIC_TV_COMMAND,
    bbq:  TOPIC_BBQ_COMMAND
  };

  const topic = topicMap[device];
  if (!topic) {
    console.warn(`[DT] Unknown device: ${device}`);
    return;
  }

  const message = {
    "@context": { "saref": "https://saref.etsi.org/core/" },
    "@type": "saref:Command",
    "saref:actsUpon": device,
    "saref:hasCommandKind": command   // home-app.js reads this field
  };

  mqttClient.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
    if (err) {
      console.error(`[DT] Command publish failed:`, err.message);
      return;
    }

    const timestamp = new Date().toISOString();
    console.log(`[DT COMMAND] ${device.toUpperCase()} <- ${command} @ ${timestamp}`);

    storeCommandTriple(device, command, timestamp).catch(err =>
      console.error('[GraphDB] Command store failed:', err.message)
    );
  });
}

// ── Extract state from SAREF state message ────────────────────────────────────

function extractState(payload) {
  return payload?.["saref:hasState"]?.["@value"] || null;
}

// ── MQTT client ───────────────────────────────────────────────────────────────

const mqttClient = mqtt.connect(MQTT_BROKER);

mqttClient.on('connect', () => {
  console.log('[DT] Connected to MQTT broker');

  // Subscribe to state topics (home-app publishes here when device state changes)
  mqttClient.subscribe(TOPIC_HVAC_STATE, { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_STATE,   { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_STATE,  { qos: 1 });
  console.log('[DT] Subscribed to state topics');
});

mqttClient.on('error', (err) => {
  console.error('[DT] MQTT error:', err.message);
});

mqttClient.on('message', (topic, payload) => {
  try {
    const msg       = JSON.parse(payload.toString());
    const timestamp = new Date().toISOString();

    // Map topic → device name
    let device = null;
    if (topic === TOPIC_TV_STATE)   device = 'tv';
    if (topic === TOPIC_HVAC_STATE) device = 'hvac';
    if (topic === TOPIC_BBQ_STATE)  device = 'bbq';
    if (!device) return;

    const state = extractState(msg);
    if (!state) {
      console.warn(`[DT] No state found in ${topic}:`, msg);
      return;
    }

    // Update twin state
    latestState[device] = { state, timestamp };
    history[device].push({ state, timestamp });
    console.log(`[DT] ${device.toUpperCase()} state updated: ${state}`);

    // Store in GraphDB
    storeStateTriple(device, state, timestamp).catch(err =>
      console.error('[GraphDB] State store failed:', err.message)
    );

    // Push to UI
    io.emit('device-state-update', { latestState, history });

  } catch (err) {
    console.error('[DT] Error processing message:', err.message);
  }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log('[DT] UI client connected');
  // Send current state immediately to newly connected client
  socket.emit('device-state-update', { latestState, history });
});

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`[DT] UI running on http://localhost:${PORT}/ui`);
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────

process.on('SIGINT', () => {
  console.log('\n[DT] Shutting down...');
  mqttClient.end();
  server.close();
  process.exit(0);
});