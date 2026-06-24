const express = require('express');
const mqtt = require('mqtt');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const axios = require('axios');
const N3 = require('n3');
const { DataFactory, Writer } = N3;
const namedNode = DataFactory.namedNode;
const literal = DataFactory.literal;

const graphQueue = [];
let graphProcessing = false;

const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: "*" } });

// Configuration 

const MQTT_BROKER = 'mqtt://localhost:1883';

// Topics — PT → DT (incoming sensor data)
const TOPIC_GPS = 'trackers/HTIT_51/gps';

// Topics — DT → PT (outgoing commands)
const TOPIC_HVAC_COMMAND = 'home/hvac/command';
const TOPIC_TV_COMMAND   = 'home/tv/command';
const TOPIC_BBQ_COMMAND  = 'home/bbq/command';

// Topics — PT → DT (device state confirmations)
const TOPIC_HVAC_STATE = 'home/hvac/state';
const TOPIC_TV_STATE   = 'home/tv/state';
const TOPIC_BBQ_STATE  = 'home/bbq/state';

// GraphDB
const GRAPHDB_ENDPOINT = 'http://localhost:7200/repositories/smart-home/statements';

const RDF_PREFIX = 'http://example.org/';
const SAREF = "https://saref.etsi.org/core/";
const GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const DCT = "http://purl.org/dc/terms/";

const GRAPH_HVAC = "http://example.org/graph/hvac";
const GRAPH_TV   = "http://example.org/graph/tv";
const GRAPH_BBQ  = "http://example.org/graph/barbecue";

// House location
const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

// Device thresholds
const HVAC_THRESHOLD_KM     = 3;
const TV_THRESHOLD_KM       = 2;
const BARBECUE_THRESHOLD_KM = 2;

//  Global Device State 

let deviceState = {
  carDistance:  null,
  carLatitude:  null,
  carLongitude: null,
  currentTemp:  0,
  lastUpdate:   null,
  messageCount: 0,
  devices: {
    hvac:     { isPowerOn: false, mode: 'OFF' },
    tv:       { isPowerOn: false },
    barbecue: { isPowerOn: true }
  }
};

// Express 

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHome.html'));
});

//  Helpers 

function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/* 
function extractDataFromSarefMessage(sarefMsg) {
  let temperature = null;
  let latitude    = null;
  let longitude   = null;

  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const measurement of measurements) {
    const propType = measurement['saref:hasProperty']?.['@type'];

    if (propType === 'saref:TemperatureProperty') {
      temperature = measurement['saref:hasProperty']['saref:hasValue']['@value'];
    }
    if (propType === 'saref:LocationProperty') {
      latitude  = measurement['saref:hasProperty']['geo:lat']?.['@value'];
      longitude = measurement['saref:hasProperty']['geo:long']?.['@value'];
    }
  }

  let distance = null;
  if (latitude !== null && longitude !== null) {
    distance = calculateDistance(Number(latitude), Number(longitude), HOUSE_LAT, HOUSE_LON);
  }

  return { distance, temperature, latitude, longitude };
}
*/ 

function updateLocationData(distance, temperature, latitude, longitude) {
  deviceState.carDistance  = distance;
  deviceState.currentTemp  = temperature;
  deviceState.carLatitude  = latitude;
  deviceState.carLongitude = longitude;
  deviceState.lastUpdate   = new Date().toISOString();
  deviceState.messageCount++;
}
  
function extractDataFromSarefMessage(sarefMsg) {
  let temperature = null;
  let latitude = null;
  let longitude = null;

  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const m of measurements) {
    const value = m['saref:hasValue']?.['@value'];
    const property = m['saref:relatesToProperty'];
    const types = [].concat(property?.['@type'] || []);

    // -------------------
    // TEMPERATURE
    // -------------------
    if (
      types.includes('saref:Temperature') ||
      types.includes('saref:TemperatureProperty')
    ) {
      temperature = value;
    }

    // -------------------
    // LOCATION
    // -------------------
    if (
      types.includes('geo:Point') ||
      types.includes('saref:Location') ||
      types.includes('saref:LocationProperty')
    ) {
      latitude = property?.['geo:lat'];
      longitude = property?.['geo:long'];
    }
  }

  let distance = null;

  if (latitude != null && longitude != null) {
    distance = calculateDistance(
      Number(latitude),
      Number(longitude),
      HOUSE_LAT,
      HOUSE_LON
    );
  }

  return { distance, temperature, latitude, longitude };
}

// GraphDB 
/*
async function storeInGraphDB(deviceName, devState, temperature, distance, graphIRI) {
  const writer = new Writer({
    format: 'application/trig',
    prefixes: { ex: RDF_PREFIX, saref: SAREF, geo: GEO, rdf: RDF, dcterms: DCT }
  });

  const graphNode       = namedNode(graphIRI);
  const uniqueId        = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
  const messageNode     = namedNode(`${RDF_PREFIX}Message_${uniqueId}`);
  const measurementNode = namedNode(`${RDF_PREFIX}Measurement_${uniqueId}`);
  const deviceNode      = namedNode(`${RDF_PREFIX}${deviceName}`);

  let stateValue = "UNKNOWN";
  if (devState?.mode !== undefined)           stateValue = devState.mode;
  else if (devState?.isPowerOn !== undefined) stateValue = devState.isPowerOn ? "ON" : "OFF";

  writer.addQuad(messageNode,     namedNode(`${RDF}type`),              namedNode(`${SAREF}Message`),     graphNode);
  writer.addQuad(messageNode,     namedNode(`${SAREF}relatesTo`),       deviceNode,                       graphNode);
  writer.addQuad(messageNode,     namedNode(`${SAREF}hasMeasurement`),  measurementNode,                  graphNode);
  writer.addQuad(measurementNode, namedNode(`${RDF}type`),              namedNode(`${SAREF}Measurement`), graphNode);
  writer.addQuad(measurementNode, namedNode(`${SAREF}hasValue`),        literal(String(temperature)),     graphNode);
  writer.addQuad(messageNode,     namedNode(`${RDF_PREFIX}hasDistance`),literal(String(distance)),        graphNode);
  writer.addQuad(messageNode,     namedNode(`${RDF_PREFIX}hasState`),   literal(stateValue),              graphNode);
  writer.addQuad(messageNode,     namedNode(`${DCT}issued`),            literal(new Date().toISOString()),graphNode);

  writer.end(async (error, result) => {
    if (error) { console.error("RDF generation error:", error); return; }
    try {
      await axios.post(GRAPHDB_ENDPOINT, result, { headers: { "Content-Type": "application/trig" } });
      console.log(`[GraphDB] Stored RDF (${graphIRI})`);
    } catch (err) {
      console.error("[GraphDB] Write failed:", err.message);
      console.error("  status:", err.response?.status);
      console.error("  data:",   err.response?.data);
    }
  });
}
*/ 


async function storeInGraphDB(deviceName, devState, temperature, distance, graphIRI) {
  const writer = new Writer({
    format: 'application/trig',
    prefixes: {
      ex: RDF_PREFIX,
      saref: SAREF,
      geo: GEO,
      rdf: RDF,
      dcterms: DCT
    }
  });

  const graphNode   = namedNode(graphIRI);
  const uniqueId    = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;

  const messageNode = namedNode(`${RDF_PREFIX}message_${uniqueId}`);
  const deviceNode  = namedNode(`${RDF_PREFIX}device_${deviceName}`);

  // --- Message ---
  writer.addQuad(messageNode, namedNode(`${RDF}type`), namedNode(`${SAREF}Command`), graphNode);
  writer.addQuad(messageNode, namedNode(`${DCT}issued`), literal(new Date().toISOString()), graphNode);

  // --- Device ---
  writer.addQuad(messageNode, namedNode(`${SAREF}actsUpon`), deviceNode, graphNode);

  const normalizedState =
  typeof devState === "string"
    ? devState
    : devState?.mode ??
      (devState?.isPowerOn !== undefined
        ? (devState.isPowerOn ? "ON" : "OFF")
        : "UNKNOWN");

writer.addQuad(
  messageNode,
  namedNode(`${SAREF}hasCommandKind`),
  literal(normalizedState),
  graphNode
);
  
  // --- Sensor-derived context (optional enrichment) ---
  if (temperature != null) {
    writer.addQuad(
      messageNode,
      namedNode(`${RDF_PREFIX}temperature`),
      literal(String(temperature)),
      graphNode
    );
  }

  if (distance != null) {
    writer.addQuad(
      messageNode,
      namedNode(`${RDF_PREFIX}distance`),
      literal(String(distance)),
      graphNode
    );
  }

  writer.end(async (error, result) => {
    if (error) {
      console.error("RDF generation error:", error);
      return;
    }

    try {
      await axios.post(GRAPHDB_ENDPOINT, result, {
        headers: { "Content-Type": "application/trig" }
      });

      console.log(`[GraphDB] Stored RDF (${graphIRI})`);
    } catch (err) {
      console.error("[GraphDB] Write failed:", err.message);
    }
  });
}
async function processGraphQueue() {
  if (graphProcessing) return;
  graphProcessing = true;
  while (graphQueue.length > 0) {
    const job = graphQueue.shift();
    try {
      await storeInGraphDB(job.deviceName, job.state, job.temperature, job.distance, job.graphIRI);
      await new Promise(r => setTimeout(r, 1000));
    } catch (err) {
      console.error("GraphDB queue error:", err.message);
    }
  }
  graphProcessing = false;
}

//  MQTT — publish helper
function publishDeviceState(topic, state) {
  const stateMessage = {
    "@context": { "saref": "https://saref.etsi.org/core/" },
    "@type": "saref:State",
    "saref:hasState": { "@value": state }
  };

  mqttClient.publish(topic, JSON.stringify(stateMessage), { qos: 1 }, (err) => {
    if (err) console.error(`[MQTT] Publish failed on ${topic}:`, err.message);
    else     console.log(`[MQTT] Published -> ${topic}: ${state}`);
  });
}

// MQTT client 
const mqttClient = mqtt.connect(MQTT_BROKER);

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to broker');

  // GPS telemetry
  mqttClient.subscribe(TOPIC_GPS, { qos: 1 }, (err) => {
    if (err) console.error('[MQTT] GPS subscribe error:', err.message);
    else     console.log(`[MQTT] Subscribed to ${TOPIC_GPS}`);
  });

  // Command topics (DT sends, PT receives — we also listen to log them)
  mqttClient.subscribe(TOPIC_HVAC_COMMAND, { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_COMMAND,   { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_COMMAND,  { qos: 1 });
  console.log('[MQTT] Subscribed to command topics');

  // State confirmation topics (PT sends back after acting)
  mqttClient.subscribe(TOPIC_HVAC_STATE, { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_STATE,   { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_STATE,  { qos: 1 });
  console.log('[MQTT] Subscribed to state topics');
});

mqttClient.on('error', (err) => {
  console.error('[MQTT] Connection error:', err.message);
});

mqttClient.on('message', (topic, payload) => {
  try {
    const msg = JSON.parse(payload.toString());

    //  GPS telemetry 
    if (topic === TOPIC_GPS) {
      const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(msg);
      if (distance === null || temperature === null) return;

      updateLocationData(distance, temperature, latitude, longitude);

      // HVAC logic
      const prevHvac = deviceState.devices.hvac.isPowerOn;
      if (distance <= HVAC_THRESHOLD_KM) {
        deviceState.devices.hvac.isPowerOn = true;
        deviceState.devices.hvac.mode = temperature > 25 ? 'COOLING' : 'HEATING';
      } else {
        deviceState.devices.hvac.isPowerOn = false;
        deviceState.devices.hvac.mode = 'OFF';
      }
      if (prevHvac !== deviceState.devices.hvac.isPowerOn) {
        console.log(`[HVAC] State changed! -> ${deviceState.devices.hvac.mode}`);
        publishDeviceState(TOPIC_HVAC_STATE, deviceState.devices.hvac.mode);
      }
      console.log(`[HVAC] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.hvac.isPowerOn ? 'ON' : 'OFF'} (${deviceState.devices.hvac.mode})`);
      graphQueue.push({ deviceName: "hvac", state: deviceState.devices.hvac, temperature, distance, graphIRI: GRAPH_HVAC });

      // TV logic
      const prevTv = deviceState.devices.tv.isPowerOn;
      deviceState.devices.tv.isPowerOn = distance <= TV_THRESHOLD_KM;
      if (prevTv !== deviceState.devices.tv.isPowerOn) {
        console.log(`[TV] State changed! -> ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'}`);
        publishDeviceState(TOPIC_TV_STATE, deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
      }
      console.log(`[TV] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'}`);
      graphQueue.push({ deviceName: "tv", state: deviceState.devices.tv, temperature, distance, graphIRI: GRAPH_TV });

      // Barbecue logic
      const prevBbq = deviceState.devices.barbecue.isPowerOn;
      deviceState.devices.barbecue.isPowerOn = distance <= BARBECUE_THRESHOLD_KM;
      if (prevBbq !== deviceState.devices.barbecue.isPowerOn) {
        console.log(`[BBQ] State changed! -> ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'}`);
        publishDeviceState(TOPIC_BBQ_STATE, deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF');
      }
      console.log(`[BBQ] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'}`);
      graphQueue.push({ deviceName: "barbecue", state: deviceState.devices.barbecue, temperature, distance, graphIRI: GRAPH_BBQ });

      processGraphQueue();
      io.emit('device-state-update', deviceState);
      return;
    }

    //  Commands (DT → PT)
    if (topic === TOPIC_HVAC_COMMAND) {
      const cmd = msg?.['saref:hasCommandKind'];
      console.log(`[HVAC] Command received: ${cmd}`);
      if (cmd === 'TurnOn'  || cmd === 'TURN_ON')  { deviceState.devices.hvac.isPowerOn = true;  deviceState.devices.hvac.mode = 'HEATING'; }
      if (cmd === 'TurnOff' || cmd === 'TURN_OFF') { deviceState.devices.hvac.isPowerOn = false; deviceState.devices.hvac.mode = 'OFF'; }
      publishDeviceState(TOPIC_HVAC_STATE, deviceState.devices.hvac.mode);
      io.emit('device-state-update', deviceState);
      return;
    }

    if (topic === TOPIC_TV_COMMAND) {
      const cmd = msg?.['saref:hasCommandKind'];
      console.log(`[TV] Command received: ${cmd}`);
      if (cmd === 'TurnOn')  deviceState.devices.tv.isPowerOn = true;
      if (cmd === 'TurnOff') deviceState.devices.tv.isPowerOn = false;
      publishDeviceState(TOPIC_TV_STATE, deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
      io.emit('device-state-update', deviceState);
      return;
    }

    if (topic === TOPIC_BBQ_COMMAND) {
      const cmd = msg?.['saref:hasCommandKind'];
      console.log(`[BBQ] Command received: ${cmd}`);
      if (cmd === 'TurnOn')  deviceState.devices.barbecue.isPowerOn = true;
      if (cmd === 'TurnOff') deviceState.devices.barbecue.isPowerOn = false;
      publishDeviceState(TOPIC_BBQ_STATE, deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF');
      io.emit('device-state-update', deviceState);
      return;
    }

    //  State confirmations (informational log)
    if ([TOPIC_HVAC_STATE, TOPIC_TV_STATE, TOPIC_BBQ_STATE].includes(topic)) {
      console.log(`[MQTT] State confirmation on ${topic}:`, msg?.['saref:hasState']?.['@value']);
    }

  } catch (err) {
    console.error('[MQTT] Error processing message:', err.message);
  }
});

// Socket.io

io.on('connection', (socket) => {
  console.log('[Socket.io] Web client connected');
  socket.emit('device-state-update', deviceState);
  socket.on('disconnect', () => console.log('[Socket.io] Web client disconnected'));
});

//  Start 

const PORT = 3000;

server.listen(PORT, () => {
  console.log(`\n=== Smart Home Digital Twin (MQTT) ===`);
  console.log(`Running on http://localhost:${PORT}`);
  console.log(`MQTT Broker: ${MQTT_BROKER}`);
  console.log(`House Location: ${HOUSE_LAT}, ${HOUSE_LON}`);
  console.log(`\nDevice Thresholds:`);
  console.log(`  HVAC:     ${HVAC_THRESHOLD_KM} km`);
  console.log(`  Smart TV: ${TV_THRESHOLD_KM} km`);
  console.log(`  Barbecue: ${BARBECUE_THRESHOLD_KM} km\n`);
});

//  Graceful shutdown 

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  mqttClient.end();
  server.close();
  process.exit(0);
});