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
const { config } = require('dotenv');

config();

const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: "*" } });

// Configuration 

const MQTT_BROKER = 'mqtt://localhost:1883';

const EXTERNAL_MQTT_BROKER   = process.env.MQTT_BROKER;
const EXTERNAL_MQTT_PORT     = process.env.MQTT_PORT;
const EXTERNAL_MQTT_USERNAME = process.env.MQTT_USERNAME;
const EXTERNAL_MQTT_PASSWORD = process.env.MQTT_PASSWORD;

const TOPIC_GPS = 'sensor/gps';

const TOPIC_HVAC_COMMAND = 'home/hvac/command';
const TOPIC_TV_COMMAND   = 'home/tv/command';
const TOPIC_BBQ_COMMAND  = 'home/bbq/command';

const TOPIC_HVAC_STATE       = 'home/hvac/state';
const TOPIC_TV_STATE         = 'home/tv/state';
const TOPIC_BBQ_STATE        = 'home/bbq/state';
const TOPIC_HUMIDIFIER_STATE = 'home/humidifier/state';

const TOPIC_LED_SEMANTIC = 'home/led/command';
const TOPIC_LED_HARDWARE = `trackers/${process.env.MQTT_USER || 'HTIT_51'}/leds/set`;
const ACCELERATION_SHAKE_THRESHOLD = 15;

const GRAPHDB_ENDPOINT = 'http://localhost:7200/repositories/smart-home/statements';

const RDF_PREFIX = 'http://example.org/';
const SAREF = "https://saref.etsi.org/core/";
const GEO   = "http://www.w3.org/2003/01/geo/wgs84_pos#";
const RDF   = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const DCT   = "http://purl.org/dc/terms/";

const GRAPH_HVAC = "http://example.org/graph/hvac";
const GRAPH_TV   = "http://example.org/graph/tv";
const GRAPH_BBQ  = "http://example.org/graph/barbecue";

const HOUSE_LAT = 52.2176;
const HOUSE_LON = 6.8904;

const HVAC_THRESHOLD_KM     = 3;
const TV_THRESHOLD_KM       = 2;
const BARBECUE_THRESHOLD_KM = 2;

//Global state 

const graphQueue = [];
let graphProcessing = false;

let deviceState = {
  carDistance:  null,
  carLatitude:  null,
  carLongitude: null,
  currentTemp:  0,
  lastUpdate:   null,
  messageCount: 0,
  devices: {
    hvac:     { isPowerOn: false, mode: 'OFF', humidifier: { isOn: false } },
    tv:       { isPowerOn: false },
    barbecue: { isPowerOn: true }
  }
};

let latestHumidity     = null;
let latestAcceleration = null;
let isMoving           = false;

//  Express

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHome.html'));
});

//Helpers 

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
  let latitude    = null;
  let longitude   = null;

  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const m of measurements) {
    const value    = m['saref:hasValue']?.['@value'];
    const property = m['saref:relatesToProperty'];
    const types    = [].concat(property?.['@type'] || []);

    if (types.includes('saref:Temperature') || types.includes('saref:TemperatureProperty')) {
      temperature = value;
    }
    if (types.includes('geo:Point') || types.includes('saref:Location') || types.includes('saref:LocationProperty')) {
      latitude  = property?.['geo:lat'];
      longitude = property?.['geo:long'];
    }
  }

  let distance = null;
  if (latitude != null && longitude != null) {
    distance = calculateDistance(Number(latitude), Number(longitude), HOUSE_LAT, HOUSE_LON);
  }

  return { distance, temperature, latitude, longitude };
}

function extractHumidityFromSarefMessage(sarefMsg) {
  const measurements = sarefMsg['saref:hasMeasurement'] || [];
  for (const m of measurements) {
    const types = [].concat(m['saref:relatesToProperty']?.['@type'] || []);
    if (types.includes('saref:Humidity')) return m['saref:hasValue']?.['@value'];
  }
  return null;
}

function extractAccelerationFromSarefMessage(sarefMsg) {
  const measurements = sarefMsg['saref:hasMeasurement'] || [];
  let accel = { x: 0, y: 0, z: 0 };

  for (const m of measurements) {
    const types = [].concat(m['saref:relatesToProperty']?.['@type'] || []);
    const value = m['saref:hasValue']?.['@value'];

    if (types.includes('ex:AccelerationX')) accel.x = parseFloat(value) || 0;
    if (types.includes('ex:AccelerationY')) accel.y = parseFloat(value) || 0;
    if (types.includes('ex:AccelerationZ')) accel.z = parseFloat(value) || 0;
  }

  accel.magnitude = Math.sqrt(accel.x ** 2 + accel.y ** 2 + accel.z ** 2);
  return accel;
}

// GraphDB 

async function storeInGraphDB(deviceName, devState, temperature, distance, graphIRI) {
  const writer = new Writer({
    format: 'application/trig',
    prefixes: { ex: RDF_PREFIX, saref: SAREF, geo: GEO, rdf: RDF, dcterms: DCT }
  });

  const graphNode   = namedNode(graphIRI);
  const uniqueId    = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
  const messageNode = namedNode(`${RDF_PREFIX}message_${uniqueId}`);
  const deviceNode  = namedNode(`${RDF_PREFIX}device_${deviceName}`);

  const normalizedState =
    typeof devState === "string" ? devState
    : devState?.mode ?? (devState?.isPowerOn !== undefined ? (devState.isPowerOn ? "ON" : "OFF") : "UNKNOWN");

  writer.addQuad(messageNode, namedNode(`${RDF}type`),              namedNode(`${SAREF}Command`), graphNode);
  writer.addQuad(messageNode, namedNode(`${DCT}issued`),            literal(new Date().toISOString()), graphNode);
  writer.addQuad(messageNode, namedNode(`${SAREF}actsUpon`),        deviceNode, graphNode);
  writer.addQuad(messageNode, namedNode(`${SAREF}hasCommandKind`),  literal(normalizedState), graphNode);

  if (temperature != null) writer.addQuad(messageNode, namedNode(`${RDF_PREFIX}temperature`), literal(String(temperature)), graphNode);
  if (distance    != null) writer.addQuad(messageNode, namedNode(`${RDF_PREFIX}distance`),    literal(String(distance)),    graphNode);

  writer.end(async (error, result) => {
    if (error) { console.error("RDF generation error:", error); return; }
    try {
      await axios.post(GRAPHDB_ENDPOINT, result, { headers: { "Content-Type": "application/trig" } });
      console.log(`[GraphDB] Stored (${graphIRI})`);
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

// MQTT publish helpers 
function publishDeviceState(topic, state) {
  const deviceMatch = topic.match(/home\/(\w+)\/state/);
  const deviceName  = deviceMatch ? deviceMatch[1] : 'unknown';

  const deviceMap = {
    hvac: { urn: 'urn:device:home:smart-hvac',     label: 'Smart HVAC',     propertyType: 'saref:OperatingMode' },
    tv:   { urn: 'urn:device:home:smart-tv',       label: 'Smart TV',       propertyType: 'saref:OnOffState' },
    bbq:  { urn: 'urn:device:home:smart-barbecue', label: 'Smart Barbecue', propertyType: 'saref:OnOffState' }
  };
  const device    = deviceMap[deviceName] || { urn: `urn:device:home:${deviceName}`, label: deviceName, propertyType: 'saref:State' };
  const timestamp = new Date().toISOString();

  const stateMessage = {
    "@context": { "saref": "https://saref.etsi.org/core/", "dcterms": "http://purl.org/dc/terms/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#" },
    "@id":   `urn:state:${deviceName}:${Date.now()}`,
    "@type": "saref:State",
    "dcterms:issued": timestamp,
    "saref:isStateOf":        { "@id": device.urn, "@type": "saref:Device", "rdfs:label": device.label },
    "saref:relatesToProperty":{ "@type": device.propertyType },
    "saref:hasValue":         { "@value": state }
  };

  mqttClient.publish(topic, JSON.stringify(stateMessage), { qos: 1 }, (err) => {
    if (err) console.error(`[MQTT] Publish failed on ${topic}:`, err.message);
    else     console.log(`[MQTT] Published -> ${topic}: ${state}`);
  });
}

function publishLedCommand(animation) {
  const sarefCommand = {
    "@context": { "saref": "https://saref.etsi.org/core/", "dcterms": "http://purl.org/dc/terms/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#" },
    "@id":   `urn:command:led:${Date.now()}`,
    "@type": "saref:Command",
    "dcterms:issued":       new Date().toISOString(),
    "saref:hasCommandKind": animation,
    "saref:actsUpon":       { "@id": "urn:device:home:smart-led", "@type": "saref:Device", "rdfs:label": "Smart LED Strip" }
  };

  mqttClient.publish(TOPIC_LED_SEMANTIC, JSON.stringify(sarefCommand), { qos: 1 }, (err) => {
    if (err) console.error(`[LED] SAREF publish failed:`, err.message);
    else     console.log(`[LED] SAREF command published: ${animation}`);
  });

  if (trackerClient) {
    trackerClient.publish(TOPIC_LED_HARDWARE, JSON.stringify({ animation }), { qos: 1 }, (err) => {
      if (err) console.error(`[LED] Hardware publish failed:`, err.message);
      else     console.log(`[LED] Hardware command sent: ${animation}`);
    });
  }
}

// MQTT clients 

const mqttClient = mqtt.connect(MQTT_BROKER);

let trackerClient = null;
if (EXTERNAL_MQTT_BROKER && EXTERNAL_MQTT_USERNAME && EXTERNAL_MQTT_PASSWORD) {
  trackerClient = mqtt.connect(`mqtt://${EXTERNAL_MQTT_BROKER}:${EXTERNAL_MQTT_PORT}`, {
    username: EXTERNAL_MQTT_USERNAME,
    password: EXTERNAL_MQTT_PASSWORD,
    clientId: `home-app-${Date.now()}`
  });
  trackerClient.on('connect', () => console.log(`[TRACKER MQTT] Connected to ${EXTERNAL_MQTT_BROKER}`));
  trackerClient.on('error',   (err) => console.error('[TRACKER MQTT] Error:', err.message));
}

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to local broker');

  mqttClient.subscribe(TOPIC_GPS,            { qos: 1 });
  mqttClient.subscribe('sensor/humidity',    { qos: 1 });
  mqttClient.subscribe('sensor/acceleration',{ qos: 1 });
  mqttClient.subscribe(TOPIC_HVAC_COMMAND,   { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_COMMAND,     { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_COMMAND,    { qos: 1 });
  mqttClient.subscribe(TOPIC_HVAC_STATE,       { qos: 1 });
  mqttClient.subscribe(TOPIC_TV_STATE,         { qos: 1 });
  mqttClient.subscribe(TOPIC_BBQ_STATE,        { qos: 1 });
  mqttClient.subscribe(TOPIC_HUMIDIFIER_STATE, { qos: 1 });
  console.log('[MQTT] Subscribed to all topics');

  // Publish initial states after 1 second so DT dashboard populates immediately
  setTimeout(() => {
    publishDeviceState(TOPIC_HVAC_STATE,       deviceState.devices.hvac.mode);
    publishDeviceState(TOPIC_TV_STATE,         deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
    publishDeviceState(TOPIC_BBQ_STATE,        deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF');
    publishDeviceState(TOPIC_HUMIDIFIER_STATE, deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF');
    console.log('[MQTT] Initial states published to DT');
  }, 1000);
  // Re-publish all current states every 10 seconds
// so the DT dashboard always catches up after a restart
setInterval(() => {
  publishDeviceState(TOPIC_HVAC_STATE,       deviceState.devices.hvac.mode);
  publishDeviceState(TOPIC_TV_STATE,         deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
  publishDeviceState(TOPIC_BBQ_STATE,        deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF');
  publishDeviceState(TOPIC_HUMIDIFIER_STATE, deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF');
  if (isMoving) publishLedCommand('rainbow');
}, 10000);
});

mqttClient.on('error', (err) => console.error('[MQTT] Connection error:', err.message));

mqttClient.on('message', (topic, payload) => {
  try {
    const msg = JSON.parse(payload.toString());

    //  GPS telemetry 
    if (topic === TOPIC_GPS) {
      const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(msg);
      if (distance === null || temperature === null) return;

      updateLocationData(distance, temperature, latitude, longitude);

      // HVAC
      const prevHvac = deviceState.devices.hvac.isPowerOn;
      if (distance <= HVAC_THRESHOLD_KM) {
        deviceState.devices.hvac.isPowerOn = true;
        deviceState.devices.hvac.mode = temperature > 25 ? 'COOLING' : 'HEATING';
      } else {
        deviceState.devices.hvac.isPowerOn = false;
        deviceState.devices.hvac.mode = 'OFF';
      }
      // Always publish so DT stays in sync
      publishDeviceState(TOPIC_HVAC_STATE, deviceState.devices.hvac.mode);
      if (prevHvac !== deviceState.devices.hvac.isPowerOn) {
        console.log(`[HVAC] State changed! -> ${deviceState.devices.hvac.mode}`);
      }
      console.log(`[HVAC] Distance: ${distance.toFixed(2)} km | ${deviceState.devices.hvac.mode}`);
      graphQueue.push({ deviceName: "hvac", state: deviceState.devices.hvac, temperature, distance, graphIRI: GRAPH_HVAC });

      // BBQ
      const prevBbq = deviceState.devices.barbecue.isPowerOn;
      deviceState.devices.barbecue.isPowerOn = distance <= BARBECUE_THRESHOLD_KM;
      // Always publish so DT stays in sync
      publishDeviceState(TOPIC_BBQ_STATE, deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF');
      if (prevBbq !== deviceState.devices.barbecue.isPowerOn) {
        console.log(`[BBQ] State changed! -> ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'}`);
      }
      console.log(`[BBQ] Distance: ${distance.toFixed(2)} km | ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'}`);
      graphQueue.push({ deviceName: "barbecue", state: deviceState.devices.barbecue, temperature, distance, graphIRI: GRAPH_BBQ });

      processGraphQueue();
      io.emit('device-state-update', deviceState);
      return;
    }

    //  Commands (DT -> PT) 
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

    //  Humidity sensor 
    if (topic === 'sensor/humidity') {
      const humidity = extractHumidityFromSarefMessage(msg);
      if (humidity !== null) {
        latestHumidity = parseFloat(humidity);
        const prevHumidifier = deviceState.devices.hvac.humidifier.isOn;
        deviceState.devices.hvac.humidifier.isOn = latestHumidity < 65;

        if (prevHumidifier !== deviceState.devices.hvac.humidifier.isOn) {
          console.log(`[HUMIDIFIER] State changed! -> ${deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF'}`);
        }

        // Always publish so DT stays in sync
        publishDeviceState(TOPIC_HUMIDIFIER_STATE, deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF');

        // Store in GraphDB
        graphQueue.push({
          deviceName: "humidifier",
          state:      deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF',
          temperature: null,
          distance:    null,
          graphIRI:   "http://example.org/graph/humidifier"
        });
        processGraphQueue();

        console.log(`[HUMIDITY] ${latestHumidity}% | Humidifier: ${deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF'}`);
        io.emit('device-state-update', deviceState);
      }
      return;
    }

    //  Accelerometer sensor 
    if (topic === 'sensor/acceleration') {
      const accel = extractAccelerationFromSarefMessage(msg);

      if (accel) {
        latestAcceleration = accel;
        console.log(`[ACCELERATION] X:${accel.x.toFixed(2)} Y:${accel.y.toFixed(2)} Z:${accel.z.toFixed(2)} mag:${accel.magnitude.toFixed(2)} m/s²`);

        // TV controlled by acceleration magnitude
        const prevTv = deviceState.devices.tv.isPowerOn;
        if      (accel.magnitude > 12) deviceState.devices.tv.isPowerOn = true;
        else if (accel.magnitude < 11) deviceState.devices.tv.isPowerOn = false;

        if (prevTv !== deviceState.devices.tv.isPowerOn) {
          console.log(`[TV] State changed! -> ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'}`);
          // Always publish TV state so DT picks it up
          publishDeviceState(TOPIC_TV_STATE, deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
        }

        // LED controlled by shake threshold
        if (accel.magnitude > ACCELERATION_SHAKE_THRESHOLD) {
          if (!isMoving) {
            publishLedCommand('rainbow');
            isMoving = true;
            console.log(`[LED] Shaking detected — rainbow started`);
          }
        } else {
          if (isMoving) {
            publishLedCommand('off');
            isMoving = false;
            console.log(`[LED] Shaking stopped — LEDs off`);
          }
        }

        io.emit('device-state-update', deviceState);
      }
      return;
    }

    //  State confirmations (log only) 
    if ([TOPIC_HVAC_STATE, TOPIC_TV_STATE, TOPIC_BBQ_STATE].includes(topic)) {
      console.log(`[MQTT] State confirmation on ${topic}:`, msg?.['saref:hasValue']?.['@value']);
    }

  } catch (err) {
    console.error('[MQTT] Error processing message:', err.message);
  }
});

//  Socket.io

io.on('connection', (socket) => {
  console.log('[Socket.io] Web client connected');
  socket.emit('device-state-update', deviceState);
  socket.on('disconnect', () => console.log('[Socket.io] Web client disconnected'));
});

// Start 

const PORT = 3000;

server.listen(PORT, () => {
  console.log(`\n=== Smart Home (MQTT) ===`);
  console.log(`Running on http://localhost:${PORT}`);
  console.log(`House: ${HOUSE_LAT}, ${HOUSE_LON}`);
  console.log(`Thresholds: HVAC ${HVAC_THRESHOLD_KM}km | BBQ ${BARBECUE_THRESHOLD_KM}km\n`);
});

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  mqttClient.end();
  if (trackerClient) trackerClient.end();
  server.close();
  process.exit(0);
});