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

// Local Mosquitto (for home devices)
const MQTT_BROKER = 'mqtt://localhost:1883';

// External Tracker MQTT (for remote sensors and LED module)
const EXTERNAL_MQTT_BROKER = process.env.MQTT_BROKER;
const EXTERNAL_MQTT_PORT = process.env.MQTT_PORT;
const EXTERNAL_MQTT_USERNAME = process.env.MQTT_USERNAME;
const EXTERNAL_MQTT_PASSWORD = process.env.MQTT_PASSWORD;

// Topics — PT → DT (incoming sensor data)
const TOPIC_GPS = 'sensor/gps';

// Topics — DT → PT (outgoing commands)
const TOPIC_HVAC_COMMAND = 'home/hvac/command';
const TOPIC_TV_COMMAND   = 'home/tv/command';
const TOPIC_BBQ_COMMAND  = 'home/bbq/command';

// Topics — PT → DT (device state confirmations)
const TOPIC_HVAC_STATE = 'home/hvac/state';
const TOPIC_TV_STATE   = 'home/tv/state';
const TOPIC_BBQ_STATE  = 'home/bbq/state';

// Actuator command topics
const TOPIC_LED_SEMANTIC = 'home/led/command';           // SAREF format (semantic)
const TOPIC_LED_HARDWARE = 'trackers/HTIT_51/leds/set';  // Hardware format (LED module)
const ACCELERATION_SHAKE_THRESHOLD = 15;

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
const HOUSE_LAT = 52.2176;
const HOUSE_LON = 6.8904;

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
    hvac:     { isPowerOn: false, mode: 'OFF', humidifier: { isOn: false } },
    tv:       { isPowerOn: false },
    barbecue: { isPowerOn: true }
  }
};

let latestHumidity = null;
let latestAcceleration = null;
let isMoving = false;

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

function extractHumidityFromSarefMessage(sarefMsg) {
  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const m of measurements) {
    const property = m['saref:relatesToProperty'];
    const types = [].concat(property?.['@type'] || []);

    if (types.includes('saref:Humidity')) {
      return m['saref:hasValue']?.['@value'];
    }
  }
  return null;
}

function extractAccelerationFromSarefMessage(sarefMsg) {
  const measurements = sarefMsg['saref:hasMeasurement'] || [];
  console.log(`[DEBUG] Number of measurements found: ${measurements.length}`);

  let accel = { x: 0, y: 0, z: 0 };

  for (const m of measurements) {
    const property = m['saref:relatesToProperty'];
    const types = [].concat(property?.['@type'] || []);
    const value = m['saref:hasValue']?.['@value'];

    console.log(`[DEBUG] Measurement type: ${types.join(', ')}, value: ${value}`);

    if (types.includes('ex:AccelerationX')) {
      accel.x = parseFloat(value) || 0;
      console.log(`[DEBUG] Set X = ${accel.x}`);
    }
    if (types.includes('ex:AccelerationY')) {
      accel.y = parseFloat(value) || 0;
      console.log(`[DEBUG] Set Y = ${accel.y}`);
    }
    if (types.includes('ex:AccelerationZ')) {
      accel.z = parseFloat(value) || 0;
      console.log(`[DEBUG] Set Z = ${accel.z}`);
    }
  }

  accel.magnitude = Math.sqrt(
    Math.pow(accel.x, 2) + Math.pow(accel.y, 2) + Math.pow(accel.z, 2)
  );

  console.log(`[DEBUG] Final magnitude: ${accel.magnitude}`);

  return accel;
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
      await axios.post(
        GRAPHDB_ENDPOINT,
        result,
        {
          headers: {
            "Content-Type": "application/trig"
          }
        }
      );

      console.log(`Stored RDF in GraphDB (${graphIRI})`);

    } catch (err) {

      console.error("GraphDB ERROR FULL DEBUG:");
      console.error("message:", err.message);
      console.error("code:", err.code);
      console.error("status:", err.response?.status);
      console.error("data:", err.response?.data);
    }
  });
}

// Non-blocking GraphDB write (fires in background, doesn't delay UI)
function writeToGraphDBAsync(deviceName, deviceState, temperature, distance, graphIRI) {
  const now = Date.now();

  // Check throttle (1 second per device)
  if (now - lastGraphWrite[deviceName] < 1000) {
    return; // Skip write if throttled
  }

  lastGraphWrite[deviceName] = now;

  // Fire off without awaiting - UI updates immediately
  storeInGraphDB(deviceName, deviceState, temperature, distance, graphIRI)
    .catch(err => console.error(`Background GraphDB write failed for ${deviceName}:`, err.message));
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

// HVAC Subscriber Logic
async function startHvacSubscriber() {
  await hvacConsumer.connect();
  console.log('[HVAC] Kafka consumer connected');

  await hvacConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[HVAC] Subscribed to Kafka topic');

  await hvacConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.hvac.isPowerOn;

          if (distance <= HVAC_THRESHOLD_KM) {
            deviceState.devices.hvac.isPowerOn = true;
            if (temperature > 25) {
              deviceState.devices.hvac.mode = 'COOLING';
            } else {
              deviceState.devices.hvac.mode = 'HEATING';
            }
          } else {
            deviceState.devices.hvac.isPowerOn = false;
            deviceState.devices.hvac.mode = 'OFF';
          }

          writeToGraphDBAsync("hvac", deviceState.devices.hvac, temperature, distance, GRAPH_HVAC);

          console.log(`[HVAC] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.hvac.isPowerOn ? 'ON' : 'OFF'} (${deviceState.devices.hvac.mode}) | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.hvac.isPowerOn) {
            console.log(`[HVAC] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[HVAC] Error processing message:', err.message);
      }
    }
  });
}

// Smart TV Subscriber Logic
async function startTvSubscriber() {
  await tvConsumer.connect();
  console.log('[TV] Kafka consumer connected');

  await tvConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[TV] Subscribed to Kafka topic');

  await tvConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.tv.isPowerOn;

          if (distance <= TV_THRESHOLD_KM) {
            deviceState.devices.tv.isPowerOn = true;
          } else {
            deviceState.devices.tv.isPowerOn = false;
          }

          writeToGraphDBAsync("tv", deviceState.devices.tv, temperature, distance, GRAPH_TV);

          console.log(`[TV] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.tv.isPowerOn) {
            console.log(`[TV] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[TV] Error processing message:', err.message);
      }
    }
  });
}

// Barbecue Subscriber Logic
async function startBarbecueSubscriber() {
  await barbecueConsumer.connect();
  console.log('[BARBECUE] Kafka consumer connected');

  await barbecueConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[BARBECUE] Subscribed to Kafka topic');

  await barbecueConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.barbecue.isPowerOn;

          if (distance > BARBECUE_THRESHOLD_KM) {
            deviceState.devices.barbecue.isPowerOn = false;
          } else {
            deviceState.devices.barbecue.isPowerOn = true;
          }

          writeToGraphDBAsync("barbecue", deviceState.devices.barbecue, temperature, distance, GRAPH_BBQ);

          console.log(`[BARBECUE] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.barbecue.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.barbecue.isPowerOn) {
            console.log(`[BARBECUE] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[BARBECUE] Error processing message:', err.message);
      }
    }
  });
}

//  MQTT — publish helper
function publishDeviceState(topic, state) {
  // Extract device name from topic (e.g., "home/hvac/state" → "hvac")
  const deviceMatch = topic.match(/home\/(\w+)\/state/);
  const deviceName = deviceMatch ? deviceMatch[1] : 'unknown';

  // Map device to URN and property type
  const deviceMap = {
    hvac: { urn: 'urn:device:home:smart-hvac', label: 'Smart HVAC', propertyType: 'saref:OperatingMode' },
    tv: { urn: 'urn:device:home:smart-tv', label: 'Smart TV', propertyType: 'saref:OnOffState' },
    bbq: { urn: 'urn:device:home:smart-barbecue', label: 'Smart Barbecue', propertyType: 'saref:OnOffState' }
  };

  const device = deviceMap[deviceName] || { urn: `urn:device:home:${deviceName}`, label: deviceName, propertyType: 'saref:State' };
  const timestamp = new Date().toISOString();

  const stateMessage = {
    "@context": {
      "saref": "https://saref.etsi.org/core/",
      "dcterms": "http://purl.org/dc/terms/",
      "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    },
    "@id": `urn:state:${deviceName}:${Date.now()}`,
    "@type": "saref:State",
    "dcterms:issued": timestamp,
    "saref:isStateOf": {
      "@id": device.urn,
      "@type": "saref:Device",
      "rdfs:label": device.label
    },
    "saref:relatesToProperty": {
      "@type": device.propertyType
    },
    "saref:hasValue": { "@value": state }
  };

  mqttClient.publish(topic, JSON.stringify(stateMessage), { qos: 1 }, (err) => {
    if (err) console.error(`[MQTT] Publish failed on ${topic}:`, err.message);
    else     console.log(`[MQTT] Published -> ${topic}: ${state}`);
  });
}

function publishLedCommand(animation) {
  const sarefCommand = {
    "@context": {
      "saref": "https://saref.etsi.org/core/",
      "dcterms": "http://purl.org/dc/terms/",
      "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    },
    "@id": `urn:command:led:${Date.now()}`,
    "@type": "saref:Command",
    "dcterms:issued": new Date().toISOString(),
    "saref:hasCommandKind": animation,
    "saref:actsUpon": {
      "@id": "urn:device:home:smart-led",
      "@type": "saref:Device",
      "rdfs:label": "Smart LED Strip"
    }
  };

  // Hardware format (for LED module)
  const hardwareCommand = { "animation": animation };

  // Publish SAREF to semantic topic (local broker)
  mqttClient.publish(TOPIC_LED_SEMANTIC, JSON.stringify(sarefCommand), { qos: 1 }, (err) => {
    if (err) console.error(`[LED] SAREF publish failed:`, err.message);
    else     console.log(`[LED] SAREF command published: ${animation}`);
  });

  // Publish to hardware topic (external tracker broker)
  if (trackerClient) {
    // SAREF format to external broker (TEST - can be removed)
    trackerClient.publish(TOPIC_LED_HARDWARE, JSON.stringify(sarefCommand), { qos: 1 }, (err) => {
      if (err) console.error(`[LED] SAREF publish to tracker failed:`, err.message);
      else     console.log(`[LED] SAREF command sent to tracker ${TOPIC_LED_HARDWARE}`);
    });

  } else {
    console.error(`[LED] Tracker MQTT client not initialized. Cannot send command.`);
  }
}

// MQTT clients
const mqttClient = mqtt.connect(MQTT_BROKER);

// External tracker MQTT client (for LED and other remote devices)
let trackerClient = null;
if (EXTERNAL_MQTT_BROKER && EXTERNAL_MQTT_USERNAME && EXTERNAL_MQTT_PASSWORD) {
  const trackerBrokerUrl = `mqtt://${EXTERNAL_MQTT_BROKER}:${EXTERNAL_MQTT_PORT}`;
  trackerClient = mqtt.connect(trackerBrokerUrl, {
    username: EXTERNAL_MQTT_USERNAME,
    password: EXTERNAL_MQTT_PASSWORD,
    clientId: `home-app-${Date.now()}`
  });

  trackerClient.on('connect', () => {
    console.log(`[TRACKER MQTT] Connected to ${EXTERNAL_MQTT_BROKER}:${EXTERNAL_MQTT_PORT}`);
  });

  trackerClient.on('error', (err) => {
    console.error('[TRACKER MQTT] Connection error:', err.message);
  });
}

mqttClient.on('connect', () => {
  console.log('[MQTT] Connected to local broker');

  // GPS telemetry
  mqttClient.subscribe(TOPIC_GPS, { qos: 1 }, (err) => {
    if (err) console.error('[MQTT] GPS subscribe error:', err.message);
    else     console.log(`[MQTT] Subscribed to ${TOPIC_GPS}`);
  });

  // Sensor topics
  mqttClient.subscribe('sensor/humidity', { qos: 1 }, (err) => {
    if (err) console.error('[MQTT] Humidity subscribe error:', err.message);
    else     console.log('[MQTT] Subscribed to sensor/humidity');
  });

  mqttClient.subscribe('sensor/acceleration', { qos: 1 }, (err) => {
    if (err) console.error('[MQTT] Acceleration subscribe error:', err.message);
    else     console.log('[MQTT] Subscribed to sensor/acceleration');
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

      // TV logic removed - now controlled by accelerometer only

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

    //  Humidity sensor
    if (topic === 'sensor/humidity') {
      const humidity = extractHumidityFromSarefMessage(msg);
      if (humidity !== null) {
        latestHumidity = parseFloat(humidity);
        const prevHumidifierState = deviceState.devices.hvac.humidifier.isOn;

        if (latestHumidity < 65) {
          deviceState.devices.hvac.humidifier.isOn = true;
        } else {
          deviceState.devices.hvac.humidifier.isOn = false;
        }

        if (prevHumidifierState !== deviceState.devices.hvac.humidifier.isOn) {
          console.log(`[HUMIDIFIER] State changed! -> ${deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF'}`);
        }
        console.log(`[HUMIDITY] Value: ${latestHumidity}% | Humidifier: ${deviceState.devices.hvac.humidifier.isOn ? 'ON' : 'OFF'}`);
        io.emit('device-state-update', deviceState);
      }
      return;
    }

    //  Accelerometer sensor
    if (topic === 'sensor/acceleration') {
      console.log(`[DEBUG] Raw acceleration message:`, JSON.stringify(msg, null, 2));
      const accel = extractAccelerationFromSarefMessage(msg);
      console.log(`[DEBUG] Extracted acceleration:`, accel);

      if (accel !== null) {
        latestAcceleration = accel;
        console.log(`[ACCELERATION] X: ${accel.x.toFixed(2)}, Y: ${accel.y.toFixed(2)}, Z: ${accel.z.toFixed(2)} m/s²`);
        console.log(`[ACCELERATION] Magnitude: ${accel.magnitude.toFixed(2)} m/s² | Threshold: ${ACCELERATION_SHAKE_THRESHOLD} m/s²`);

        const prevTv = deviceState.devices.tv.isPowerOn;

        if (accel.magnitude > 12) {
          deviceState.devices.tv.isPowerOn = true;
        } else if (accel.magnitude < 11) {
          deviceState.devices.tv.isPowerOn = false;
        }

        if (prevTv !== deviceState.devices.tv.isPowerOn) {
          console.log(`[TV] State changed! -> ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'}`);
          publishDeviceState(TOPIC_TV_STATE, deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF');
        }

        // LED actuator control based on shaking
        if (accel.magnitude > ACCELERATION_SHAKE_THRESHOLD) {
          if (!isMoving) {
            publishLedCommand('rainbow');
            isMoving = true;
            console.log(`[LED] ✨ Shaking detected! Rainbow animation started.`);
          }
        } else {
          if (isMoving) {
            publishLedCommand('off');
            isMoving = false;
            console.log(`[LED] Shaking stopped! LEDs turned off.`);
          }
        }

        console.log(`[ACCELERATION] TV: ${deviceState.devices.tv.isPowerOn ? 'ON' : 'OFF'} | Shaking: ${isMoving ? 'YES' : 'NO'}`);
        io.emit('device-state-update', deviceState);
      } else {
        console.log(`[DEBUG] Failed to extract acceleration from message`);
      }
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