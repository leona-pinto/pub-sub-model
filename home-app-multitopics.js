require('dotenv').config();

const express = require('express');
const { Kafka } = require('kafkajs');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const axios = require('axios');
const N3 = require('n3');
const { DataFactory, Writer } = N3;
const namedNode = DataFactory.namedNode;
const literal = DataFactory.literal;

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: { origin: "*" }
});

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER ;
// const KAFKA_TOPIC = process.env.KAFKA_TOPIC ;

// Multiple topics configuration
const KAFKA_TOPIC_1 = process.env.KAFKA_TOPIC ;
const KAFKA_TOPIC_2 = process.env.KAFKA_TOPIC_2 ;
const KAFKA_TOPIC_3 = process.env.KAFKA_TOPIC_3;


// GraphDB 
const GRAPHDB_ENDPOINT =
  'http://localhost:7200/repositories/smart-home/statements';

const RDF_PREFIX = 'http://example.org/';
const SAREF = "https://saref.etsi.org/core/";
const GEO = "http://www.w3.org/2003/01/geo/wgs84_pos#";
const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const DCT = "http://purl.org/dc/terms/";

const GRAPH_HVAC = "http://example.org/graph/hvac";
const GRAPH_TV = "http://example.org/graph/tv";
const GRAPH_BBQ = "http://example.org/graph/barbecue";



// House location
const HOUSE_LAT = 52.2176;
const HOUSE_LON = 6.8904;

// Device thresholds
const HVAC_THRESHOLD_KM = 3;
const TV_THRESHOLD_KM = 2;
const BARBECUE_THRESHOLD_KM = 2;

// GraphDB throttling 
const GRAPHDB_INTERVAL_MS = 2000; // 2 seconds per device

let lastGraphWrite = {
  hvac: 0,
  tv: 0,
  barbecue: 0
};

// Global Device State
let deviceState = {
  carDistance: null,
  carLatitude: null,
  carLongitude: null,
  currentTemp: 0,
  lastUpdate: null,
  messageCount: 0,
  devices: {
    hvac: {
      isPowerOn: false,
      mode: 'OFF' // OFF, HEATING, COOLING
    },
    tv: {
      isPowerOn: false
    },
    barbecue: {
      isPowerOn: true // Default to ON
    }
  }
};

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'smart-home-subscriber',
  brokers: [KAFKA_BROKER]
});

// Create separate consumers for each topic (multi-topic setup)
const topic1Consumer = kafka.consumer({ groupId: 'topic1-consumer-group' });
const topic2Consumer = kafka.consumer({ groupId: 'topic2-consumer-group' });
const topic3Consumer = kafka.consumer({ groupId: 'topic3-consumer-group' });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHome.html'));
});

// Calculate distance between two GPS coordinates using Haversine formula
function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371; // Earth's radius in km
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// Extract temperature and location from SAREF message
function extractDataFromSarefMessage(sarefMsg) {
  let temperature = null;
  let latitude = null;
  let longitude = null;

  const measurements = sarefMsg['saref:hasMeasurement'] || [];

  for (const measurement of measurements) {
    const propType = measurement['saref:hasProperty']?.['@type'];

    if (propType === 'saref:TemperatureProperty') {
      temperature = measurement['saref:hasProperty']['saref:hasValue']['@value'];
    }

    if (propType === 'saref:LocationProperty') {
      latitude = measurement['saref:hasProperty']['geo:lat']?.['@value'];
      longitude = measurement['saref:hasProperty']['geo:long']?.['@value'];
    }
  }

  // Calculate distance from home
  let distance = null;
  if (latitude !== null && longitude !== null) {
    distance = calculateDistance(latitude, longitude, HOUSE_LAT, HOUSE_LON);
  }

  return { distance, temperature, latitude, longitude };
}

// Update shared location data
function updateLocationData(distance, temperature, latitude, longitude) {
  deviceState.carDistance = distance;
  deviceState.currentTemp = temperature;
  deviceState.carLatitude = latitude;
  deviceState.carLongitude = longitude;
  deviceState.lastUpdate = new Date().toISOString();
  deviceState.messageCount++;
}


// making rdf triples out of message & uploading to GraphDB
async function storeInGraphDB(
  deviceName,
  deviceState,
  temperature,
  distance,
  graphIRI
) {

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

  // graph node
  const graphNode = namedNode(graphIRI);

  // unique IDs
  const uniqueId = `${Date.now()}_${Math.random().toString(36).substring(2,8)}`;

  const messageNode = namedNode(`${RDF_PREFIX}Message_${uniqueId}`);
  const measurementNode = namedNode(`${RDF_PREFIX}Measurement_${uniqueId}`);
  const deviceNode = namedNode(`${RDF_PREFIX}${deviceName}`);

  // determine device state
  let stateValue = "UNKNOWN";

  if (deviceState?.mode !== undefined) {
    stateValue = deviceState.mode; // HVAC
  } else if (deviceState?.isPowerOn !== undefined) {
    stateValue = deviceState.isPowerOn ? "ON" : "OFF"; // TV / BBQ
  }


  writer.addQuad(
    messageNode,
    namedNode(`${RDF}type`),
    namedNode(`${SAREF}Message`),
    graphNode
  );


  writer.addQuad(
    messageNode,
    namedNode(`${SAREF}relatesTo`),
    deviceNode,
    graphNode
  );

  

  writer.addQuad(
    messageNode,
    namedNode(`${SAREF}hasMeasurement`),
    measurementNode,
    graphNode
  );

  // measurement type
  writer.addQuad(
    measurementNode,
    namedNode(`${RDF}type`),
    namedNode(`${SAREF}Measurement`),
    graphNode
  );

  // temperature value
  writer.addQuad(
    measurementNode,
    namedNode(`${SAREF}hasValue`),
    literal(String(temperature)),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${RDF_PREFIX}hasDistance`),
    literal(String(distance)),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${RDF_PREFIX}hasState`),
    literal(stateValue),
    graphNode
  );

  writer.addQuad(
    messageNode,
    namedNode(`${DCT}issued`),
    literal(new Date().toISOString()),
    graphNode
  );

  
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

   

// Multi-Topic Subscriber 1 (Topic 1)
async function startTopic1Subscriber() {
  await topic1Consumer.connect();
  console.log('[TOPIC-1] Kafka consumer connected');

  await topic1Consumer.subscribe({ topic: KAFKA_TOPIC_1, fromBeginning: false });
  console.log(`[TOPIC-1] Subscribed to Kafka topic: ${KAFKA_TOPIC_1}`);

  await topic1Consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          console.log(`[TOPIC-1] Received from ${KAFKA_TOPIC_1} | Distance: ${distance.toFixed(2)} km | Temp: ${temperature.toFixed(2)}°C`);
          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error(`[TOPIC-1] Error processing message from ${KAFKA_TOPIC_1}:`, err.message);
      }
    }
  });
}

// Multi-Topic Subscriber 2 (Topic 2)
async function startTopic2Subscriber() {
  await topic2Consumer.connect();
  console.log('[TOPIC-2] Kafka consumer connected');

  await topic2Consumer.subscribe({ topic: KAFKA_TOPIC_2, fromBeginning: false });
  console.log(`[TOPIC-2] Subscribed to Kafka topic: ${KAFKA_TOPIC_2}`);

  await topic2Consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          console.log(`[TOPIC-2] Received from ${KAFKA_TOPIC_2} | Distance: ${distance.toFixed(2)} km | Temp: ${temperature.toFixed(2)}°C`);
          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error(`[TOPIC-2] Error processing message from ${KAFKA_TOPIC_2}:`, err.message);
      }
    }
  });
}

// Multi-Topic Subscriber 3 (Topic 3)
async function startTopic3Subscriber() {
  await topic3Consumer.connect();
  console.log('[TOPIC-3] Kafka consumer connected');

  await topic3Consumer.subscribe({ topic: KAFKA_TOPIC_3, fromBeginning: false });
  console.log(`[TOPIC-3] Subscribed to Kafka topic: ${KAFKA_TOPIC_3}`);

  await topic3Consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          console.log(`[TOPIC-3] Received from ${KAFKA_TOPIC_3} | Distance: ${distance.toFixed(2)} km | Temp: ${temperature.toFixed(2)}°C`);
          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error(`[TOPIC-3] Error processing message from ${KAFKA_TOPIC_3}:`, err.message);
      }
    }
  });
}

// Socket.io events
io.on('connection', (socket) => {
  console.log('[Socket.io] Web client connected');

  // Send current global device state to new client
  socket.emit('device-state-update', deviceState);

  socket.on('disconnect', () => {
    console.log('[Socket.io] Web client disconnected');
  });
});

// Start server
const PORT = 3000;
server.listen(PORT, () => {
  console.log(`\n=== Smart Home Multi-Device Subscriber ===`);
  console.log(`Running on http://localhost:${PORT}`);
  console.log(`House Location: ${HOUSE_LAT}, ${HOUSE_LON}\n`);

  console.log(`Device Thresholds:`);
  console.log(`  HVAC: ${HVAC_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Smart TV: ${TV_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Barbecue: ${BARBECUE_THRESHOLD_KM} km (separate consumer group)\n`);

  console.log(`Starting subscribers...\n`);

  startTopic1Subscriber().catch(err => console.error('Topic 1 subscriber error:', err));
  startTopic2Subscriber().catch(err => console.error('Topic 2 subscriber error:', err));
  startTopic3Subscriber().catch(err => console.error('Topic 3 subscriber error:', err));
});

process.on('SIGINT', () => {
  console.log('\n\nShutting down all subscribers...');
  Promise.all([
    topic1Consumer.disconnect(),
    topic2Consumer.disconnect(),
    topic3Consumer.disconnect()
  ]).then(() => {
    server.close();
    process.exit(0);
  });
});
