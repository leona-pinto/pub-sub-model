const express = require('express');
const { Kafka } = require('kafkajs');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: { origin: "*" }
});

// Configuration
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'trackers.HTIT_51.gps';

// Consumer Groups for Each Device
const HVAC_CONSUMER_GROUP = 'hvac-subscriber-group';
const TV_CONSUMER_GROUP = 'tv-subscriber-group';
const BARBECUE_CONSUMER_GROUP = 'barbecue-subscriber-group';
const FAN_CONSUMER_GROUP = 'fan-subscriber-group';
const AIR_PURIFIER_CONSUMER_GROUP = 'air-purifier-subscriber-group';
const DRYER_CONSUMER_GROUP = 'dryer-subscriber-group';
const OVEN_CONSUMER_GROUP = 'oven-subscriber-group';
const WASHING_MACHINE_CONSUMER_GROUP = 'washing-machine-subscriber-group';

// House location
const HOUSE_LAT = 52.2180;
const HOUSE_LON = 6.8900;

// Device thresholds
const HVAC_THRESHOLD_KM = 3;
const TV_THRESHOLD_KM = 2;
const BARBECUE_THRESHOLD_KM = 2;
const FAN_THRESHOLD_KM = 1.5;
const AIR_PURIFIER_THRESHOLD_KM = 2.5;
const DRYER_THRESHOLD_KM = 1;
const OVEN_THRESHOLD_KM = 1.5;
const WASHING_MACHINE_THRESHOLD_KM = 1;

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
      mode: 'OFF'
    },
    tv: {
      isPowerOn: false
    },
    barbecue: {
      isPowerOn: true
    },
    fan: {
      isPowerOn: false
    },
    airPurifier: {
      isPowerOn: false
    },
    dryer: {
      isPowerOn: false
    },
    oven: {
      isPowerOn: false
    },
    washingMachine: {
      isPowerOn: false
    }
  }
};

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'smart-home-subscriber',
  brokers: [KAFKA_BROKER]
});

// Create separate consumers for each device
const hvacConsumer = kafka.consumer({ groupId: HVAC_CONSUMER_GROUP });
const tvConsumer = kafka.consumer({ groupId: TV_CONSUMER_GROUP });
const barbecueConsumer = kafka.consumer({ groupId: BARBECUE_CONSUMER_GROUP });
const fanConsumer = kafka.consumer({ groupId: FAN_CONSUMER_GROUP });
const airPurifierConsumer = kafka.consumer({ groupId: AIR_PURIFIER_CONSUMER_GROUP });
const dryerConsumer = kafka.consumer({ groupId: DRYER_CONSUMER_GROUP });
const ovenConsumer = kafka.consumer({ groupId: OVEN_CONSUMER_GROUP });
const washingMachineConsumer = kafka.consumer({ groupId: WASHING_MACHINE_CONSUMER_GROUP });

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'smartHomescalable.html'));
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

// Fan Subscriber Logic
async function startFanSubscriber() {
  await fanConsumer.connect();
  console.log('[FAN] Kafka consumer connected');

  await fanConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[FAN] Subscribed to Kafka topic');

  await fanConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.fan.isPowerOn;

          if (distance <= FAN_THRESHOLD_KM) {
            deviceState.devices.fan.isPowerOn = true;
          } else {
            deviceState.devices.fan.isPowerOn = false;
          }

          console.log(`[FAN] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.fan.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.fan.isPowerOn) {
            console.log(`[FAN] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[FAN] Error processing message:', err.message);
      }
    }
  });
}

// Air Purifier Subscriber Logic
async function startAirPurifierSubscriber() {
  await airPurifierConsumer.connect();
  console.log('[AIR_PURIFIER] Kafka consumer connected');

  await airPurifierConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[AIR_PURIFIER] Subscribed to Kafka topic');

  await airPurifierConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.airPurifier.isPowerOn;

          if (distance <= AIR_PURIFIER_THRESHOLD_KM) {
            deviceState.devices.airPurifier.isPowerOn = true;
          } else {
            deviceState.devices.airPurifier.isPowerOn = false;
          }

          console.log(`[AIR_PURIFIER] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.airPurifier.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.airPurifier.isPowerOn) {
            console.log(`[AIR_PURIFIER] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[AIR_PURIFIER] Error processing message:', err.message);
      }
    }
  });
}

// Dryer Subscriber Logic
async function startDryerSubscriber() {
  await dryerConsumer.connect();
  console.log('[DRYER] Kafka consumer connected');

  await dryerConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[DRYER] Subscribed to Kafka topic');

  await dryerConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.dryer.isPowerOn;

          if (distance <= DRYER_THRESHOLD_KM) {
            deviceState.devices.dryer.isPowerOn = true;
          } else {
            deviceState.devices.dryer.isPowerOn = false;
          }

          console.log(`[DRYER] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.dryer.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.dryer.isPowerOn) {
            console.log(`[DRYER] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[DRYER] Error processing message:', err.message);
      }
    }
  });
}

// Oven Subscriber Logic
async function startOvenSubscriber() {
  await ovenConsumer.connect();
  console.log('[OVEN] Kafka consumer connected');

  await ovenConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[OVEN] Subscribed to Kafka topic');

  await ovenConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.oven.isPowerOn;

          if (distance <= OVEN_THRESHOLD_KM && temperature <= 25) {
            deviceState.devices.oven.isPowerOn = true;
          } else {
            deviceState.devices.oven.isPowerOn = false;
          }

          console.log(`[OVEN] Distance: ${distance.toFixed(2)} km | Temp: ${temperature.toFixed(2)}°C | Power: ${deviceState.devices.oven.isPowerOn ? 'ON' : 'OFF'}`);

          if (previousState !== deviceState.devices.oven.isPowerOn) {
            console.log(`[OVEN] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[OVEN] Error processing message:', err.message);
      }
    }
  });
}

// Washing Machine Subscriber Logic
async function startWashingMachineSubscriber() {
  await washingMachineConsumer.connect();
  console.log('[WASHING_MACHINE] Kafka consumer connected');

  await washingMachineConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log('[WASHING_MACHINE] Subscribed to Kafka topic');

  await washingMachineConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const sarefMsg = JSON.parse(message.value.toString());
        const { distance, temperature, latitude, longitude } = extractDataFromSarefMessage(sarefMsg);

        if (distance !== null && temperature !== null) {
          updateLocationData(distance, temperature, latitude, longitude);

          const previousState = deviceState.devices.washingMachine.isPowerOn;

          if (distance <= WASHING_MACHINE_THRESHOLD_KM) {
            deviceState.devices.washingMachine.isPowerOn = true;
          } else {
            deviceState.devices.washingMachine.isPowerOn = false;
          }

          console.log(`[WASHING_MACHINE] Distance: ${distance.toFixed(2)} km | Power: ${deviceState.devices.washingMachine.isPowerOn ? 'ON' : 'OFF'} | Temp: ${temperature.toFixed(2)}°C`);

          if (previousState !== deviceState.devices.washingMachine.isPowerOn) {
            console.log(`[WASHING_MACHINE] State changed!\n`);
          }

          io.emit('device-state-update', deviceState);
        }
      } catch (err) {
        console.error('[WASHING_MACHINE] Error processing message:', err.message);
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
  console.log(`  Barbecue: ${BARBECUE_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Fan: ${FAN_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Air Purifier: ${AIR_PURIFIER_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Dryer: ${DRYER_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Oven: ${OVEN_THRESHOLD_KM} km (separate consumer group)`);
  console.log(`  Washing Machine: ${WASHING_MACHINE_THRESHOLD_KM} km (separate consumer group)\n`);

  console.log(`Starting subscribers...\n`);

  startHvacSubscriber().catch(err => console.error('HVAC subscriber error:', err));
  startTvSubscriber().catch(err => console.error('TV subscriber error:', err));
  startBarbecueSubscriber().catch(err => console.error('Barbecue subscriber error:', err));
  startFanSubscriber().catch(err => console.error('Fan subscriber error:', err));
  startAirPurifierSubscriber().catch(err => console.error('Air Purifier subscriber error:', err));
  startDryerSubscriber().catch(err => console.error('Dryer subscriber error:', err));
  startOvenSubscriber().catch(err => console.error('Oven subscriber error:', err));
  startWashingMachineSubscriber().catch(err => console.error('Washing Machine subscriber error:', err));
});

process.on('SIGINT', () => {
  console.log('\n\nShutting down all subscribers...');
  Promise.all([
    hvacConsumer.disconnect(),
    tvConsumer.disconnect(),
    barbecueConsumer.disconnect(),
    fanConsumer.disconnect(),
    airPurifierConsumer.disconnect(),
    dryerConsumer.disconnect(),
    ovenConsumer.disconnect(),
    washingMachineConsumer.disconnect()
  ]).then(() => {
    server.close();
    process.exit(0);
  });
});
