# Smart Home — IoT Pipeline & Digital Twin

A smart home prototype combining real-time GPS tracking, motion sensing, and soil humidity monitoring to automate home devices through MQTT. Built with SAREF-encoded semantic messages and a GraphDB knowledge graph.


**Sensors:** GPS tracker, accelerometer (IMU), soil humidity sensor  
**Actuators:** LED module (physical), HVAC / TV / BBQ (simulated)  
**Brokers:** University broker (sensors + LED) · Local Mosquitto (all internal topics)

## Prerequisites

- **Node.js** v18+
- **Python** 3.x
- **Mosquitto** MQTT broker — [mosquitto.org/download](https://mosquitto.org/download/)
- **GraphDB** (optional) — [graphdb.ontotext.com](https://graphdb.ontotext.com/) — repository named `smart-home`

## Installation

**1. Install Node.js dependencies**
```bash
npm install
```

**2. Install Python dependencies**
```bash
pip install paho-mqtt python-dotenv flask flask-socketio
```

**3. Create a `.env` file** in the project root:
```env
# External university broker (sensors + LED module)
MQTT_BROKER=mqtt.iot-lab.utwente.nl
MQTT_PORT=1883
MQTT_USERNAME=your_mqtt_user
MQTT_PASSWORD=your_mqtt_password
MQTT_USER=your_mqtt_user          # used for LED topic: trackers/{MQTT_USER}/leds/set

# Sensor topics on external broker
MQTT_TOPIC_GPS=trackers/your_mqtt_user/gps
MQTT_TOPIC_HUMID=trackers/your_mqtt_user/plant
MQTT_TOPIC_ACC=trackers/your_mqtt_user/imu

# Local broker (optional overrides — defaults to localhost:1883)
LOCAL_MQTT_BROKER=localhost
LOCAL_MQTT_PORT=1883
```

Replace `your_mqtt_user` with the `MQTT_USER` value provided by lab staff (same as in `secrets.h` on the LED module).

## Running the System

Start each component in a separate terminal, in this order:

**Terminal 1 — Mosquitto broker** (skip if already running as a Windows service)
```bash
mosquitto -v
```

**Terminal 2 — Home application** (subscriber + actuation logic)
```bash
node home-app.js
```

**Terminal 3 — Digital Twin** (dashboard + command interface)
```bash
node dt-server.js
```

**Terminal 4 — SAREF Publisher** (bridges external sensors to local broker)
```bash
python saref_publisher.py
```

**Terminal 5 — Actuator simulator** (optional — simulates HVAC / TV / BBQ responses)
```bash
python actuator_sim.py
```

**Browsers**
| Interface | URL |
|---|---|
| Digital Twin control center | http://localhost:4000/ui |
| GraphDB workbench | http://localhost:7200 |


## MQTT Topics

### External broker (`mqtt.iot-lab.utwente.nl`)
| Topic | Direction | Purpose |
|---|---|---|
| `trackers/{user}/gps` | Sensor → publisher | Raw GPS coordinates |
| `trackers/{user}/imu` | Sensor → publisher | Raw accelerometer data |
| `trackers/{user}/plant` | Sensor → publisher | Raw soil humidity data |
| `trackers/{user}/leds/set` | Home app / DT → LED | Hardware LED command (plain JSON) |

### Local broker (`localhost:1883`)
| Topic | Direction | Purpose |
|---|---|---|
| `sensor/gps` | Publisher → Home app / DT | GPS + temperature (SAREF JSON-LD) |
| `sensor/acceleration` | Publisher → Home app / DT | Acceleration X/Y/Z (SAREF JSON-LD) |
| `sensor/humidity` | Publisher → Home app / DT | Soil humidity (SAREF JSON-LD) |
| `home/hvac/state` | Home app → DT | HVAC state (OFF / HEATING / COOLING) |
| `home/tv/state` | Home app → DT | TV state (ON / OFF) |
| `home/bbq/state` | Home app → DT | BBQ state (ON / OFF) |
| `home/humidifier/state` | Home app → DT | Humidifier state (ON / OFF) |
| `home/led/command` | Home app / DT → DT | SAREF LED command (semantic record) |
| `home/hvac/command` | DT → Home app | Manual command to HVAC |
| `home/tv/command` | DT → Home app | Manual command to TV |
| `home/bbq/command` | DT → Home app | Manual command to BBQ |

> **Note on LED:** The LED module firmware only accepts plain JSON (`{ "animation": "..." }`), not SAREF. The system publishes a SAREF command to `home/led/command` for semantic completeness, and a translated hardware message to `trackers/{user}/leds/set` on the external broker.


## GraphDB

All device state changes and Digital Twin commands are stored as RDF triples in TriG format, organised into named graphs:

| Named graph | Contents |
|---|---|
| `http://example.org/graph/hvac` | HVAC state history |
| `http://example.org/graph/tv` | TV state history |
| `http://example.org/graph/barbecue` | BBQ state history |
| `http://example.org/graph/humidifier` | Humidifier state history |
| `http://example.org/dt` | DT commands and state events |
