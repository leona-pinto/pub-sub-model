# Smart Home — MQTT Pub/Sub Architecture

## Getting Started

### Prerequisites

- **Node.js** v14+
- **Python 3.x**
- **Mosquitto** MQTT broker (download at https://mosquitto.org/download/)

### Installation

1. Install Node.js dependencies
```bash
   npm install
```

2. Install Python dependencies
```bash
   pip install paho-mqtt python-dotenv
```

### Running the System

**Terminal 1: Start Mosquitto** (if not already running as a service)
```bash
mosquitto -v
```

**Terminal 2: Start the Home Application**
```bash
node home-app.js
```

**Terminal 3: Start the Digital Twin**
```bash
node dt-server.js
```

**Terminal 4: Start the SAREF Publisher**
```bash
python saref_publisher.py
```

**Browser: Access Smart Home Dashboard**
http:://localhost:3000 

**Browser: Access Digital Twin Control Center**
http://localhost:4000/ui 

### MQTT Topics

| Topic | Direction | Purpose |
|---|---|---|
| `trackers/HTIT_51/gps` | Publisher → Home app | GPS location and temperature |
| `home/hvac/state` | Home app → DT | HVAC state after change |
| `home/tv/state` | Home app → DT | TV state after change |
| `home/bbq/state` | Home app → DT | BBQ state after change |
| `home/hvac/command` | DT → Home app | Command to HVAC |
| `home/tv/command` | DT → Home app | Command to TV |
| `home/bbq/command` | DT → Home app | Command to BBQ |

### Environment Variables

Create a `.env` file in the project root:

```env
MQTT_BROKER=<external tracker broker address>
MQTT_PORT=<port>
MQTT_TOPIC=<topic the tracker publishes to>
MQTT_USERNAME=<username>
MQTT_PASSWORD=<password>
```

> Note: `LOCAL_MQTT_BROKER` and `LOCAL_MQTT_PORT` default to `localhost` and `1883`
> and do not need to be set unless your local Mosquitto runs elsewhere.