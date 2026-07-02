import paho.mqtt.client as mqtt
import json
import time
import threading
import os
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# MQTT broker for receiving raw sensor data (external tracker)
MQTT_BROKER   = os.getenv("MQTT_BROKER")
MQTT_PORT     = os.getenv("MQTT_PORT")
MQTT_TOPIC_GPS   = os.getenv("MQTT_TOPIC_GPS")
MQTT_TOPIC_HUMID   = os.getenv("MQTT_TOPIC_HUMID")
MQTT_TOPIC_ACC   = os.getenv("MQTT_TOPIC_ACC")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# Local Mosquitto broker — where we forward SAREF messages
LOCAL_MQTT_BROKER = os.getenv("LOCAL_MQTT_BROKER", "localhost")
LOCAL_MQTT_PORT   = int(os.getenv("LOCAL_MQTT_PORT", "1883"))
LOCAL_MQTT_TOPIC_GPS = "sensor/gps"
LOCAL_MQTT_TOPIC_HUMID = "sensor/humidity"
LOCAL_MQTT_TOPIC_ACC = "sensor/acceleration"

missing_vars = []
if not MQTT_BROKER:       missing_vars.append("MQTT_BROKER")
if not MQTT_PORT:         missing_vars.append("MQTT_PORT")
if not MQTT_TOPIC_GPS:    missing_vars.append("MQTT_TOPIC_GPS")
if not MQTT_USERNAME:     missing_vars.append("MQTT_USERNAME")
if not MQTT_PASSWORD:     missing_vars.append("MQTT_PASSWORD")

if missing_vars:
    print(f"\nError: Missing environment variables: {', '.join(missing_vars)}")
    print("Create a .env file with the required variables.")
    exit(1)

try:
    MQTT_PORT = int(MQTT_PORT)
except ValueError:
    print(f"\nError: MQTT_PORT must be a number, got '{MQTT_PORT}'\n")
    exit(1)

# Shared state
latest_gps_data = None
latest_humidity_data = None
latest_acceleration_data = None
mqtt_data_lock = threading.Lock()

# Temperature trend simulation — oscillates ±5°C around raw sensor value over 10 seconds
TEMPERATURE_AMPLITUDE = 5  
CYCLE_TIME = 10            

def get_temperature_with_trend(raw_temperature):
   
    cycle_value = (time.time() % CYCLE_TIME) / CYCLE_TIME  

    if cycle_value < 0.5:
        
        progress = cycle_value * 2  # 0 to 1
        temperature = raw_temperature - (progress * TEMPERATURE_AMPLITUDE)
    else:
        progress = (cycle_value - 0.5) * 2  # 0 to 1
        temperature = (raw_temperature - TEMPERATURE_AMPLITUDE) + (progress * TEMPERATURE_AMPLITUDE)

    return temperature

# Humidity trend simulation — oscillates ±5% around raw sensor value over 10 seconds
HUMIDITY_AMPLITUDE = 5 
HUMIDITY_CYCLE_TIME = 10  

def get_humidity_with_trend(raw_humidity):
    """Oscillate humidity ±5% around raw sensor value over 10-second cycle."""
    cycle_value = (time.time() % HUMIDITY_CYCLE_TIME) / HUMIDITY_CYCLE_TIME  # 0 to 1 over HUMIDITY_CYCLE_TIME seconds

    if cycle_value < 0.5:
       
        progress = cycle_value * 2  
        humidity = raw_humidity - (progress * HUMIDITY_AMPLITUDE)
    else:
       
        progress = (cycle_value - 0.5) * 2 
        humidity = (raw_humidity - HUMIDITY_AMPLITUDE) + (progress * HUMIDITY_AMPLITUDE)

    return humidity


def create_saref_message(gps_data):
    timestamp = datetime.now().isoformat()

    car_lat = float(gps_data.get("latitude", 0))
    car_lon = float(gps_data.get("longitude", 0))

    # Simulate car movement: oscillate every 15 seconds (0 km ↔ 6 km)
    cycle_value = (time.time() % 15) / 15
    if cycle_value < 0.5:
        # First 7.5s: moving away (0 → 6 km)
        distance_frac = 1 - (cycle_value * 2)
    else:
        # Next 7.5s: moving closer (6 → 0 km)
        distance_frac = (cycle_value - 0.5) * 2

    # Convert distance fraction to lat/lon offset (~6 km radius)
    lat_offset = (distance_frac * 6) / 111
    lon_offset = (distance_frac * 6) / 111

    car_lat += lat_offset
    car_lon += lon_offset

    return {
        "@context": {
            "saref": "https://saref.etsi.org/core/",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dcterms": "http://purl.org/dc/terms/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },

        "@id": f"urn:message:location:car:{timestamp}",
        "@type": "ex:Message",
        "dcterms:issued": timestamp,

        "saref:hasMeasurement": [
            {
                "@id": f"urn:measurement:location:car:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "geo:Point",
                    "geo:lat": car_lat,
                    "geo:long": car_lon
                }
            }
        ],

        "ex:forDevice": {
            "@id": "urn:device:car:gps-tracker",
            "@type": "saref:Device",
            "rdfs:label": "Car GPS Tracker",
            "dcterms:identifier": "HTIT_51"
        },

        "ex:targetDevice": [
            {"@id": "urn:device:home:smart-tv", "@type": "saref:Device", "rdfs:label": "Smart TV"},
            {"@id": "urn:device:home:smart-hvac", "@type": "saref:Device", "rdfs:label": "Smart HVAC"},
            {"@id": "urn:device:home:smart-barbecue", "@type": "saref:Device", "rdfs:label": "Smart Barbecue"}
        ]
    }


def create_saref_message_humidity(humidity_data):
    timestamp = datetime.now().isoformat()
    raw_humidity_value = float(humidity_data.get("humidity", 0))
    raw_temperature_value = float(humidity_data.get("temperature", 0))

    # APPLY SIMULATED TRENDS TO BOTH TEMPERATURE AND HUMIDITY
    temperature_value = get_temperature_with_trend(raw_temperature_value)
    humidity_value = get_humidity_with_trend(raw_humidity_value)

    return {
        "@context": {
            "saref": "https://saref.etsi.org/core/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dcterms": "http://purl.org/dc/terms/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },

        "@id": f"urn:message:humidity:{timestamp}",
        "@type": "ex:Message",
        "dcterms:issued": timestamp,

        "saref:hasMeasurement": [
            {
                "@id": f"urn:measurement:humidity:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "saref:Humidity"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": humidity_value
                },
                "saref:hasUnit": "saref:Percent"
            },
            {
                "@id": f"urn:measurement:temperature:humidity:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "saref:Temperature"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": temperature_value
                },
                "saref:hasUnit": "saref:Celsius"
            }
        ],

        "ex:forDevice": {
            "@id": "urn:device:sensor:humidity-sensor",
            "@type": "saref:Device",
            "rdfs:label": "Humidity Sensor",
            "dcterms:identifier": "HUMIDITY_01"
        },

        "ex:targetDevice": [
            {"@id": "urn:device:home:smart-hvac", "@type": "saref:Device", "rdfs:label": "Smart HVAC"}
        ]
    }


def create_saref_message_acceleration(acc_data):
    timestamp = datetime.now().isoformat()
    acc_x = float(acc_data.get("accel_x", 0))
    acc_y = float(acc_data.get("accel_y", 0))
    acc_z = float(acc_data.get("accel_z", 0))

    return {
        "@context": {
            "saref": "https://saref.etsi.org/core/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dcterms": "http://purl.org/dc/terms/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },

        "@id": f"urn:message:acceleration:{timestamp}",
        "@type": "ex:Message",
        "dcterms:issued": timestamp,

        "saref:hasMeasurement": [
            {
                "@id": f"urn:measurement:acceleration:x:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "ex:AccelerationX"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": acc_x
                },
                "saref:hasUnit": "saref:MetersPerSecondSquared"
            },
            {
                "@id": f"urn:measurement:acceleration:y:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "ex:AccelerationY"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": acc_y
                },
                "saref:hasUnit": "saref:MetersPerSecondSquared"
            },
            {
                "@id": f"urn:measurement:acceleration:z:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "ex:AccelerationZ"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": acc_z
                },
                "saref:hasUnit": "saref:MetersPerSecondSquared"
            }
        ],

        "ex:forDevice": {
            "@id": "urn:device:sensor:accelerometer-sensor",
            "@type": "saref:Device",
            "rdfs:label": "Accelerometer Sensor",
            "dcterms:identifier": "ACC_01"
        },

        "ex:targetDevice": [
            {"@id": "urn:device:home:smart-tv", "@type": "saref:Device", "rdfs:label": "Smart TV"}
        ]
    }


#  Local Mosquitto publisher

local_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="saref-publisher-trend")

def connect_local_broker():
    local_client.connect(LOCAL_MQTT_BROKER, LOCAL_MQTT_PORT, 60)
    local_client.loop_start()
    print(f"[LOCAL MQTT] Connected to {LOCAL_MQTT_BROKER}:{LOCAL_MQTT_PORT}")

# External tracker MQTT callbacks

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[TRACKER MQTT] Connected to {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC_GPS)
        client.subscribe(MQTT_TOPIC_HUMID)
        client.subscribe(MQTT_TOPIC_ACC)
        print(f"[TRACKER MQTT] Subscribed to all sensors (GPS, Humidity, Acceleration)")
    else:
        print(f"[TRACKER MQTT] Connection failed with code {rc}")

def on_message(client, userdata, msg):
    global latest_gps_data, latest_humidity_data, latest_acceleration_data
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        with mqtt_data_lock:
            if msg.topic == MQTT_TOPIC_GPS:
                latest_gps_data = payload
                print(f"[TRACKER MQTT] GPS received: {payload.get('latitude')}, {payload.get('longitude')}")
            elif msg.topic == MQTT_TOPIC_HUMID:
                latest_humidity_data = payload
                print(f"[TRACKER MQTT] Humidity received: {payload.get('humidity')}%")
            elif msg.topic == MQTT_TOPIC_ACC:
                latest_acceleration_data = payload
                print(f"[TRACKER MQTT] Acceleration received: X={payload.get('accel_x')}, Y={payload.get('accel_y')}, Z={payload.get('accel_z')}")
    except json.JSONDecodeError:
        print(f"[TRACKER MQTT] Non-JSON message on '{msg.topic}'")

#  SAREF publish loop
def publish_saref_messages():
    print("[PUBLISHER-TREND] Starting SAREF publish loop with sensor-based oscillation...")
    print(f"[PUBLISHER-TREND] Temperature oscillates ±{TEMPERATURE_AMPLITUDE}°C around sensor value over {CYCLE_TIME} seconds")
    print(f"[PUBLISHER-TREND] Humidity oscillates ±{HUMIDITY_AMPLITUDE}% around sensor value over {HUMIDITY_CYCLE_TIME} seconds\n")

    while True:
        with mqtt_data_lock:
            gps_snapshot = latest_gps_data
            humidity_snapshot = latest_humidity_data
            acceleration_snapshot = latest_acceleration_data

        # Publish GPS data
        if gps_snapshot is not None:
            saref_msg = create_saref_message(gps_snapshot)
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_GPS, payload, qos=1)
            if result.rc == 0:
                print(f"\n[PUBLISHER-TREND] GPS published to {LOCAL_MQTT_TOPIC_GPS}")
            else:
                print(f"[PUBLISHER-TREND] GPS publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER-TREND] Waiting for GPS data...")

        # Publish Humidity data
        if humidity_snapshot is not None:
            saref_msg = create_saref_message_humidity(humidity_snapshot)
            raw_temp = float(humidity_snapshot.get("temperature", 0))
            raw_humid = float(humidity_snapshot.get("humidity", 0))
            trending_temp = get_temperature_with_trend(raw_temp)
            trending_humid = get_humidity_with_trend(raw_humid)
            cycle_val = (time.time() % HUMIDITY_CYCLE_TIME) / HUMIDITY_CYCLE_TIME
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_HUMID, payload, qos=1)
            if result.rc == 0:
                print(f"[DEBUG] Amplitude=±{HUMIDITY_AMPLITUDE}%, cycle_value={cycle_val:.2f}, trending_humid={trending_humid:.1f}")
                print(f"[PUBLISHER-TREND] Humidity published | Temp: {raw_temp:.1f}°C→{trending_temp:.1f}°C | Humidity: {raw_humid:.1f}%→{trending_humid:.1f}%")
                print(saref_msg)
            else:
                print(f"[PUBLISHER-TREND] Humidity publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER-TREND] Waiting for Humidity data...")

        # Publish Acceleration data
        if acceleration_snapshot is not None:
            saref_msg = create_saref_message_acceleration(acceleration_snapshot)
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_ACC, payload, qos=1)
            if result.rc == 0:
                print(f"[PUBLISHER-TREND] Acceleration published to {LOCAL_MQTT_TOPIC_ACC}")
            else:
                print(f"[PUBLISHER-TREND] Acceleration publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER-TREND] Waiting for Acceleration data...")

        time.sleep(2)

#  Main
# Connect to local Mosquitto first
connect_local_broker()

# Connect to external tracker broker
tracker_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="saref-publisher-trend-tracker")
tracker_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
tracker_client.on_connect = on_connect
tracker_client.on_message  = on_message

print(f"[TRACKER MQTT] Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
tracker_client.connect(MQTT_BROKER, MQTT_PORT, 60)

tracker_thread = threading.Thread(target=tracker_client.loop_forever, daemon=True)
tracker_thread.start()

# Give connections a moment to settle
time.sleep(2)

# Start publishing
try:
    publish_saref_messages()
except KeyboardInterrupt:
    print("\nShutting down...")
    tracker_client.disconnect()
    local_client.loop_stop()
    local_client.disconnect()
