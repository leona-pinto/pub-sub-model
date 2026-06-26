import paho.mqtt.client as mqtt
import json
import time
import threading
import random
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


def create_saref_message(gps_data, temperature):
    """Build a JSON-LD message following a SAREF-aligned structure."""
    timestamp = datetime.now().isoformat()

    car_lat = float(gps_data.get("latitude", 0))
    car_lon = float(gps_data.get("longitude", 0))

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

            # LOCATION
            {
                "@id": f"urn:measurement:location:car:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "geo:Point",
                    "geo:lat": car_lat,
                    "geo:long": car_lon
                }
            },

            # TEMPERATURE
            {
                "@id": f"urn:measurement:temperature:car:{timestamp}",
                "@type": "saref:Measurement",
                "dcterms:created": timestamp,
                "saref:relatesToProperty": {
                    "@type": "saref:Temperature"
                },
                "saref:hasValue": {
                    "@type": "xsd:float",
                    "@value": temperature
                },
                "saref:hasUnit": "saref:Celsius"
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
    """Build a JSON-LD message for humidity sensor."""
    timestamp = datetime.now().isoformat()
    humidity_value = float(humidity_data.get("humidity", 0))

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
    """Build a JSON-LD message for accelerometer sensor."""
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

local_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="saref-publisher-local")

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
    """Every 2 seconds, wrap sensor data in SAREF messages and forward to local broker."""
    print("[PUBLISHER] Starting SAREF publish loop...")

    while True:
        with mqtt_data_lock:
            gps_snapshot = latest_gps_data
            humidity_snapshot = latest_humidity_data
            acceleration_snapshot = latest_acceleration_data

        # Publish GPS data
        if gps_snapshot is not None:
            temperature = round(random.uniform(15, 35), 2)
            saref_msg = create_saref_message(gps_snapshot, temperature)
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_GPS, payload, qos=1)
            if result.rc == 0:
                print(f"\n[PUBLISHER] GPS published to {LOCAL_MQTT_TOPIC_GPS}")
            else:
                print(f"[PUBLISHER] GPS publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER] Waiting for GPS data...")

        # Publish Humidity data
        if humidity_snapshot is not None:
            saref_msg = create_saref_message_humidity(humidity_snapshot)
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_HUMID, payload, qos=1)
            if result.rc == 0:
                print(f"[PUBLISHER] Humidity published to {LOCAL_MQTT_TOPIC_HUMID}")
            else:
                print(f"[PUBLISHER] Humidity publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER] Waiting for Humidity data...")

        # Publish Acceleration data
        if acceleration_snapshot is not None:
            saref_msg = create_saref_message_acceleration(acceleration_snapshot)
            payload = json.dumps(saref_msg)
            result = local_client.publish(LOCAL_MQTT_TOPIC_ACC, payload, qos=1)
            if result.rc == 0:
                print(f"[PUBLISHER] Acceleration published to {LOCAL_MQTT_TOPIC_ACC}")
            else:
                print(f"[PUBLISHER] Acceleration publish failed (rc={result.rc})")
        else:
            print("[PUBLISHER] Waiting for Acceleration data...")

        time.sleep(2)

#  Main 
# Connect to local Mosquitto first
connect_local_broker()

# Connect to external tracker broker
tracker_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="saref-publisher-tracker")
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