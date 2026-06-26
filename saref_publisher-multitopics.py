import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import time
import threading
import random
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MQTT broker 
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = os.getenv("MQTT_PORT")
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# Kafka broker details
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Multiple topics 
KAFKA_TOPIC_1 = os.getenv("KAFKA_TOPIC")
KAFKA_TOPIC_2 = os.getenv("KAFKA_TOPIC_2")
KAFKA_TOPIC_3 = os.getenv("KAFKA_TOPIC_3")

missing_vars = []
if not MQTT_BROKER:
    missing_vars.append("MQTT_BROKER")
if not MQTT_PORT:
    missing_vars.append("MQTT_PORT")
if not MQTT_TOPIC:
    missing_vars.append("MQTT_TOPIC")
if not MQTT_USERNAME:
    missing_vars.append("MQTT_USERNAME")
if not MQTT_PASSWORD:
    missing_vars.append("MQTT_PASSWORD")

if missing_vars:
    print(f"\n Error: Missing environment variables: {', '.join(missing_vars)}")
    print("\nCreate a .env file in the project root with the following variables:")
    exit(1)

try:
    MQTT_PORT = int(MQTT_PORT)
except ValueError:
    print(f"\nError: MQTT_PORT must be a number, got '{MQTT_PORT}'\n")
    exit(1)


latest_gps_data = None
gps_data_timestamp = None
mqtt_data_lock = threading.Lock()

# Initialize Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def create_saref_message(gps_data, timestamp):

    car_lat = float(gps_data.get("latitude", 0))
    car_lon = float(gps_data.get("longitude", 0))

    # Simulate car movement and temperature: oscillate every 30 seconds
    cycle_value = (time.time() % 30) / 30  

    if cycle_value < 0.5:
        
        distance_frac = 1 - (cycle_value * 2)  
        base_temp = 22 + (cycle_value * 2 * 6)  
    else:
        
        distance_frac = (cycle_value - 0.5) * 2 
        base_temp = 28 - ((cycle_value - 0.5) * 2 * 6)  

    
    temperature = round(base_temp + random.uniform(-1, 1), 2)

    # Convert distance to lat/long offsets 
    lat_offset = (distance_frac * 5) / 111  
    lon_offset = (distance_frac * 5) / 111

    car_lat += lat_offset
    car_lon += lon_offset

    saref_message = {
        "@context": {
            "saref": "https://saref.etsi.org/core/v4.1.1/",
            "saref-core": "https://saref.etsi.org/core/",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/"
        },
        "@id": f"urn:message:location:car:{timestamp}",
        "@type": "saref:Message",
        "dcterms:issued": timestamp,
        "saref:hasMeasurement": [
            {
                "@id": f"urn:measurement:location:car:{timestamp}",
                "@type": "saref:Measurement",
                "saref:hasProperty": {
                    "@type": "saref:LocationProperty",
                    "geo:lat": {
                        "@type": "xsd:float",
                        "@value": car_lat
                    },
                    "geo:long": {
                        "@type": "xsd:float",
                        "@value": car_lon
                    }
                }
            },
            {
                "@id": f"urn:measurement:temperature:car:{timestamp}",
                "@type": "saref:Measurement",
                "saref:hasProperty": {
                    "@type": "saref:TemperatureProperty",
                    "saref:hasValue": {
                        "@type": "xsd:float",
                        "@value": temperature
                    },
                    "saref:hasUnit": "saref:Celsius"
                }
            }
        ],
        "saref:forDevice": {
            "@id": "urn:device:car:gps-tracker",
            "@type": "saref:Device",
            "rdfs:label": "Car GPS Tracker",
            "dcterms:identifier": "HTIT_51"
        },
        "saref:targetDevice": [
            {
                "@id": "urn:device:home:smart-tv",
                "@type": "saref:Device",
                "rdfs:label": "Smart TV"
            },
            {
                "@id": "urn:device:home:smart-hvac",
                "@type": "saref:Device",
                "rdfs:label": "Smart HVAC"
            },
            {
                "@id": "urn:device:home:smart-barbecue",
                "@type": "saref:Device",
                "rdfs:label": "Smart Barbecue"
            }
        ]
    }

    return saref_message

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    global latest_gps_data, gps_data_timestamp
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        with mqtt_data_lock:
            latest_gps_data = payload
            gps_data_timestamp = datetime.now().isoformat()  # Capture timestamp when message arrives
        print(f"Received GPS data from MQTT at {gps_data_timestamp}: {payload.get('latitude')}, {payload.get('longitude')}")
    except json.JSONDecodeError:
        print(f"Received non-JSON message on topic '{msg.topic}'")

def publish_saref_messages():

    print("Starting Multi-Topic SAREF publisher...")
    print(f"Publishing to 3 topics:")
    print(f"  1. {KAFKA_TOPIC_1}")
    print(f"  2. {KAFKA_TOPIC_2}")
    print(f"  3. {KAFKA_TOPIC_3}\n")
    last_published_timestamp = None

    while True:
        with mqtt_data_lock:
            if latest_gps_data is not None and gps_data_timestamp != last_published_timestamp:
                
                saref_msg = create_saref_message(latest_gps_data, gps_data_timestamp)

                kafka_producer.send(KAFKA_TOPIC_1, value=saref_msg)
                kafka_producer.send(KAFKA_TOPIC_2, value=saref_msg)
                kafka_producer.send(KAFKA_TOPIC_3, value=saref_msg)
                last_published_timestamp = gps_data_timestamp

                temp_value = saref_msg['saref:hasMeasurement'][1]['saref:hasProperty']['saref:hasValue']['@value']
                print(f"\nPublished SAREF message to all 3 topics:")
                print(f"  Temperature: {temp_value}°C")
                print(f"  Latitude: {latest_gps_data.get('latitude')}, Longitude: {latest_gps_data.get('longitude')}")
                print(f"  Topics: {KAFKA_TOPIC_1}, {KAFKA_TOPIC_2}, {KAFKA_TOPIC_3}")
                print("-" * 60)
            elif latest_gps_data is not None and gps_data_timestamp == last_published_timestamp:
                print(f"[Waiting for new GPS data... Last update: {gps_data_timestamp}]")

        time.sleep(2)

# Create MQTT client
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker
print("Connecting to MQTT broker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)


mqtt_thread = threading.Thread(target=mqtt_client.loop_forever, daemon=True)
mqtt_thread.start()


time.sleep(2)

try:
    publish_saref_messages()
except KeyboardInterrupt:
    print("\nShutting down...")
    mqtt_client.disconnect()
    kafka_producer.close()
