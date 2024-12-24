import paho.mqtt.client as mqtt
import requests
import random
import time
import psycopg2
from flask import Flask, jsonify
import threading
from pyspark.sql import SparkSession

# Constants
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/electricity_monitoring"
UBIDOTS_TOKEN = "BBUS-z6uwckpQould16rieRIz4mqOYByOrt"
UBIDOTS_DEVICE_LABEL = "electricity_monitor"
UBIDOTS_URL = f"https://industrial.api.ubidots.com/api/v1.6/devices/{UBIDOTS_DEVICE_LABEL}/"
DB_CONNECTION = "dbname='electricity_monitor' user='postgres' password='password' host='localhost' port='5432'"

# Flask app for API Gateway
app = Flask(__name__)

# PostgreSQL Setup
class DatabaseClient:
    def __init__(self, connection_str):
        self.connection_str = connection_str
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.connection_str)
        self.cursor = self.conn.cursor()

    def insert_data(self, voltage, current, energy_consumption):
        query = """
        INSERT INTO electricity_data (voltage, current, energy_consumption)
        VALUES (%s, %s, %s);
        """
        self.cursor.execute(query, (voltage, current, energy_consumption))
        self.conn.commit()

    def fetch_all_data(self):
        query = "SELECT voltage, current, energy_consumption FROM electricity_data;"
        self.cursor.execute(query)
        return self.cursor.fetchall()

# Apache Spark Setup
spark = SparkSession.builder \
    .appName("ElectricityDataProcessing") \
    .config("spark.jars", r"C:\JDBC\postgresql-42.7.4.jar") \
    .config("spark.driver.extraClassPath", r"C:\JDBC\postgresql-42.7.4.jar") \
    .getOrCreate()

def compute_averages_from_spark():
    # Fetch all data from PostgreSQL
    df = spark.read.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/electricity_monitor",
        dbtable="electricity_data",
        user="postgres",
        password="password",
        driver="org.postgresql.Driver"
    ).load()

    # Compute averages
    avg_data = df.groupBy().avg("voltage", "current", "energy_consumption").collect()[0]
    return avg_data

# CoAP Server (Placeholder)
class CoAPServer:
    pass  # Placeholder if using CoAP server

# Constrained Device Class (Sensor)
class ConstrainedDevice:
    def __init__(self, name):
        self.name = name

    def generate_data(self):
        voltage = random.uniform(220.0, 240.0)
        current = random.uniform(10.0, 20.0)
        energy_consumption = random.uniform(1.0, 5.0)
        return {
            "voltage": round(voltage, 2),
            "current": round(current, 2),
            "energy_consumption": round(energy_consumption, 2)
        }

# Gateway Class
class Gateway:
    def __init__(self):
        self.status = "OK"

    def process_data(self, data):
        if data["voltage"] < 225.0 or data["current"] > 18.0:
            self.status = "ALERT"
        else:
            self.status = "OK"
        data["status"] = self.status
        return data

# MQTT Client Class
class MQTTClient:
    def __init__(self, broker, port, topic):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client = mqtt.Client()

    def connect(self):
        self.client.connect(self.broker, self.port)

    def publish(self, message):
        self.client.publish(self.topic, message)

# Ubidots Client Class
class UbidotsClient:
    def __init__(self, token, url):
        self.token = token
        self.url = url

    def send_data(self, data):
        headers = {
            "X-Auth-Token": self.token,
            "Content-Type": "application/json"
        }
        response = requests.post(self.url, headers=headers, json=data)
        return response.status_code, response.text

# Flask RESTful API
@app.route('/api/data', methods=['GET'])
def get_data():
    avg_data = db_client.fetch_all_data()
    return jsonify({
        "data": avg_data
    })

@app.route('/api/average', methods=['GET'])
def get_average():
    avg_data = compute_averages_from_spark()
    return jsonify({
        "average_voltage": avg_data['avg(voltage)'],
        "average_current": avg_data['avg(current)'],
        "average_energy_consumption": avg_data['avg(energy_consumption)']
    })

# Main Application
def start_flask():
    app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    sensor = ConstrainedDevice("ElectricitySensor")
    gateway = Gateway()
    mqtt_client = MQTTClient(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC)
    ubidots_client = UbidotsClient(UBIDOTS_TOKEN, UBIDOTS_URL)
    db_client = DatabaseClient(DB_CONNECTION)

    db_client.connect()
    mqtt_client.connect()

    # Start Flask API in a separate thread
    flask_thread = threading.Thread(target=start_flask)
    flask_thread.daemon = True
    flask_thread.start()

    try:
        while True:
            # Generate sensor data
            sensor_data = sensor.generate_data()

            # Process data through gateway
            processed_data = gateway.process_data(sensor_data)

            # Save to PostgreSQL
            db_client.insert_data(
                processed_data['voltage'],
                processed_data['current'],
                processed_data['energy_consumption']
            )

            # Publish data via MQTT
            mqtt_message = f"Voltage: {processed_data['voltage']}V, Current: {processed_data['current']}A, Energy: {processed_data['energy_consumption']}kWh, Status: {processed_data['status']}"
            mqtt_client.publish(mqtt_message)
            print(f"Published to MQTT: {mqtt_message}")

            # Send data to Ubidots
            ubidots_payload = {
                "voltage": processed_data["voltage"],
                "current": processed_data["current"],
                "energy_consumption": processed_data["energy_consumption"],
                "status": 1 if processed_data["status"] == "OK" else 0
            }
            status_code, response_text = ubidots_client.send_data(ubidots_payload)
            print(f"Sent to Ubidots: {status_code}, {response_text}")

            # Simulate delay
            time.sleep(5)

    except KeyboardInterrupt:
        print("Program stopped by user.")
