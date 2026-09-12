import json
import requests
from kafka import KafkaProducer

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Weather API
url = "https://api.open-meteo.com/v1/forecast?latitude=18.5204&longitude=73.8567&current=temperature_2m,relative_humidity_2m"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()

    weather_data = {
        "city": "Pune",
        "temperature": data["current"]["temperature_2m"],
        "humidity": data["current"]["relative_humidity_2m"],
        "timestamp": data["current"]["time"]
    }

    # Send weather data to Kafka
    producer.send("weather-data", value=weather_data)

    # Make sure the message is sent
    producer.flush()

    print("Sent to Kafka:", weather_data)

else:
    print("Failed to fetch weather data")

producer.close()