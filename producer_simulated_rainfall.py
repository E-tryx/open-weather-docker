from confluent_kafka import Producer
import requests
import json
import time
import logging
from dotenv import load_dotenv
import os
from datetime import datetime
import random

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITIES = [
    {"name": "nairobi", "country": "KE", "lat": -1.286389, "lon": 36.817223},
    {"name": "eldoret", "country": "KE", "lat": 0.5143, "lon": 35.2698},
    {"name": "annex_eldoret", "country": "KE", "lat": 0.5200, "lon": 35.2750},
    {"name": "biretwo", "country": "KE", "lat": -1.3000, "lon": 36.8500},
    {"name": "naiberi", "country": "KE", "lat": -0.4667, "lon": 36.9667}
]

# Rainfall patterns by month for Kenya (approximate mm)
RAINFALL_PATTERNS = {
    1: 50,   # January - Dry
    2: 40,   # February - Dry  
    3: 80,   # March - Long rains start
    4: 200,  # April - Peak long rains
    5: 150,  # May - Long rains
    6: 50,   # June - Dry
    7: 30,   # July - Dry
    8: 40,   # August - Dry
    9: 50,   # September - Dry
    10: 100, # October - Short rains start
    11: 200, # November - Peak short rains
    12: 100  # December - Short rains
}

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'weather-producer'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def get_weather_data_with_simulated_rain(lat, lon, city, country):
    """Fetch real weather data but simulate realistic rainfall"""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        current_month = datetime.now().month
        base_rainfall = RAINFALL_PATTERNS.get(current_month, 50)
        
        # Simulate rainfall based on weather condition and season
        weather_main = data['weather'][0]['main'].lower()
        if 'rain' in weather_main:
            simulated_rainfall = random.uniform(0.5, 5.0)  # Light to moderate rain
        elif 'drizzle' in weather_main:
            simulated_rainfall = random.uniform(0.1, 0.5)  # Light drizzle
        else:
            # Random chance of rain based on season
            rain_chance = base_rainfall / 200  # Convert to probability
            if random.random() < rain_chance:
                simulated_rainfall = random.uniform(0.1, 2.0)
            else:
                simulated_rainfall = 0
        
        weather_data = {
            'city': city,
            'country': country,
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'wind_speed': data['wind']['speed'],
            'rainfall_1h': simulated_rainfall,
            'rainfall_3h': simulated_rainfall * 3,  # Estimate 3h from 1h
            'timestamp': data['dt'],
            'month': current_month,
            'year': datetime.now().year,
            'data_type': 'simulated_rainfall'
        }
        
        return weather_data
    except Exception as e:
        logger.error(f"Error fetching weather data for {city}: {e}")
        return None

def produce_weather_data():
    """Produce weather data with simulated rainfall to Kafka topics"""
    for city_info in CITIES:
        weather_data = get_weather_data_with_simulated_rain(
            city_info['lat'], 
            city_info['lon'], 
            city_info['name'], 
            city_info['country']
        )
        
        if weather_data:
            # Produce to Kafka
            producer.produce(
                topic=city_info['name'],
                key=city_info['name'],
                value=json.dumps(weather_data),
                callback=delivery_report
            )
            producer.poll(0)
            
            logger.info(f"Weather data produced for {city_info['name']}: Rainfall = {weather_data['rainfall_1h']:.2f}mm")
        else:
            logger.error(f"Failed to fetch weather data for {city_info['name']}")
        
        time.sleep(1)
    
    producer.flush()

if __name__ == "__main__":
    while True:
        produce_weather_data()
        logger.info("Completed one round of data collection. Waiting 1 minutes...")
        time.sleep(60)