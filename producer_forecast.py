from confluent_kafka import Producer
import requests
import json
import time
import logging
from dotenv import load_dotenv
import os
from datetime import datetime

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

def get_forecast_data(lat, lon, city, country):
    """Fetch 5-day weather forecast including rain probability"""
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Get the first forecast entry (next 3 hours)
        forecast = data['list'][0]
        
        # Extract rainfall data with probability
        rainfall = forecast.get('rain', {}).get('3h', 0)
        pop = forecast.get('pop', 0)  # Probability of precipitation
        
        weather_data = {
            'city': city,
            'country': country,
            'temperature': forecast['main']['temp'],
            'feels_like': forecast['main']['feels_like'],
            'humidity': forecast['main']['humidity'],
            'pressure': forecast['main']['pressure'],
            'weather_main': forecast['weather'][0]['main'],
            'weather_description': forecast['weather'][0]['description'],
            'wind_speed': forecast['wind']['speed'],
            'rainfall_3h': rainfall,
            'rain_probability': pop * 100,  # Convert to percentage
            'timestamp': forecast['dt'],
            'month': datetime.fromtimestamp(forecast['dt']).month,
            'year': datetime.fromtimestamp(forecast['dt']).year,
            'data_type': 'forecast'
        }
        
        return weather_data
    except Exception as e:
        logger.error(f"Error fetching forecast data for {city}: {e}")
        return None

def produce_weather_data():
    """Produce weather forecast data to Kafka topics"""
    for city_info in CITIES:
        weather_data = get_forecast_data(
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
            
            logger.info(f"Weather data produced for {city_info['name']}: Rainfall = {weather_data['rainfall_3h']}mm, Rain Probability = {weather_data['rain_probability']}%")
        else:
            logger.error(f"Failed to fetch weather data for {city_info['name']}")
        
        time.sleep(1)
    
    producer.flush()

if __name__ == "__main__":
    while True:
        produce_weather_data()
        logger.info("Completed one round of forecast data collection. Waiting 1 hour...")
        time.sleep(3600)  # Wait 1 hour for forecast data