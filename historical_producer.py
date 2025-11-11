# historical_producer.py
from confluent_kafka import Producer
import json
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_historical_weather(city, lat, lon, start_date, end_date):
    """Generate realistic historical weather data"""
    current_date = start_date
    historical_data = []
    
    # Kenya climate patterns
    seasonal_patterns = {
        'dry_season': [1, 2, 6, 7, 8, 9],  # Jan-Feb, Jun-Sep
        'long_rains': [3, 4, 5],           # Mar-May
        'short_rains': [10, 11, 12]        # Oct-Dec
    }
    
    while current_date <= end_date:
        month = current_date.month
        year = current_date.year
        
        # Base temperatures with slight warming trend
        base_temp = 22 + (year - 2014) * 0.03  # Simulate climate change
        
        if month in seasonal_patterns['dry_season']:
            temp = base_temp + random.uniform(2, 6)
            rainfall = random.uniform(0, 20)
        elif month in seasonal_patterns['long_rains']:
            temp = base_temp + random.uniform(-1, 2)
            rainfall = random.uniform(30, 150)
        else:  # short_rains
            temp = base_temp + random.uniform(0, 3)
            rainfall = random.uniform(20, 100)
        
        weather_record = {
            'city': city,
            'country': 'KE',
            'temperature': round(temp, 2),
            'feels_like': round(temp + random.uniform(-2, 2), 2),
            'humidity': random.randint(40, 90),
            'pressure': random.randint(1010, 1020),
            'wind_speed': round(random.uniform(1, 8), 2),
            'rainfall_1h': round(rainfall / 24, 2),  # Convert daily to hourly
            'rainfall_3h': round(rainfall / 8, 2),
            'rain_probability': random.randint(0, 100),
            'timestamp': int(current_date.timestamp()),
            'month': month,
            'year': year,
            'data_type': 'historical_simulated'
        }
        
        historical_data.append(weather_record)
        current_date += timedelta(days=1)
    
    return historical_data

# Produce historical data
producer = Producer({'bootstrap.servers': 'localhost:9094'})

cities = [
    {"name": "nairobi", "lat": -1.292, "lon": 36.821},
    {"name": "eldoret", "lat": 0.514, "lon": 35.269},
]

start_date = datetime(2014, 1, 1)
end_date = datetime.now()

for city in cities:
    historical_data = generate_historical_weather(
        city['name'], city['lat'], city['lon'], start_date, end_date
    )
    
    for record in historical_data:
        producer.produce(
            topic=f"{city['name']}_historical",
            value=json.dumps(record)
        )
    
    producer.flush()
    logger.info(f"Produced {len(historical_data)} historical records for {city['name']}")