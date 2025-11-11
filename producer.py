# historical_producer_complete.py
from confluent_kafka import Producer
import json
import logging
from datetime import datetime, timedelta
import random
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your existing data generation functions
from data_gen import biretwo, eldoret, naiberi, annex_eldoret, nairobi

# Kafka configuration
producer_config = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'historical-producer'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_historical_data():
    """Generate 10 years of historical data for all cities"""
    
    cities = {
        "biretwo": {"lat": 0.519, "lon": 35.270},
        "eldoret": {"lat": 0.514, "lon": 35.269},
        "naiberi": {"lat": 0.553, "lon": 35.357},
        "annex_eldoret": {"lat": 0.520, "lon": 35.280},
        "nairobi": {"lat": -1.292, "lon": 36.821}
    }
    
    # Kenya climate patterns
    seasonal_patterns = {
        'dry_season': [1, 2, 6, 7, 8, 9],  # Jan-Feb, Jun-Sep
        'long_rains': [3, 4, 5],           # Mar-May
        'short_rains': [10, 11, 12]        # Oct-Dec
    }
    
    start_date = datetime(2014, 1, 1)
    end_date = datetime.now()
    total_records = 0
    
    for city_name, coords in cities.items():
        logger.info(f"Generating historical data for {city_name}...")
        city_records = 0
        current_date = start_date
        
        while current_date <= end_date:
            month = current_date.month
            year = current_date.year
            
            # Base temperature with seasonal variations and slight warming trend
            base_temp = 22 + (year - 2014) * 0.02  # Climate change effect
            
            if month in seasonal_patterns['dry_season']:
                temp = base_temp + random.uniform(2, 6)
                rainfall = random.uniform(0, 10)
                rain_prob = random.randint(5, 20)
            elif month in seasonal_patterns['long_rains']:
                temp = base_temp + random.uniform(-1, 2)
                rainfall = random.uniform(20, 100)
                rain_prob = random.randint(60, 90)
            else:  # short_rains
                temp = base_temp + random.uniform(0, 3)
                rainfall = random.uniform(10, 60)
                rain_prob = random.randint(40, 80)
            
            # Create historical record
            historical_record = {
                'city': city_name.capitalize(),
                'country': 'KE',
                'temperature': round(temp, 2),
                'feels_like': round(temp + random.uniform(-2, 2), 2),
                'humidity': random.randint(40, 85),
                'pressure': random.randint(1010, 1020),
                'weather_main': random.choice(['Clear', 'Clouds', 'Rain', 'Drizzle']),
                'weather_description': 'historical simulated data',
                'wind_speed': round(random.uniform(1, 12), 2),
                'rainfall_1h': round(rainfall, 2),
                'rainfall_3h': round(rainfall * random.uniform(2, 4), 2),
                'rain_probability': rain_prob,
                'timestamp': int(current_date.timestamp()),
                'month': month,
                'year': year,
                'data_type': 'historical_simulated'
            }
            
            # Produce to Kafka historical topic
            producer.produce(
                topic=f"{city_name}_historical",
                key=city_name,
                value=json.dumps(historical_record),
                callback=delivery_report
            )
            
            city_records += 1
            total_records += 1
            
            # Flush every 500 records to avoid memory issues
            if total_records % 500 == 0:
                producer.flush()
                logger.info(f"Produced {total_records} records so far...")
            
            current_date += timedelta(days=1)
        
        producer.flush()
        logger.info(f"✅ Generated {city_records} historical records for {city_name}")
    
    producer.flush()
    logger.info(f"🎉 Historical data generation complete! Total records: {total_records}")
    return total_records

if __name__ == "__main__":
    logger.info("Starting historical data generation...")
    total_records = generate_historical_data()
    logger.info(f"Successfully generated {total_records} historical weather records")