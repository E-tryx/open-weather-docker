# consumer_confluent.py
from confluent_kafka import Consumer, KafkaError
import psycopg2
import json
import logging
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'sslmode': os.getenv('POSTGRES_SSL_MODE')
}

def create_table_if_not_exists(conn):
    """Create the weather data table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100) NOT NULL,
        country VARCHAR(10) NOT NULL,
        temperature DECIMAL(5,2),
        feels_like DECIMAL(5,2),
        humidity INTEGER,
        pressure INTEGER,
        weather_main VARCHAR(100),
        weather_description VARCHAR(100),
        wind_speed DECIMAL(5,2),
        timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city);
    CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_data(timestamp);
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        logger.info("Weather table created or already exists")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()

def get_postgres_connection(max_retries=3, retry_delay=5):
    """Establish connection to Aiven PostgreSQL with retry logic"""
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            create_table_if_not_exists(conn)
            logger.info("Successfully connected to Aiven PostgreSQL")
            return conn
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    logger.error("All connection attempts failed")
    return None

def insert_weather_data(conn, data):
    """Insert weather data into PostgreSQL"""
    insert_query = """
    INSERT INTO weather_data 
    (city, country, temperature, feels_like, humidity, pressure, 
     weather_main, weather_description, wind_speed, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, (
                data['city'],
                data['country'],
                data['temperature'],
                data['feels_like'],
                data['humidity'],
                data['pressure'],
                data['weather_main'],
                data['weather_description'],
                data['wind_speed'],
                data['timestamp']
            ))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
        return False

def insert_weather_data_batch(conn, data_list):
    """Insert multiple weather records in batch"""
    insert_query = """
    INSERT INTO weather_data 
    (city, country, temperature, feels_like, humidity, pressure, 
     weather_main, weather_description, wind_speed, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, [
                (
                    data['city'], data['country'], data['temperature'],
                    data['feels_like'], data['humidity'], data['pressure'],
                    data['weather_main'], data['weather_description'],
                    data['wind_speed'], data['timestamp']
                ) for data in data_list
            ])
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error inserting batch data: {e}")
        conn.rollback()
        return False

# Initialize PostgreSQL connection
conn = get_postgres_connection()

# Kafka configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'weather-kenya-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_config)
consumer.subscribe(["biretwo", "eldoret", "naiberi", "annex_eldoret", "nairobi"])

# Batch processing configuration
batch_size = 100
batch_buffer = []
batch_timeout = 10  # seconds
last_batch_time = time.time()

try:
    while True:
        msg = consumer.poll(1.0)
        current_time = time.time()
        
        # Process batch if timeout reached or batch size met
        if (batch_buffer and 
            (len(batch_buffer) >= batch_size or 
             current_time - last_batch_time >= batch_timeout)):
            
            if conn and insert_weather_data_batch(conn, batch_buffer):
                logger.info(f"Inserted batch of {len(batch_buffer)} records")
            else:
                logger.error(f"Failed to insert batch of {len(batch_buffer)} records")
                # Try to reconnect if connection is lost
                conn = get_postgres_connection()
            
            batch_buffer.clear()
            last_batch_time = current_time
        
        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                break
        
        topic = msg.topic()
        value = json.loads(msg.value().decode('utf-8'))

        # Add to batch buffer
        batch_buffer.append(value)
        logger.info(f"Added to batch from {topic}: {value}")

except KeyboardInterrupt:
    # Process any remaining messages in buffer before exit
    if batch_buffer and conn:
        if insert_weather_data_batch(conn, batch_buffer):
            logger.info(f"Inserted final batch of {len(batch_buffer)} records before shutdown")
    logger.info("Consumer stopped by user")
except Exception as e:
    logger.error(f"Consumer error: {e}")
finally:
    consumer.close()
    if conn:
        conn.close()