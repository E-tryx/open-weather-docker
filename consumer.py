# consumer_fixed.py
from confluent_kafka import Consumer, KafkaError, KafkaException
import psycopg2
import json
import logging
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import sys

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'sslmode': os.getenv('POSTGRES_SSL_MODE')
}

def add_missing_columns(conn):
    """Add missing columns to existing table"""
    alter_queries = [
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS rainfall_1h DECIMAL(5,2) DEFAULT 0",
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS rainfall_3h DECIMAL(5,2) DEFAULT 0",
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS rain_probability DECIMAL(5,2) DEFAULT 0",
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS month INTEGER",
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS year INTEGER",
        "ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS data_type VARCHAR(50) DEFAULT 'current'",
    ]
    
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_month ON weather_data(month)",
        "CREATE INDEX IF NOT EXISTS idx_city_month ON weather_data(city, month)",
        "CREATE INDEX IF NOT EXISTS idx_data_type ON weather_data(data_type)",
        "CREATE INDEX IF NOT EXISTS idx_year ON weather_data(year)",
    ]
    
    try:
        with conn.cursor() as cursor:
            # Add missing columns
            for query in alter_queries:
                cursor.execute(query)
            
            # Create indexes
            for query in index_queries:
                cursor.execute(query)
                
        conn.commit()
        logger.info("Successfully added missing columns to weather_data table")
        return True
    except Exception as e:
        logger.error(f"Error adding columns: {e}")
        conn.rollback()
        return False

def create_table_if_not_exists(conn):
    """Create the weather data table if it doesn't exist with rainfall columns"""
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
        rainfall_1h DECIMAL(5,2) DEFAULT 0,
        rainfall_3h DECIMAL(5,2) DEFAULT 0,
        rain_probability DECIMAL(5,2) DEFAULT 0,
        month INTEGER,
        year INTEGER,
        data_type VARCHAR(50) DEFAULT 'current',
        timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city)",
        "CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_data(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_month ON weather_data(month)",
        "CREATE INDEX IF NOT EXISTS idx_city_month ON weather_data(city, month)",
        "CREATE INDEX IF NOT EXISTS idx_data_type ON weather_data(data_type)",
        "CREATE INDEX IF NOT EXISTS idx_year ON weather_data(year)",
    ]
    
    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_query)
            
            # Create indexes
            for query in index_queries:
                cursor.execute(query)
                
        conn.commit()
        logger.info("Weather table created or already exists")
        
        # Always try to add missing columns in case of schema changes
        add_missing_columns(conn)
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        # Try to add missing columns even if table creation fails
        add_missing_columns(conn)

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

def convert_timestamp(timestamp_int):
    """Convert Unix timestamp to PostgreSQL timestamp"""
    try:
        # Convert integer timestamp to datetime object
        return datetime.fromtimestamp(timestamp_int)
    except (ValueError, TypeError):
        # If conversion fails, use current time
        return datetime.now()

def insert_weather_data_batch(conn, data_list):
    """Insert multiple weather records in batch"""
    if not data_list:
        return True
        
    insert_query = """
    INSERT INTO weather_data 
    (city, country, temperature, feels_like, humidity, pressure, 
     weather_main, weather_description, wind_speed, rainfall_1h, 
     rainfall_3h, rain_probability, month, year, data_type, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        batch_data = []
        for data in data_list:
            # Convert timestamp for each record
            timestamp_value = convert_timestamp(data['timestamp'])
            
            batch_data.append((
                data['city'], data['country'], data['temperature'],
                data['feels_like'], data['humidity'], data['pressure'],
                data['weather_main'], data['weather_description'],
                data['wind_speed'], data.get('rainfall_1h', 0),
                data.get('rainfall_3h', 0), data.get('rain_probability', 0),
                data.get('month', datetime.now().month),
                data.get('year', datetime.now().year),
                data.get('data_type', 'current'),
                timestamp_value  # Use converted timestamp
            ))
        
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, batch_data)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error inserting batch data: {e}")
        conn.rollback()
        return False

def get_available_topics(consumer, requested_topics):
    """Check which topics are actually available"""
    try:
        # Get cluster metadata to see available topics
        metadata = consumer.list_topics(timeout=10)
        available_topics = []
        unavailable_topics = []
        
        for topic in requested_topics:
            if topic in metadata.topics:
                available_topics.append(topic)
            else:
                unavailable_topics.append(topic)
        
        if unavailable_topics:
            logger.warning(f"Topics not available: {unavailable_topics}")
        
        if available_topics:
            logger.info(f"Subscribing to available topics: {available_topics}")
        else:
            logger.error("No requested topics are available!")
            
        return available_topics
        
    except Exception as e:
        logger.error(f"Error checking topic availability: {e}")
        return requested_topics  # Fallback to original topics

def main():
    """Main function to run the consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Weather Data Consumer')
    parser.add_argument('--mode', choices=['current', 'historical', 'both', 'auto'], 
                       default='auto', help='Consumption mode')
    parser.add_argument('--batch-size', type=int, default=100, 
                       help='Batch size for database inserts')
    parser.add_argument('--batch-timeout', type=int, default=10, 
                       help='Batch timeout in seconds')
    
    args = parser.parse_args()
    
    # Initialize PostgreSQL connection
    conn = get_postgres_connection()
    if not conn:
        logger.error("Failed to connect to PostgreSQL. Exiting.")
        sys.exit(1)
    
    # Define topics based on mode
    current_topics = ["biretwo", "eldoret", "naiberi", "annex_eldoret", "nairobi"]
    historical_topics = [f"{city}_historical" for city in current_topics]
    
    if args.mode == 'current':
        topics = current_topics
    elif args.mode == 'historical':
        topics = historical_topics
    elif args.mode == 'both':
        topics = current_topics + historical_topics
    else:  # auto - start with current, dynamically add historical later
        topics = current_topics
    
    # Kafka configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9094',
        'group.id': f'weather-consumer-{args.mode}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 60000,
        'heartbeat.interval.ms': 20000
    }
    
    consumer = Consumer(consumer_config)
    
    # Check topic availability and subscribe only to available ones
    available_topics = get_available_topics(consumer, topics)
    
    if not available_topics:
        logger.error("No topics available to subscribe to. Exiting.")
        consumer.close()
        conn.close()
        sys.exit(1)
    
    consumer.subscribe(available_topics)
    
    logger.info(f"Consumer started in {args.mode} mode, subscribed to {len(available_topics)} topics")
    
    # Batch processing configuration
    batch_buffer = []
    last_batch_time = time.time()
    processed_count = 0
    last_topic_check = time.time()
    topic_check_interval = 30  # Check for new topics every 30 seconds
    
    try:
        while True:
            msg = consumer.poll(1.0)
            current_time = time.time()
            
            # Check for new topics periodically (for auto mode)
            if args.mode == 'auto' and current_time - last_topic_check > topic_check_interval:
                new_available_topics = get_available_topics(consumer, current_topics + historical_topics)
                current_subscribed = set(available_topics)
                new_available_set = set(new_available_topics)
                
                if new_available_set != current_subscribed:
                    new_topics = new_available_set - current_subscribed
                    if new_topics:
                        logger.info(f"New topics detected: {list(new_topics)}")
                        consumer.subscribe(new_available_topics)
                        available_topics = new_available_topics
                
                last_topic_check = current_time
            
            # Process batch if timeout reached or batch size met
            if (batch_buffer and 
                (len(batch_buffer) >= args.batch_size or 
                 current_time - last_batch_time >= args.batch_timeout)):
                
                if insert_weather_data_batch(conn, batch_buffer):
                    processed_count += len(batch_buffer)
                    logger.info(f"Inserted batch of {len(batch_buffer)} records "
                               f"(Total: {processed_count})")
                else:
                    logger.error(f"Failed to insert batch of {len(batch_buffer)} records")
                    # Try to reconnect if connection is lost
                    conn = get_postgres_connection()
                    if not conn:
                        logger.error("Failed to reconnect to PostgreSQL")
                        break
                
                batch_buffer.clear()
                last_batch_time = current_time
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Topic error: {msg.error()}")
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                batch_buffer.append(value)
                
                logger.debug(f"Added to batch from {msg.topic()}: "
                           f"City={value['city']}, "
                           f"Temp={value.get('temperature', 'N/A')}°C, "
                           f"Rainfall={value.get('rainfall_1h', 0)}mm")
                           
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for message from {msg.topic()}: {e}")
            except Exception as e:
                logger.error(f"Error processing message from {msg.topic()}: {e}")
    
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        # Process any remaining messages in buffer before exit
        if batch_buffer and conn:
            if insert_weather_data_batch(conn, batch_buffer):
                logger.info(f"Inserted final batch of {len(batch_buffer)} records before shutdown")
        
        consumer.close()
        if conn:
            conn.close()
        logger.info("Consumer shutdown complete")

if __name__ == "__main__":
    main()