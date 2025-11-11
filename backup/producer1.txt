from kafka import KafkaProducer 
from data_gen import gen
import json
import time
import sys

try:
    print("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 0, 2)  # Explicitly set API version
    )
    
    print("✅ Kafka producer created successfully!")
    
    print("📤 Sending employee records...")
    for i, record in enumerate(gen(4)):
        try:
            future = producer.send("employee", record)
            # Get the result to ensure message was sent
            metadata = future.get(timeout=10)
            print(f"✅ Sent record {i+1}: {record}")
            print(f"   Topic: {metadata.topic}, Partition: {metadata.partition}, Offset: {metadata.offset}")
        except Exception as e:
            print(f"❌ Failed to send record {i+1}: {e}")
            continue
        
        time.sleep(1)
    
    producer.flush()
    print("🎉 All messages sent successfully!")
    
except Exception as e:
    print(f"🚨 Error: {e}")
    sys.exit(1)
finally:
    if 'producer' in locals():
        producer.close()