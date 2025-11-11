from kafka import KafkaConsumer
import json

def main():
    print("👂 Starting Kafka Consumer...")
    
    consumer = KafkaConsumer(
        'employee',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Read from beginning
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='employee-consumer-group'
    )
    
    print("✅ Consumer started. Waiting for messages...")
    print("Press Ctrl+C to stop\n")
    
    try:
        for message in consumer:
            print(f"📨 Received message:")
            print(f"   Offset: {message.offset}")
            print(f"   Partition: {message.partition}")
            print(f"   Data: {message.value}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()