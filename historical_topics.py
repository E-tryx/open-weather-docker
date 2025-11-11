# create_topics.py
from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_historical_topics():
    # Kafka configuration
    admin_config = {
        'bootstrap.servers': 'localhost:9094',
        'client.id': 'topic-creator'
    }

    admin_client = AdminClient(admin_config)

    # Historical topics to create
    historical_topics = [
        "biretwo_historical",
        "eldoret_historical", 
        "naiberi_historical",
        "annex_eldoret_historical",
        "nairobi_historical"
    ]

    # Create topic objects
    topic_list = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in historical_topics]

    # Create topics
    logger.info("Creating historical topics...")
    fs = admin_client.create_topics(topic_list)

    # Wait for each operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"✅ Topic {topic} created successfully")
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"ℹ️  Topic {topic} already exists")
            else:
                logger.error(f"❌ Failed to create topic {topic}: {e}")

def list_topics():
    """List all existing topics"""
    admin_config = {
        'bootstrap.servers': 'localhost:9094'
    }
    
    admin_client = AdminClient(admin_config)
    
    try:
        metadata = admin_client.list_topics(timeout=10)
        topics = metadata.topics
        logger.info("📋 Existing topics:")
        for topic_name, topic_metadata in topics.items():
            logger.info(f"   - {topic_name} (partitions: {len(topic_metadata.partitions)})")
        return list(topics.keys())
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return []

if __name__ == "__main__":
    print("Creating historical Kafka topics...")
    
    # First, list existing topics
    existing_topics = list_topics()
    
    # Create historical topics
    create_historical_topics()
    
    # List topics again to confirm
    print("\nFinal topic list:")
    list_topics()