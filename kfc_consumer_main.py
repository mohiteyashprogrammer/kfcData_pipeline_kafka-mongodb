import os
from dotenv import load_dotenv
load_dotenv()
from src.kafka_manager.consumer_kfc import KafkaConsumerToMongoDB


if __name__ == '__main__':

    # Load environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
    KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
    SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
    
    # Create KafkaManager instance
    KAFKA_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "sasl.username": KAFKA_SASL_USERNAME,
        "sasl.password": KAFKA_SASL_PASSWORD,
        'group.id': 'mongo_consumer_group_6',
        'auto.offset.reset': 'earliest'
    }

    SCHEMA_REGISTRY_CONFIG = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO
    }

    MONGO_URI = os.getenv("MONGODB_CONNECTION_STRING")
    
    
    # Initialize KafkaManager
    kafka_manager = KafkaConsumerToMongoDB(
        kafka_config=KAFKA_CONFIG,
        schema_registry_config=SCHEMA_REGISTRY_CONFIG,
        topic="kfc_data_dev",
        mongo_uri=MONGO_URI,
        db_name="KFC",
        collection_name="kfc_order_details"
    )

    # Start consuming and storing Kafka messages in MongoDB
    kafka_manager.start_consuming()

    # Stop consuming after processing all messages
    kafka_manager.stop_consuming()

