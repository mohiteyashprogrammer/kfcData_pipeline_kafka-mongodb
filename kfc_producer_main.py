import os
from dotenv import load_dotenv
load_dotenv()
from src.kafka_manager.producer_kfc import  KafkaManager


if __name__ == '__main__':

    # Load environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
    KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
    SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")


    #MySQL connection parameters
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

    # Create KafkaManager instance
    KAFKA_CONFIG = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "sasl.username": KAFKA_SASL_USERNAME,
        "sasl.password": KAFKA_SASL_PASSWORD
    }

    SCHEMA_REGISTRY_CONFIG = {
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO
    }

    MYSQL_CONFIG = {
        "host": MYSQL_HOST,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "database": MYSQL_DATABASE
    }


    # Initialize KafkaManager
    kafka_manager = KafkaManager(
        kafka_config= KAFKA_CONFIG,
        schema_registry_config=SCHEMA_REGISTRY_CONFIG,
        topic="kfc_data_dev",
        mysql_config=MYSQL_CONFIG,
        subject_name="kfc_data_dev-value",
        send_interval=1,
        use_threading=False
    )

    # Start sending messages
    kafka_manager.send_messages()

    # Close KafkaManager
    kafka_manager.stop()