import pymongo
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

class KafkaConsumerToMongoDB:
    def __init__(self, kafka_config, schema_registry_config, topic, mongo_uri, 
                 db_name, collection_name):
        """
        Initialize Kafka Consumer with direct MongoDB connection.
        
        :param kafka_config: Dictionary with Kafka configuration
        :param schema_registry_config: Dictionary with Schema Registry configuration
        :param topic: Kafka topic to consume from
        :param mongo_uri: MongoDB connection string
        :param db_name: MongoDB database name
        :param collection_name: MongoDB collection name
        """
        self.kafka_config = kafka_config
        self.schema_registry_config = schema_registry_config
        self.topic = topic
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.consumer = self._create_consumer()
        self.running = False
        
        # Initialize MongoDB connection
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        
        # Verify MongoDB connection
        try:
            self.client.admin.command('ping')
            print("MongoDB connection successful")
        except pymongo.errors.ConnectionFailure:
            raise Exception("MongoDB connection failed")

    def _create_consumer(self):
        """Create and configure the Avro DeserializingConsumer"""
        schema_registry_client = SchemaRegistryClient(self.schema_registry_config)
        subject_name = f'{self.topic}-value'
        schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

        return DeserializingConsumer({
            'bootstrap.servers': self.kafka_config['bootstrap.servers'],
            'security.protocol': self.kafka_config['security.protocol'],
            'sasl.mechanisms': self.kafka_config['sasl.mechanisms'],
            'sasl.username': self.kafka_config['sasl.username'],
            'sasl.password': self.kafka_config['sasl.password'],
            'group.id': self.kafka_config['group.id'],
            'auto.offset.reset': self.kafka_config['auto.offset.reset'],
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': AvroDeserializer(schema_registry_client, schema_str)
        })

    def _write_to_mongodb(self, document: dict):
        """Directly write document to MongoDB collection"""
        try:
            result = self.collection.insert_one(document)
            print(f"Inserted document ID: {result.inserted_id}")
            return result.inserted_id
        except pymongo.errors.DuplicateKeyError:
            print(f"Duplicate document key: {document.get('_id')}")
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error: {str(e)}")

    def start_consuming(self):
        """Start consuming messages and writing to MongoDB"""
        self.consumer.subscribe([self.topic])
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Write raw message value to MongoDB
                self._write_to_mongodb(msg.value())
                print(f"Processed message with key: {msg.key()}")

        except KeyboardInterrupt:
            print("Consumption interrupted")
        finally:
            self.stop_consuming()

    def stop_consuming(self):
        """Clean shutdown of consumer and MongoDB connection"""
        self.running = False
        self.consumer.close()
        self.client.close()
        print("Stopped consumer and closed MongoDB connection")


