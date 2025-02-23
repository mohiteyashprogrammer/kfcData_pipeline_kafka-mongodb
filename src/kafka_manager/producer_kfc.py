import os
import threading
import time
import mysql.connector
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

class KafkaManager:
    def __init__(
        self,
        kafka_config: dict,
        schema_registry_config: dict,
        topic: str,
        mysql_config: dict,
        subject_name: str = None,
        send_interval: int = 2,
        use_threading: bool = False,
        preprocessing_func: callable = None
    ):
        self.kafka_config = kafka_config
        self.schema_registry_config = schema_registry_config
        self.topic = topic
        self.mysql_config = mysql_config
        self.subject_name = subject_name if subject_name else f"{topic}-value"
        self.send_interval = send_interval
        self.use_threading = use_threading
        self.preprocessing_func = preprocessing_func

        # Initialize components
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        self.producer = self._create_producer()
        self._running = False

    def _create_producer(self) -> SerializingProducer:
        """Create and configure the Avro SerializingProducer"""
        # Fetch latest schema version
        schema_str = self.schema_registry_client.get_latest_version(
            self.subject_name
        ).schema.schema_str

        # Create serializers
        key_serializer = StringSerializer("utf-8")
        avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema_str
        )

        return SerializingProducer({
            **self.kafka_config,
            "key.serializer": key_serializer,
            "value.serializer": avro_serializer
        })

    @staticmethod
    def delivery_report(err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def _fetch_incremental_data(self, last_order_date: str):
        """
        Fetch incremental data from MySQL based on the last_order_date.
        
        Args:
            last_order_date (str): Last processed order date in 'YYYY-MM-DD HH:MM:SS' format.
        
        Returns:
            list: List of formatted records according to Avro schema.
        """
        connection = mysql.connector.connect(**self.mysql_config)
        cursor = connection.cursor(dictionary=True)

        try:
            print(f"Fetching data with orders after: {last_order_date}")

            query = """
                SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.phone,
                c.email,
                c.address,
                o.order_id,
                o.order_date,
                o.total_amount,
                oi.order_item_id,
                oi.quantity,
                oi.subtotal,
                m.menu_id,
                m.item_name,
                m.category,
                m.price
            FROM ym_customers c
            JOIN ym_orders o ON c.customer_id = o.customer_id
            JOIN ym_order_items oi ON o.order_id = oi.order_id
            JOIN ym_menu m ON oi.menu_id = m.menu_id;
            WHERE o.order_date > %s
            ORDER BY o.order_date ASC
            """

            cursor.execute(query, (last_order_date,))
            rows = cursor.fetchall()

            if not rows:
                print("No new records found.")
                return []

            formatted_rows = []
            for row in rows:
                # Handle NULL values and format data types
                formatted_row = {
                    "customer_id": row["customer_id"],
                    "first_name": row["first_name"] if row["first_name"] is not None else None,
                    "last_name": row["last_name"] if row["last_name"] is not None else None,
                    "phone": row["phone"] if row["phone"] is not None else None,
                    "email": row["email"] if row["email"] is not None else None,
                    "address": row["address"] if row["address"] is not None else None,
                    "order_id": row["order_id"],
                    "order_date": row["order_date"].strftime('%Y-%m-%d %H:%M:%S') if row["order_date"] is not None else None,
                    "total_amount": float(row["total_amount"]) if row["total_amount"] is not None else None,
                    "order_item_id": row["order_item_id"],
                    "quantity": row["quantity"],
                    "subtotal": float(row["subtotal"]) if row["subtotal"] is not None else None,
                    "menu_id": row["menu_id"],
                    "item_name": row["item_name"] if row["item_name"] is not None else None,
                    "category": row["category"] if row["category"] is not None else None,
                    "price": float(row["price"]) if row["price"] is not None else None
                }
                formatted_rows.append(formatted_row)

            return formatted_rows

        except Exception as e:
            print(f"Error processing data: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def _read_last_timestamp(self) -> str:
        """Read the last processed order date from a file"""
        try:
            with open("last_order_date.txt", "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return "1970-01-01 00:00:00"  #Default timestamp

    def _update_last_timestamp(self, new_order_date: str):
        """Update the last processed order date in a file"""
        with open("last_order_date.txt", "w") as f:
            f.write(new_order_date)

    def _process_and_send(self):
        """Internal method to fetch data and send messages"""
        try:
            while self._running:
                last_order_date = self._read_last_timestamp()
                rows = self._fetch_incremental_data(last_order_date)

                if not rows:
                    print("No more data found. Stopping producer.")
                    self._running = False
                    break

                for row in rows:
                    if not self._running:
                        break

                    # Apply preprocessing if provided
                    record = row
                    if self.preprocessing_func:
                        record = self.preprocessing_func(record)

                    # Produce message with order_item_id as key
                    self.producer.produce(
                        topic=self.topic,
                        key=str(row["order_item_id"]),
                        value=record,
                        on_delivery=self.delivery_report
                    )

                    # Maintain 2-second delay between individual messages
                    self.producer.poll(0)
                    time.sleep(self.send_interval)  # 2-second per-record delay

                # Update timestamp after each batch
                last_order_date_in_batch = rows[-1]["order_date"]
                if last_order_date_in_batch:
                    self._update_last_timestamp(last_order_date_in_batch)

                # Immediate check for next batch without additional delay

            # Flush remaining messages
            self.producer.flush()
            print("\nAll messages successfully published")

        except Exception as e:
            print(f"Error processing messages: {str(e)}")
        finally:
            self._running = False

    def send_messages(self):
        """Start message sending process"""
        if self._running:
            print("Producer is already running")
            return

        self._running = True

        if self.use_threading:
            producer_thread = threading.Thread(target=self._process_and_send)
            producer_thread.start()
        else:
            self._process_and_send()

    def stop(self):
        """Gracefully stop message sending"""
        self._running = False
        self.producer.flush()