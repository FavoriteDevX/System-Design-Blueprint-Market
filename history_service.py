import json
import psycopg2
from kafka import KafkaConsumer

class HistoryIngestionService:
    def __init__(self):
        # Database Connection
        self.db_conn = psycopg2.connect(
            host="localhost",
            database="ride_history_db",
            user="uber_admin",
            password="super_secure_password"
        )
        self.cursor = self.db_conn.cursor()

        # Kafka Event Consumer Group Setup
        self.consumer = KafkaConsumer(
            'driver-telemetry',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='history-persist-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_ingestion_loop(self):
        print("[Ingestion] Subscribed to Kafka 'driver-telemetry' stream. Waiting for events...")
        try:
            for message in self.consumer:
                event = message.value
                self.persist_to_history(event)
        except KeyboardInterrupt:
            print("[Ingestion] Shutting down connection templates safely.")
        finally:
            self.cursor.close()
            self.db_conn.close()

    def persist_to_history(self, event_data: dict):
        """Asynchronously writes streaming data into PostgreSQL storage."""
        query = """
            INSERT INTO ride_history (driver_id, rider_id, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude, fare, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        # Mocking an order resolution based on location ping data
        record_values = (
            event_data["driver_id"],
            "rider_mock_101",
            event_data["latitude"],
            event_data["longitude"],
            event_data["latitude"] + 0.02, # simulated destination
            event_data["longitude"] + 0.02,
            24.50, # baseline pricing
            "COMPLETED"
        )
        
        self.cursor.execute(query, record_values)
        self.db_conn.commit()
        print(f"[Database] Event persisted securely for Driver: {event_data['driver_id']}")

if __name__ == "__main__":
    ingestor = HistoryIngestionService()
    ingestor.start_ingestion_loop()
