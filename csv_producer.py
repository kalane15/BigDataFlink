import json
import os
import time
import csv
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVHandler(FileSystemEventHandler):
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.processed_files = set()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            logger.info(f"New CSV file detected: {event.src_path}")
            time.sleep(0.5)
            self.process_csv_file(event.src_path)

    def process_csv_file(self, filepath):
        if filepath in self.processed_files:
            return

        try:
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)

                for row_num, row in enumerate(csv_reader, 1):
                    json_message = json.dumps(row, ensure_ascii=False)

                    self.producer.send(self.topic, value=json_message.encode('utf-8'))
                    logger.info(f"Sent row {row_num} from {os.path.basename(filepath)}: {json_message}")

                self.producer.flush()
                self.processed_files.add(filepath)
                logger.info(f"Completed processing {filepath}")

        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")


def process_existing_files(csv_dir, handler):
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(csv_dir, filename)
            handler.process_csv_file(filepath)


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'csv-data')
    csv_path = os.getenv('CSV_PATH', '/data')

    logger.info(f"Connecting to Kafka at {bootstrap_servers}")
    logger.info(f"Using topic: {topic}")
    logger.info(f"Watching CSV directory: {csv_path}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: x,
            acks='all',
            retries=3
        )
        logger.info("Successfully connected to Kafka")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

    handler = CSVHandler(producer, topic)

    logger.info("Checking for existing CSV files...")
    process_existing_files(csv_path, handler)

    observer = Observer()
    observer.schedule(handler, csv_path, recursive=False)
    observer.start()

    logger.info(f"Watching directory {csv_path} for new CSV files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
    producer.close()


if __name__ == "__main__":
    time.sleep(10)
    main()
