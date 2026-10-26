import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cmapss-telemetry")
DATA_PATH = os.getenv("DATA_PATH", "data/train_FD001.txt")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/spark_results")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "5"))