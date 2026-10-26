import json
import time
import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, DATA_PATH, EVENTS_PER_SECOND

COLUMNS = (
    ["unit_number", "time_in_cycles"]
    + [f"op_setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)

def main():
    df = pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(f"Wysyłam {len(df)} rekordów do tematu {KAFKA_TOPIC}")

    for _, row in df.iterrows():
        event = row.to_dict()
        event["event_timestamp"] = int(time.time() * 1000)

        key = str(int(event["unit_number"]))

        producer.send(
            topic=KAFKA_TOPIC,
            key=key,
            value=event,
        )

        print(
            f"engine={event['unit_number']} "
            f"cycle={event['time_in_cycles']} "
            f"sensor_2={event['sensor_2']}"
        )

        time.sleep(1 / EVENTS_PER_SECOND)

    producer.flush()
    producer.close()
    print("Producer zakończony.")


if __name__ == "__main__":
    main()