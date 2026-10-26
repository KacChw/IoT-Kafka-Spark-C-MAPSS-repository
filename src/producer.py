import json
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, DATA_PATH, EVENTS_PER_SECOND

COLUMNS = (
    ["unit_number", "time_in_cycles"]
    + [f"op_setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)


def main():
    if EVENTS_PER_SECOND <= 0:
        raise ValueError("EVENTS_PER_SECOND musi być większe od zera.")

    df = pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=COLUMNS)
    if df.empty:
        raise ValueError(f"Plik danych jest pusty: {DATA_PATH}")
    if df.isna().any().any():
        raise ValueError(
            f"Plik {DATA_PATH} zawiera brakujące wartości lub nie ma 26 kolumn."
        )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(
        f"Wysyłam {len(df)} rekordów do tematu {KAFKA_TOPIC} "
        f"przez {KAFKA_BOOTSTRAP}"
    )

    try:
        for index, row in enumerate(df.itertuples(index=False, name=None), start=1):
            event = {
                name: (
                    int(value)
                    if name in ("unit_number", "time_in_cycles")
                    else float(value)
                )
                for name, value in zip(COLUMNS, row)
            }
            event["event_timestamp"] = datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            )

            key = str(event["unit_number"])

            producer.send(
                topic=KAFKA_TOPIC,
                key=key,
                value=event,
            ).get(timeout=30)

            if index % 100 == 0 or index == len(df):
                print(f"Wysłano {index}/{len(df)} rekordów")

            time.sleep(1 / EVENTS_PER_SECOND)

        producer.flush()
    finally:
        producer.close()

    print("Producer zakończony.")


if __name__ == "__main__":
    main()