"""Consumer de la Dead Letter Queue.

Affiche les lignes rejetées par le producer Kafka avec la raison du rejet.
Lancement depuis la racine du projet :

    python ingestion/dlq_consumer.py
"""
import json
import os
import signal

from confluent_kafka import Consumer, KafkaError


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "hospital-discharges-dlq")
GROUP_ID = os.getenv("DLQ_GROUP_ID", "dlq-inspector")

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([DLQ_TOPIC])

    print(f"Lecture de la DLQ : {DLQ_TOPIC}")
    print("Appuie sur Ctrl+C pour arrêter.\n")

    try:
        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"[ERREUR Kafka] {message.error()}")
                continue

            try:
                rejected = json.loads(message.value().decode("utf-8"))

                print("=" * 70)
                print(f"Ligne rejetée : {rejected.get('row_number')}")
                print(f"Raison : {rejected.get('error')}")
                print(f"Heure : {rejected.get('rejected_at')}")
                print("Données brutes :")
                print(json.dumps(rejected.get("raw_row"), ensure_ascii=False, indent=2))

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                print(f"[ERREUR] Message DLQ illisible : {error}")

    finally:
        consumer.close()
        print("\nConsumer DLQ arrêté.")


if __name__ == "__main__":
    main()