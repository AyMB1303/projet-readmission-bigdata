"""Producteur Kafka — couche d'ingestion du projet.

Rejoue le dataset « Diabetes 130-US Hospitals (1999-2008) » ligne par ligne,
comme si chaque sortie d'hôpital était publiée en temps réel par le système
d'information hospitalier.

  - topic      : hospital-discharges (variable KAFKA_TOPIC)
  - clé        : patient_nbr -> tous les séjours d'un patient vont dans la
                 même partition, donc restent ordonnés
  - valeur     : événement JSON plat (voir schema.py)
  - débit      : EVENTS_PER_SECOND événements / seconde (simulation du flux)

Variables d'environnement (toutes optionnelles) :
  KAFKA_BOOTSTRAP     kafka:9092 dans Docker, localhost:29092 depuis l'hôte
  KAFKA_TOPIC         hospital-discharges
  DATA_FILE           /data/diabetic_data.csv
  EVENTS_PER_SECOND   20     (0 = aussi vite que possible)
  MAX_EVENTS          0      (0 = tout le fichier)
  START_ROW           0      (reprendre le rejeu à partir d'une ligne)
  LOOP                false  (rejouer le fichier en boucle)
"""
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from schema import to_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "hospital-discharges")
DATA_FILE = os.getenv("DATA_FILE", "/data/diabetic_data.csv")
RATE = float(os.getenv("EVENTS_PER_SECOND", "20"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))
START_ROW = int(os.getenv("START_ROW", "0"))
LOOP = os.getenv("LOOP", "false").lower() in ("1", "true", "yes")

stats = {"sent": 0, "acked": 0, "failed": 0}
running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def on_delivery(err, msg):
    """Callback appelé par Kafka pour chaque message : confirmation ou erreur."""
    if err is not None:
        stats["failed"] += 1
        print(f"[ERREUR] livraison échouée : {err}", file=sys.stderr)
    else:
        stats["acked"] += 1


def main():
    if not os.path.exists(DATA_FILE):
        sys.exit(f"Fichier introuvable : {DATA_FILE} (lancez scripts/download_dataset)")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "readmission-producer",
        "acks": "all",                 # le broker confirme l'écriture
        "enable.idempotence": True,    # pas de doublons en cas de nouvel essai
        "linger.ms": 20,
        "compression.type": "lz4",
    })
    print(f"Producteur connecté à {BOOTSTRAP} | topic={TOPIC} | débit={RATE or 'max'} evt/s")

    source = f"replay:{os.path.basename(DATA_FILE)}"
    pause = 1.0 / RATE if RATE > 0 else 0
    seq, t0 = 0, time.time()

    while running:
        with open(DATA_FILE, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                if not running or (MAX_EVENTS and stats["sent"] >= MAX_EVENTS):
                    break
                if i < START_ROW:
                    continue
                now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                event = to_event(row, seq, now, source)
                producer.produce(
                    TOPIC,
                    key=str(event["patient_nbr"]),
                    value=json.dumps(event, ensure_ascii=False),
                    on_delivery=on_delivery,
                )
                seq += 1
                stats["sent"] += 1
                producer.poll(0)
                if stats["sent"] % 500 == 0:
                    rate = stats["sent"] / (time.time() - t0)
                    print(f"{stats['sent']} événements envoyés "
                          f"({stats['acked']} confirmés, {rate:.1f} evt/s) "
                          f"- dernier encounter_id={event['encounter_id']}")
                if pause:
                    time.sleep(pause)
        if not LOOP or (MAX_EVENTS and stats["sent"] >= MAX_EVENTS):
            break

    producer.flush(30)
    print(f"Terminé : {stats['sent']} envoyés, {stats['acked']} confirmés, "
          f"{stats['failed']} en échec, durée {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
