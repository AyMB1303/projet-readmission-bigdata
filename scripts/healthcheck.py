"""Vérifie que HDFS, Kafka et Spark sont opérationnels."""

import json
import re
import subprocess
import sys
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient


def check_hdfs():
    """Vérifie que HDFS répond et que 2 DataNodes sont vivants."""
    try:
        for datanode in ("datanode1", "datanode2"):
            status = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", datanode],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if status.stdout.strip().lower() != "true":
                return False, f"HDFS : conteneur {datanode} arrêté."
        result = subprocess.run(
            [
                "docker",
                "exec",
                "namenode",
                "/opt/hadoop/bin/hdfs",
                "dfsadmin",
                "-report",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        output = result.stdout + result.stderr

        if result.returncode != 0:
            return False, f"HDFS indisponible : {output.strip()}"

        match = re.search(r"Live datanodes \((\d+)\)", output)

        if not match:
            return False, "HDFS : nombre de DataNodes introuvable."

        live_datanodes = int(match.group(1))

        if live_datanodes != 2:
            return False, f"HDFS : {live_datanodes}/2 DataNodes vivants."

        return True, "HDFS : 2 DataNodes vivants."

    except Exception as error:
        return False, f"HDFS indisponible : {error}"


def check_kafka():
    """Vérifie que Kafka répond et que les topics existent."""
    try:
        client = AdminClient({"bootstrap.servers": "localhost:29092"})
        metadata = client.list_topics(timeout=5)

        required_topics = {
            "hospital-discharges",
            "hospital-discharges-dlq",
        }

        missing_topics = required_topics - set(metadata.topics)

        if missing_topics:
            return False, f"Kafka : topic(s) manquant(s) : {', '.join(missing_topics)}"

        return True, "Kafka : disponible, topics présents."

    except Exception as error:
        return False, f"Kafka indisponible : {error}"


def check_spark():
    """Vérifie que Spark Master répond et possède 2 workers ALIVE."""
    try:
        with urlopen("http://localhost:8080/json/", timeout=5) as response:
            data = json.load(response)

        workers = data.get("workers", [])
        alive_workers = [
            worker for worker in workers
            if worker.get("state") == "ALIVE"
        ]

        if len(alive_workers) != 2:
            return False, f"Spark : {len(alive_workers)}/2 workers ALIVE."

        return True, "Spark : Master disponible, 2 workers ALIVE."

    except Exception as error:
        return False, f"Spark indisponible : {error}"


def main():
    checks = [check_hdfs(), check_kafka(), check_spark()]
    failed = False

    for ok, message in checks:
        prefix = "OK" if ok else "ERREUR"
        print(f"[{prefix}] {message}")

        if not ok:
            failed = True

    if failed:
        sys.exit(1)

    print("\nOK : le cluster est opérationnel.")


if __name__ == "__main__":
    main()