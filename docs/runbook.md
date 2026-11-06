# Runbook - dépannage du cluster Big Data

Ce guide aide à diagnostiquer et corriger les incidents courants du cluster Docker.

La commande générale de vérification est :

```powershell
python .\scripts\healthcheck.py
```

## 1. Un DataNode est arrêté

| Élément | Contenu |
|---|---|
| Symptôme | Le healthcheck affiche `HDFS : 1/2 DataNodes vivants`. |
| Diagnostic | `docker compose ps` puis `docker exec namenode /opt/hadoop/bin/hdfs dfsadmin -report` |
| Correction | `docker compose start datanode1` ou `docker compose start datanode2` |
| Vérification | Relancer `python .\scripts\healthcheck.py`. Le résultat doit afficher `HDFS : 2 DataNodes vivants`. |

## 2. Kafka ne démarre pas

| Élément | Contenu |
|---|---|
| Symptôme | Le healthcheck affiche `Kafka indisponible` ou le producer ne peut pas se connecter. |
| Diagnostic | `docker compose ps` puis `docker compose logs kafka` |
| Correction | Vérifier que le port `29092` est disponible, puis lancer `docker compose up -d kafka kafka-init`. |
| Vérification | Ouvrir Kafka UI sur `http://localhost:8090` et relancer le healthcheck. |

## 3. Un worker Spark manque de mémoire

| Élément | Contenu |
|---|---|
| Symptôme | Un worker Spark s’arrête, ou les traitements deviennent très lents. |
| Diagnostic | `docker stats`, `docker compose logs spark-worker-1` et l’interface Spark `http://localhost:8080`. |
| Correction | Augmenter la mémoire Docker Desktop ou modifier `SPARK_WORKER_MEMORY` dans le fichier `.env`, puis relancer les workers avec `docker compose up -d`. |
| Vérification | Le healthcheck doit afficher `Spark : Master disponible, 2 workers ALIVE`. |

## 4. Le producer attend Kafka

| Élément | Contenu |
|---|---|
| Symptôme | Le producer affiche une erreur de connexion ou n’envoie aucun événement. |
| Diagnostic | `docker compose logs producer` puis `docker compose ps`. Vérifier que Kafka est `healthy`. |
| Correction | Démarrer Kafka avec `docker compose up -d kafka kafka-init`, puis relancer le producer. Vérifier que `KAFKA_BOOTSTRAP` vaut `kafka:9092` dans le conteneur. |
| Vérification | Le producer doit afficher le nombre d’événements envoyés et confirmés. |

## 5. Le NameNode reste en safe mode

| Élément | Contenu |
|---|---|
| Symptôme | HDFS refuse les écritures ou le pipeline Spark ne peut pas écrire les fichiers Parquet. |
| Diagnostic | `docker exec namenode /opt/hadoop/bin/hdfs dfsadmin -safemode get` |
| Correction | Attendre que les deux DataNodes se reconnectent. En dernier recours : `docker exec namenode /opt/hadoop/bin/hdfs dfsadmin -safemode leave`. |
| Vérification | La commande de diagnostic doit afficher `Safe mode is OFF`, puis le healthcheck doit être vert. |