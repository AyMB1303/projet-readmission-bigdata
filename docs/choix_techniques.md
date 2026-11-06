# Justification des choix techniques

## Objectif

Le projet met en place un pipeline Big Data pour traiter les données de séjours hospitaliers de patients diabétiques et préparer la prédiction des réadmissions.

Dans le livrable 1, l’objectif principal est d’ingérer le dataset UCI en continu, puis de l’archiver de manière distribuée et fiable.

## Docker Compose

Docker Compose permet de démarrer tous les services du projet avec une seule commande et dans un environnement identique pour chaque membre de l’équipe.

Il facilite la reproductibilité du projet : Kafka, Spark et HDFS utilisent les mêmes versions et la même configuration sur chaque machine.

## Kafka

Kafka est utilisé pour l’ingestion des données en temps réel.

Le producer Python lit le fichier `diabetic_data.csv` ligne par ligne et publie chaque séjour hospitalier sous forme d’événement JSON dans le topic `hospital-discharges`.

Kafka permet de découpler le producer et Spark : le producer peut continuer à envoyer des données même si le traitement Spark est momentanément indisponible.

Le topic contient 3 partitions, ce qui prépare le système à un traitement parallèle.

## Spark Structured Streaming

Spark Structured Streaming lit les événements depuis Kafka de manière continue grâce à `readStream`.

Spark est choisi car il permet de traiter des volumes importants de données et de répartir le travail entre plusieurs workers.

Dans cette architecture, un Spark Master coordonne deux Spark Workers. Le Job A transforme le flux Kafka et archive les données dans HDFS au format Parquet.

Les checkpoints Spark sont stockés dans HDFS afin de permettre au job de reprendre son traitement après un arrêt ou une panne, sans relire ou perdre les données déjà traitées.

## HDFS

HDFS est utilisé pour le stockage distribué des données archivées.

Le NameNode gère les métadonnées et indique l’emplacement des fichiers. Les deux DataNodes stockent les blocs de données.

Le facteur de réplication est fixé à 2 : chaque bloc est copié sur les deux DataNodes. Ainsi, si un DataNode devient indisponible, les données restent accessibles depuis l’autre DataNode.

Les données brutes archivées sont stockées dans :

```text
/data/raw/hospital_discharges
```

## Format Parquet

Parquet est un format de stockage orienté colonnes, adapté aux traitements analytiques avec Spark.

Il est plus efficace que CSV ou JSON pour les requêtes sur certaines colonnes, réduit l’espace de stockage grâce à la compression et conserve les types de données.

Ce format facilitera les analyses statistiques et l’entraînement des modèles de machine learning dans les prochains livrables.

## Conformité avec les exigences du projet

L’architecture proposée respecte les exigences de la fiche de projet :

- le cluster Docker contient un Spark Master et deux Spark Workers ;
- HDFS contient un NameNode et deux DataNodes avec une réplication de 2 ;
- les événements sont publiés dans Kafka par un script producer ;
- la source du traitement Spark Structured Streaming est Kafka ;
- le flux est archivé dans HDFS au format Parquet ;
- les traitements Spark sont exécutés sur le cluster ;
- l’architecture prépare la voie froide pour l’analyse historique et l’entraînement du modèle, ainsi que la voie chaude pour les prédictions en temps réel.

Le langage choisi pour les jobs Spark est Python avec PySpark, afin d’utiliser un même langage pour l’ingestion, le traitement distribué et les futurs travaux de machine learning.

