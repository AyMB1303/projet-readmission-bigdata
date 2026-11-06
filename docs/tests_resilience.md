\# Tests de résilience du pipeline



> \*\*Statut : brouillon.\*\* Le test 1 est en cours de validation et le test 2 n'est pas encore fait. Les champs « À COMPLÉTER » seront remplis avec les mesures réelles.



\*\*Objectif\*\* : prouver que le pipeline Kafka → Spark Structured Streaming → HDFS ne perd pas et ne duplique pas de données quand un composant tombe.



\*\*Environnement\*\* : Docker Desktop (Windows), 12 Go de RAM alloués à WSL 2, 2 DataNodes HDFS (`dfs.replication` = 2), 2 workers Spark, 1 broker Kafka (3 partitions).

\*\*Jeu de données\*\* : `diabetic\_data.csv`, 101 766 séjours attendus.

\*\*Débit du producer\*\* : environ 94 événements/s mesurés (accéléré par rapport aux 20/s par défaut pour raccourcir le test). Le test dure donc environ 18 minutes.



\## État de référence (avant les tests)



\- `hdfs dfsadmin -report` : \*\*Live datanodes (2)\*\*, 0 bloc sous-répliqué, 0 bloc manquant.

\- `hdfs getconf -confKey dfs.replication` : \*\*2\*\*.

\- Topic Kafka `hospital-discharges` : 3 partitions, vide au départ.



!\[État initial HDFS](img/etat\_initial\_hdfs.png)



\## Test 1 : arrêt brutal de Spark en plein run (checkpoint)



\### Commandes exécutées



```powershell

docker compose up -d --build

powershell -ExecutionPolicy Bypass -File scripts\\start\_archive.ps1

docker compose --profile ingestion up -d producer

\# surveillance du compteur dans Kafka UI (localhost:8090)

docker exec namenode /opt/hadoop/bin/hdfs dfs -ls /data/raw/hospital\_discharges/\_spark\_metadata

docker exec namenode /opt/hadoop/bin/hdfs dfs -ls /checkpoints/archive\_raw/offsets

docker kill spark-master

docker ps

docker start spark-master

powershell -ExecutionPolicy Bypass -File scripts\\start\_archive.ps1

docker exec namenode /opt/hadoop/bin/hdfs dfs -ls /checkpoints/archive\_raw/offsets

```



\### Observations



\- Le producer a démarré à 20:36:20 (UTC). Le job Spark tournait depuis 20:30.

\- \*\*Avant le crash\*\* : le dernier lot validé dans `\_spark\_metadata` est le \*\*18\*\*, et le dossier `offsets/` du checkpoint va jusqu'au \*\*19\*\* (lot planifié, pas encore validé). Kafka contenait alors environ 32 000 à 45 000 messages (À COMPLÉTER avec la valeur de la capture).

\- \*\*Crash\*\* : `docker kill spark-master`. `docker ps` ne montre plus `spark-master`, alors que les deux workers, les DataNodes, Kafka et le producer continuent de tourner. Le producer continue d'envoyer pendant la panne. Heure du kill : À COMPLÉTER.

\- \*\*Reprise\*\* : `docker start spark-master`, puis `start\_archive.ps1`. Le journal affiche `Archivage démarré` vers 20:44:07 (UTC), suivi de `\[batch 24] 1055 événements archivés`. La numérotation des lots \*\*continue\*\* (24, 25, 26...) au lieu de repartir de 0 : Spark a relu son checkpoint dans HDFS.

\- Après la reprise, le dossier `offsets/` s'allonge (de 19 jusqu'à 26 une minute après), sans retour à zéro.

\- Ensuite, Spark traite environ 1 890 événements par lot toutes les 20 secondes, soit le rythme du producer : il n'accumule pas de retard.

\- État de HDFS pendant le run : `Status: HEALTHY`, réplication moyenne 2.0.



!\[Kafka UI avant le crash](img/test1\_kafka\_avant\_crash.png)

!\[docker ps sans spark-master](img/test1\_docker\_ps\_apres\_kill.png)

!\[Reprise : journal du job](img/test1\_reprise\_journal.png)



\### Résultats chiffrés



| Mesure | Valeur |

|---|---|

| Dernier lot validé avant le kill | 18 |

| Dernier fichier d'offsets avant le kill | 19 |

| Premier lot après la reprise | 24 |

| Temps de reprise (kill → job actif) | À COMPLÉTER |

| Lignes dans HDFS (`count(\*)`) | À COMPLÉTER (attendu 101 766) |

| Séjours distincts (`encounter\_id`) | À COMPLÉTER (attendu 101 766) |

| Positions Kafka distinctes (partition + offset) | À COMPLÉTER (attendu 101 766) |



\### Verdict



À COMPLÉTER (PASS si les trois nombres valent 101 766, FAIL sinon).



\## Test 2 : arrêt d'un DataNode



À FAIRE. Plan :



1\. Pendant que l'archivage tourne : `docker stop datanode1`.

2\. `hdfs dfsadmin -report` : 1 DataNode vivant attendu (le NameNode peut mettre plusieurs minutes à constater la panne).

3\. Vérifier que Spark continue d'écrire.

4\. `hdfs fsck /data/raw` : blocs sous-répliqués attendus (une seule copie).

5\. `docker start datanode1`, puis `fsck` régulièrement jusqu'à `HEALTHY`. Noter le temps de récupération.



\## Conclusion



À COMPLÉTER après les deux tests.

