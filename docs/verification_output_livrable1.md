
Lecture de hdfs://namenode:8020/data/raw/hospital_discharges ...

1/5 — Intégrité des données ...
2/5 — Conformité du schéma ...
3/5 — Valeurs sentinelles ...
4/5 — Partitionnement HDFS ...
5/5 — Relecture après restart ...
# Rapport de Vérification — Livrable 1

Généré le 2026-11-06 19:22 UTC  
Source : `hdfs://namenode:8020/data/raw/hospital_discharges`  
Checkpoint : `hdfs://namenode:8020/checkpoints/archive_raw`

---

## 1. Intégrité des données — ✅ PASS

- Lignes totales : **101,766** (attendu 101,766) — ✅ PASS
- Unicité `encounter_id` : 101,766 distincts, 0 doublon(s) — ✅ PASS
- Lignes entièrement nulles : **0** — ✅ PASS

## 2. Conformité du schéma — ✅ PASS

- Colonnes vérifiées : **59/59** correctes — ✅ PASS

## 3. Valeurs sentinelles — ✅ PASS

- Valeurs `?` résiduelles : **0** dans toutes les colonnes — ✅ PASS
- Colonnes avec nulls (7) — top 10 :
  - `weight` : 98,569 nulls (96.9 %)
  - `medical_specialty` : 49,949 nulls (49.1 %)
  - `payer_code` : 40,256 nulls (39.6 %)
  - `race` : 2,273 nulls (2.2 %)
  - `diag_3` : 1,423 nulls (1.4 %)
  - `diag_2` : 358 nulls (0.4 %)
  - `diag_1` : 21 nulls (0.0 %)

## 4. Partitionnement HDFS — ✅ PASS

- Partitions `ingest_date` : **1**
  - `2026-11-06`
- Répartition par partition Kafka :
  - Partition 0 : 34,164 events (33.6 %)
  - Partition 1 : 33,953 events (33.4 %)
  - Partition 2 : 33,649 events (33.1 %)
- Taille Parquet dans HDFS : **3.70 Mo** (CSV source : 18.0 Mo, ratio compression : 4.9×) — ✅ PASS

## 5. Relecture & checkpoint — ✅ PASS

- Checkpoint HDFS (`hdfs://namenode:8020/checkpoints/archive_raw`) : présent — ✅ PASS
- Répertoires dans le checkpoint : 4
  - `commits`
  - `metadata`
  - `offsets`
  - `sources`
- Relecture Parquet : **101,766** lignes — ✅ PASS

---

**Résultat global : ✅ PIPELINE L1 VALIDÉ**

