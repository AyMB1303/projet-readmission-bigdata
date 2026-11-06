"""Vérification de l'output du Livrable 1.

Lit les fichiers Parquet archivés dans HDFS et produit un rapport de validation
imprimé sur stdout (rediriger vers docs/verification_output_livrable1.md).

Vérifications :
  1. Intégrité des données   : compte de lignes, unicité encounter_id, lignes nulles
  2. Conformité du schéma    : types Parquet vs définitions de archive_to_hdfs.py
  3. Valeurs sentinelles     : absence de '?' dans le Parquet, nulls par colonne
  4. Partitionnement HDFS    : partitions ingest_date, compression vs CSV source
  5. Relecture après restart : checkpoint HDFS présent et cohérent

Lancement :
  docker exec spark-master spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/app/data/verify_output.py

  Ou depuis la racine du projet :
    ./scripts/run_verify.sh          (Linux / Mac)
    powershell -File scripts/run_verify.ps1  (Windows)
"""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, LongType, StringType, TimestampType,
)

# ---------------------------------------------------------------------------
# Constantes du schéma — mirroir de ingestion/schema.py et archive_to_hdfs.py
# Toute modification là-bas doit être répercutée ici.
# ---------------------------------------------------------------------------
INT_COLUMNS = [
    "encounter_id", "patient_nbr", "admission_type_id",
    "discharge_disposition_id", "admission_source_id",
    "time_in_hospital", "num_lab_procedures", "num_procedures",
    "num_medications", "number_outpatient", "number_emergency",
    "number_inpatient", "number_diagnoses",
]
STRING_COLUMNS = [
    "race", "gender", "age", "weight", "payer_code", "medical_specialty",
    "diag_1", "diag_2", "diag_3", "max_glu_serum", "A1Cresult",
    "metformin", "repaglinide", "nateglinide", "chlorpropamide",
    "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide",
    "pioglitazone", "rosiglitazone", "acarbose", "miglitol", "troglitazone",
    "tolazamide", "examide", "citoglipton", "insulin",
    "glyburide-metformin", "glipizide-metformin",
    "glimepiride-pioglitazone", "metformin-rosiglitazone",
    "metformin-pioglitazone", "change", "diabetesMed", "readmitted",
]
METADATA_COLUMNS = {
    "event_id":        StringType(),
    "event_type":      StringType(),
    "event_time":      TimestampType(),
    "seq":             LongType(),
    "source":          StringType(),
    "kafka_partition": IntegerType(),
    "kafka_offset":    LongType(),
    "kafka_timestamp": TimestampType(),
    "ingest_date":     DateType(),
}

EXPECTED_ROWS   = 101_766
RAW_PATH        = os.getenv("RAW_PATH",    "hdfs://namenode:8020/data/raw/hospital_discharges")
CHECKPOINT_PATH = os.getenv("RAW_CHECKPOINT", "hdfs://namenode:8020/checkpoints/archive_raw")
CSV_SIZE_MB     = 18.0   # taille connue du CSV source (docs/donnees.md)

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _status(ok: bool, warn: bool = False) -> str:
    if warn:
        return WARN
    return PASS if ok else FAIL


def _type_name(dt) -> str:
    return type(dt).__name__.replace("Type", "")


def _hdfs_du(spark: SparkSession, path: str) -> int:
    """Retourne la taille totale (octets) d'un chemin HDFS via hadoop fs -du."""
    sc = spark.sparkContext
    conf = sc._jvm.org.apache.hadoop.conf.Configuration()
    fs   = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path), conf
    )
    content = fs.getContentSummary(sc._jvm.org.apache.hadoop.fs.Path(path))
    return int(content.getLength())


def _hdfs_ls(spark: SparkSession, path: str) -> list[str]:
    """Liste les chemins enfants directs d'un répertoire HDFS."""
    sc = spark.sparkContext
    conf = sc._jvm.org.apache.hadoop.conf.Configuration()
    fs   = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path), conf
    )
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        return []
    statuses = fs.listStatus(hadoop_path)
    return [str(s.getPath()) for s in statuses]


def _hdfs_exists(spark: SparkSession, path: str) -> bool:
    sc = spark.sparkContext
    conf = sc._jvm.org.apache.hadoop.conf.Configuration()
    fs   = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path), conf
    )
    return bool(fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)))


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_integrite(df) -> tuple[str, list[str]]:
    lines = []
    all_ok = True

    # 1a. Compte de lignes
    count = df.count()
    ok = count == EXPECTED_ROWS
    all_ok = all_ok and ok
    lines.append(f"- Lignes totales : **{count:,}** (attendu {EXPECTED_ROWS:,}) — {_status(ok)}")

    # 1b. Unicité de encounter_id
    distinct_ids = df.select("encounter_id").distinct().count()
    ok_uniq = distinct_ids == count
    all_ok = all_ok and ok_uniq
    duplicates = count - distinct_ids
    lines.append(
        f"- Unicité `encounter_id` : {distinct_ids:,} distincts, "
        f"{duplicates} doublon(s) — {_status(ok_uniq)}"
    )

    # 1c. Lignes entièrement nulles (sur les colonnes cliniques)
    clinical = INT_COLUMNS + STRING_COLUMNS
    null_expr = F.lit(True)
    for c in clinical:
        null_expr = null_expr & F.col(c).isNull()
    full_null = df.filter(null_expr).count()
    ok_null = full_null == 0
    all_ok = all_ok and ok_null
    lines.append(f"- Lignes entièrement nulles : **{full_null}** — {_status(ok_null)}")

    return _status(all_ok), lines


def check_schema(df) -> tuple[str, list[str]]:
    lines = []
    all_ok = True
    schema_map = {f.name: f.dataType for f in df.schema.fields}

    expected = (
        {c: IntegerType() for c in INT_COLUMNS}
        | {c: StringType() for c in STRING_COLUMNS}
        | METADATA_COLUMNS
    )

    missing, wrong_type = [], []
    for col_name, expected_type in expected.items():
        if col_name not in schema_map:
            missing.append(col_name)
        elif type(schema_map[col_name]) != type(expected_type):
            wrong_type.append(
                f"`{col_name}` : attendu {_type_name(expected_type)}, "
                f"trouvé {_type_name(schema_map[col_name])}"
            )

    # Colonnes inconnues dans le Parquet
    known = set(expected.keys())
    extra = [c for c in schema_map if c not in known]

    ok = not missing and not wrong_type
    all_ok = all_ok and ok

    total = len(expected)
    correct = total - len(missing) - len(wrong_type)
    lines.append(f"- Colonnes vérifiées : **{correct}/{total}** correctes — {_status(ok)}")

    if missing:
        lines.append(f"- Colonnes manquantes : {', '.join(f'`{c}`' for c in missing)}")
    if wrong_type:
        for wt in wrong_type:
            lines.append(f"- Type incorrect : {wt}")
    if extra:
        lines.append(f"- Colonnes supplémentaires (non bloquantes) : {', '.join(f'`{c}`' for c in extra)}")

    return _status(all_ok), lines


def check_sentinelles(df) -> tuple[str, list[str]]:
    lines = []
    all_ok = True

    # 3a. Absence de '?' dans les colonnes string
    question_mark_cols = []
    for c in STRING_COLUMNS:
        if c in {f.name for f in df.schema.fields}:
            n = df.filter(F.col(c) == "?").count()
            if n > 0:
                question_mark_cols.append(f"`{c}` ({n})")

    ok_qm = len(question_mark_cols) == 0
    all_ok = all_ok and ok_qm
    if ok_qm:
        lines.append(f"- Valeurs `?` résiduelles : **0** dans toutes les colonnes — {PASS}")
    else:
        lines.append(
            f"- Valeurs `?` résiduelles dans : {', '.join(question_mark_cols)} — {FAIL}"
        )

    # 3b. Nulls par colonne (top 15 non nuls)
    null_counts = []
    for c in STRING_COLUMNS + INT_COLUMNS:
        if c in {f.name for f in df.schema.fields}:
            n = df.filter(F.col(c).isNull()).count()
            if n > 0:
                null_counts.append((c, n))

    null_counts.sort(key=lambda x: -x[1])
    if not null_counts:
        lines.append("- Nulls par colonne : **0** colonne avec des nulls")
    else:
        lines.append(f"- Colonnes avec nulls ({len(null_counts)}) — top 10 :")
        for c, n in null_counts[:10]:
            pct = n / EXPECTED_ROWS * 100
            lines.append(f"  - `{c}` : {n:,} nulls ({pct:.1f} %)")
        if len(null_counts) > 10:
            lines.append(f"  - … et {len(null_counts) - 10} autre(s)")

    return _status(all_ok), lines


def check_partitionnement(spark, df) -> tuple[str, list[str]]:
    lines = []
    all_ok = True

    # 4a. Partitions ingest_date
    partitions = (
        df.select("ingest_date")
        .distinct()
        .orderBy("ingest_date")
        .rdd.map(lambda r: str(r[0]))
        .collect()
    )
    lines.append(f"- Partitions `ingest_date` : **{len(partitions)}**")
    for p in partitions:
        lines.append(f"  - `{p}`")

    # 4b. Répartition par partition Kafka
    lines.append("- Répartition par partition Kafka :")
    dist = (
        df.groupBy("kafka_partition")
        .count()
        .orderBy("kafka_partition")
        .collect()
    )
    for row in dist:
        pct = row["count"] / EXPECTED_ROWS * 100
        lines.append(f"  - Partition {row['kafka_partition']} : {row['count']:,} events ({pct:.1f} %)")

    # 4c. Taille HDFS et ratio de compression
    try:
        parquet_bytes = _hdfs_du(spark, RAW_PATH)
        parquet_mb    = parquet_bytes / 1_048_576
        ratio         = CSV_SIZE_MB / parquet_mb if parquet_mb > 0 else 0
        lines.append(
            f"- Taille Parquet dans HDFS : **{parquet_mb:.2f} Mo** "
            f"(CSV source : {CSV_SIZE_MB} Mo, ratio compression : {ratio:.1f}×) — {_status(parquet_mb > 0)}"
        )
    except Exception as e:
        lines.append(f"- Taille HDFS : impossible à calculer ({e}) — {WARN}")
        all_ok = False

    return _status(all_ok), lines


def check_relecture(spark) -> tuple[str, list[str]]:
    lines = []
    all_ok = True

    # 5a. Checkpoint présent
    cp_exists = _hdfs_exists(spark, CHECKPOINT_PATH)
    all_ok = all_ok and cp_exists
    lines.append(f"- Checkpoint HDFS (`{CHECKPOINT_PATH}`) : {'présent' if cp_exists else 'ABSENT'} — {_status(cp_exists)}")

    # 5b. Contenu du checkpoint
    if cp_exists:
        cp_children = _hdfs_ls(spark, CHECKPOINT_PATH)
        lines.append(f"- Répertoires dans le checkpoint : {len(cp_children)}")
        for child in cp_children[:5]:
            lines.append(f"  - `{child.split('/')[-1]}`")

    # 5c. Re-lecture du Parquet (preuve que les fichiers sont lisibles)
    try:
        reread_count = spark.read.option("basePath", RAW_PATH).parquet(RAW_PATH + "/ingest_date=*").count()
        ok_reread = reread_count == EXPECTED_ROWS
        all_ok = all_ok and ok_reread
        lines.append(
            f"- Relecture Parquet : **{reread_count:,}** lignes — {_status(ok_reread)}"
        )
    except Exception as e:
        lines.append(f"- Relecture Parquet : ECHEC ({e}) — {FAIL}")
        all_ok = False

    return _status(all_ok), lines


# ---------------------------------------------------------------------------
# Rapport Markdown
# ---------------------------------------------------------------------------

def render_report(results: list[tuple[str, str, list[str]]]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Rapport de Vérification — Livrable 1",
        "",
        f"Généré le {now}  ",
        f"Source : `{RAW_PATH}`  ",
        f"Checkpoint : `{CHECKPOINT_PATH}`",
        "",
        "---",
        "",
    ]
    for title, status, details in results:
        lines.append(f"## {title} — {status}")
        lines.append("")
        lines.extend(details)
        lines.append("")

    all_pass = all(FAIL not in s for _, s, _ in results)
    lines += [
        "---",
        "",
        f"**Résultat global : {'✅ PIPELINE L1 VALIDÉ' if all_pass else '❌ ANOMALIES DÉTECTÉES'}**",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("verify-livrable1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nLecture de {RAW_PATH} ...\n", file=sys.stderr)
    # Lire uniquement les partitions ingest_date=* pour éviter _spark_metadata
    df = spark.read.option("basePath", RAW_PATH).parquet(RAW_PATH + "/ingest_date=*")
    df.cache()

    results = []

    print("1/5 — Intégrité des données ...", file=sys.stderr)
    status, lines = check_integrite(df)
    results.append(("1. Intégrité des données", status, lines))

    print("2/5 — Conformité du schéma ...", file=sys.stderr)
    status, lines = check_schema(df)
    results.append(("2. Conformité du schéma", status, lines))

    print("3/5 — Valeurs sentinelles ...", file=sys.stderr)
    status, lines = check_sentinelles(df)
    results.append(("3. Valeurs sentinelles", status, lines))

    print("4/5 — Partitionnement HDFS ...", file=sys.stderr)
    status, lines = check_partitionnement(spark, df)
    results.append(("4. Partitionnement HDFS", status, lines))

    print("5/5 — Relecture après restart ...", file=sys.stderr)
    status, lines = check_relecture(spark)
    results.append(("5. Relecture & checkpoint", status, lines))

    df.unpersist()
    report = render_report(results)
    print(report)

    spark.stop()

    any_fail = any(FAIL in s for _, s, _ in results)
    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
