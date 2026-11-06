"""Description d'un événement « sortie d'hôpital » publié dans Kafka.

Un événement = un séjour hospitalier (une ligne de diabetic_data.csv) au moment
de la sortie du patient. Ce module est la référence unique du schéma, partagée
par le producteur (Python) et documentée dans docs/donnees.md pour Spark.
"""

# Colonnes numériques entières du dataset UCI (les autres sont des chaînes)
INT_COLUMNS = [
    "encounter_id", "patient_nbr",
    "admission_type_id", "discharge_disposition_id", "admission_source_id",
    "time_in_hospital", "num_lab_procedures", "num_procedures",
    "num_medications", "number_outpatient", "number_emergency",
    "number_inpatient", "number_diagnoses",
]

# Colonnes à ne JAMAIS utiliser comme variables explicatives :
#  - identifiants
#  - la cible "readmitted", connue seulement 30 jours après la sortie
#    (transportée dans l'événement pour l'entraînement et l'évaluation en ligne)
NON_FEATURE_COLUMNS = ["encounter_id", "patient_nbr", "readmitted"]

LABEL_COLUMN = "readmitted"          # valeurs : "<30", ">30", "NO"
EVENT_TYPE = "hospital_discharge"
MISSING_MARKERS = {"?", "", "NA", "NaN"}


def to_event(row: dict, seq: int, event_time: str, source: str) -> dict:
    """Convertit une ligne CSV en événement JSON plat, typé et sans marqueurs '?'."""
    event = {
        "event_id": str(row["encounter_id"]),
        "event_type": EVENT_TYPE,
        "event_time": event_time,          # horodatage ISO-8601 UTC de publication
        "seq": seq,                        # numéro d'ordre dans le rejeu
        "source": source,
    }
    for col, raw in row.items():
        value = None if raw is None or raw.strip() in MISSING_MARKERS else raw.strip()
        if value is not None and col in INT_COLUMNS:
            value = int(value)
        event[col] = value
    return event
