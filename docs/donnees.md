# Documentation des données

## Provenance et conditions d'utilisation
- **Jeu de données :** *Diabetes 130-US Hospitals for Years 1999–2008*, UCI Machine Learning Repository, ID 296 — https://archive.ics.uci.edu/dataset/296
- **Licence :** CC BY 4.0, réutilisation libre avec citation.
- **Référence :** Strack B. *et al.* (2014), *Impact of HbA1c Measurement on Hospital Readmission Rates*, BioMed Research International.
- **Volume :** 101 766 séjours, 50 colonnes, environ 18 Mo au format CSV. On compte environ 70 000 patients distincts.

## Sens d'un événement
Un événement Kafka = **la sortie d'hôpital d'un patient diabétique** (un séjour, *encounter*).

| Élément | Champ |
|---|---|
| Identifiant de l'événement | `event_id` = `encounter_id` (unique) |
| Identifiant du patient (clé Kafka) | `patient_nbr` |
| Horodatage | `event_time` (UTC, date de publication lors du rejeu) ; le dataset ne contient pas de date réelle |
| Ordre de rejeu | `seq` (ordre croissant de `encounter_id`) |
| Métadonnées techniques ajoutées par Spark | `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingest_date` |

## Cible
- `readmitted` ∈ {`<30`, `>30`, `NO`}. La cible binaire du projet vaut `label = 1` si `<30` et `0` sinon.
- Elle est déséquilibrée : environ 11 % de `<30`.
- **Elle n'est connue que 30 jours après la sortie.** Elle n'est donc utilisée que pour l'entraînement et l'évaluation, jamais en entrée du modèle.

## Variables explicatives (disponibles au moment de la sortie)
| Famille | Colonnes |
|---|---|
| Démographie | `race`, `gender`, `age` (tranches de 10 ans) ; `weight` est vide à 97 % |
| Admission / sortie | `admission_type_id`, `discharge_disposition_id`, `admission_source_id` (codes, voir `IDs_mapping.csv`) |
| Séjour | `time_in_hospital`, `num_lab_procedures`, `num_procedures`, `num_medications`, `number_diagnoses` |
| Historique de l'année précédente | `number_outpatient`, `number_emergency`, `number_inpatient` |
| Diagnostics | `diag_1`, `diag_2`, `diag_3` (codes ICD-9) |
| Examens | `max_glu_serum`, `A1Cresult` |
| Traitements | 23 antidiabétiques (`metformin` … `insulin`) : `No`, `Steady`, `Up` ou `Down` ; `change`, `diabetesMed` |
| Administratif | `payer_code` (environ 40 % manquant), `medical_specialty` (environ 49 % manquant) |

**Exclues des variables explicatives :**
- `encounter_id` et `patient_nbr`, qui sont des identifiants ;
- `readmitted`, qui est la cible.

## Qualité des données (traitements prévus au livrable 2)
- **Valeurs manquantes :** les `?` sont remplacés par `null` dès l'ingestion.
- **Patients décédés ou en soins palliatifs** (`discharge_disposition_id` ∈ {11, 13, 14, 19, 20, 21}) : ils ne peuvent pas être réadmis et sont donc exclus.
- **Sexe invalide :** suppression des lignes `Unknown/Invalid`.
- **Plusieurs séjours par patient :** le découpage entraînement / test se fait **par patient** pour éviter la fuite de données.
