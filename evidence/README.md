# Index des preuves (captures d'écran)

Règles de l'énoncé : chaque capture doit montrer **l'identifiant / namespace Onyxia** et **la date et l'heure**, sinon elle n'est pas comptée.
Nommage : `NN_description.png`, numérotation continue. Chaque capture est citée dans le rapport (colonne « Rapport »).

Exigences minimales : contenu du bucket S3 (raw **et** processed) + une trace d'exécution **par composant** (Job k8s, Spark, DuckDB…).

| N° | Fichier | Étape | Qui | Doit montrer | Rapport | État |
|---|---|---|---|---|---|---|
| 01 | `01_ingest_job_logs.png` | 2 – ingestion raw | Arthur | `kubectl wait` + logs `download` (taille, sha256) et `upload` du Job `ingest-raw-tgv` | Workflow / preuve composant Kubernetes | à faire |
| 02 | `02_s3_raw_listing.png` | 2 – ingestion raw | Arthur | `aws s3 ls` (ou « Mes fichiers » Onyxia) de `raw/regularite_tgv/extraction=2026-09/` : CSV + `.meta` | Preuve bucket S3 raw | à faire |

<!-- Les lignes des étapes suivantes (Spark silver/gold, DuckDB, benchmark, Iceberg, CronJob, rejeu croisé) sont ajoutées au fur et à mesure. -->
