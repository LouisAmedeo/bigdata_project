# Big Data project — Ponctualité des TGV (2018-2026)

## Authors

- AMEDEO, Louis
- BENATAR, Arthur

## Prerequisites

- Compte Onyxia Adaltas (`onyxia.adm.adaltas.cloud`) et un service **vscode-pyspark** (image `inseefrlab/onyxia-vscode-pyspark:py3.13.15-spark4.1.1`, options par défaut : S3 et rôle Kubernetes activés). Tout se lance depuis son terminal, rien à installer.
- Outils déjà présents dans le service : `kubectl`, `aws` (CLI v2), `duckdb` (CLI ≥ 1.5), Spark 4.1.1. Identifiants S3 : variables `AWS_*` injectées par Onyxia (valables 12 h).
- Accès Internet du cluster vers `ressources.data.sncf.com` et Docker Hub (images `curlimages/curl:8.11.1`, `amazon/aws-cli:2.22.0`, `inseefrlab/onyxia-vscode-pyspark`).

## Dataset

| Champ | Valeur |
|---|---|
| Nom | Régularité mensuelle TGV par liaisons (SNCF Voyageurs / AQST) |
| URL | https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/ |
| Licence | ODbL (Open Database License) |
| Date de téléchargement | 2026-11-16 (extraction figée, re-téléchargée à l'identique le 2026-11-17) |
| Taille (brute) | 2 909 048 octets (2,9 Mo), CSV `;`, 12 907 lignes × 26 colonnes |
| Version | extraction figée `date <= 2026-09` (2018-01 → 2026-09), triée par date, service, gares |
| sha256 | `8b2f5a27593f32516f0781ab591d8316f9b09e639fa5f4d76496046e837602ee` |

## Problem statement

Entre 2018-2019 (avant Covid) et 2024-2025, quelles liaisons TGV ont le plus perdu en ponctualité à l'arrivée, et quelle famille de causes explique cette dégradation ? Le pipeline produit un classement des liaisons par variation du taux de trains à l'heure, avec la contribution de chaque cause de retard. Le résultat est vérifié par un recalcul indépendant sous DuckDB à partir du CSV brut.

## S3 paths

Bucket = namespace Onyxia de la personne qui lance (`BUCKET=${BUCKET:-$KUBERNETES_NAMESPACE}`). Exécution de référence (preuves) : bucket `user-a-benatar-ece`.

| Couche | Chemin |
|---|---|
| Raw | `s3://user-a-benatar-ece/bigdata_project/raw/regularite_tgv/extraction=2026-09/regularite-mensuelle-tgv-aqst.csv` (+ `.meta`) |
| Processed | `s3://user-a-benatar-ece/bigdata_project/processed/` : `silver/regularite_mensuelle/annee=*/` (Parquet), `gold/degradation_liaisons/` (Parquet, + `_csv/`), `gold/ponctualite_annuelle/`, `quality/rapport_qualite/`, `verification/`, `benchmark/`, `_run_info/` |

## Reproduction

Dans le terminal du service vscode-pyspark (les numéros d'étape sont ceux affichés par les scripts) :

1. `git clone https://github.com/LouisAmedeo/bigdata_project.git && cd bigdata_project`
2. `bash src/run_pipeline.sh` (≈ 3 min) enchaîne les étapes 2 à 4 ; on peut aussi les lancer une par une :
   - **Étape 2, ingestion raw** : `bash src/01_ingest_raw.sh` — Job k8s `ingest-raw-tgv` (`k8s/01-job-ingest-raw.yaml`) : télécharge à la source, refuse un sha256 ou une taille inattendus, dépose CSV + `.meta` dans `raw/`.
   - **Étape 3, transformation** : `bash src/02_transform.sh` — Job k8s `transform-spark-tgv` (`k8s/02-job-spark-transform.yaml`, `spark-submit local[2]` de `src/02_transform_spark.py`) : nettoyage, 11 règles qualité, silver, gold.
   - **Étape 4, vérification** : `bash src/03_verify.sh` — DuckDB relit le CSV brut, recalcule tout en SQL (`src/03_verify_duckdb.sql`) et doit afficher `VERIFICATION OK` (sinon code retour ≠ 0).
3. Facultatif, **étape 5** : `bash src/04_benchmark.sh` (≈ 10 min, Job `benchmark-tgv`, `k8s/03-job-benchmark.yaml`) — pandas / DuckDB / Spark, CSV / Parquet, ×1 / ×10 / ×100.
4. Lire le résultat : `aws s3 cp s3://$KUBERNETES_NAMESPACE/bigdata_project/processed/gold/degradation_liaisons_csv/ . --recursive`. (Étape 1, facultative : `bash src/00_download.sh` télécharge seulement le brut en local.)

**Relance / reprise :** chaque étape est idempotente (ConfigMaps et Secret en `apply`, ancien Job supprimé puis recréé, écritures en `overwrite` sur des chemins fixes) : on relance simplement la commande. Reprise à partir d'une étape : `bash src/run_pipeline.sh 3` (transformation puis vérification) ou `bash src/run_pipeline.sh 4` (vérification seule). Échec d'un Job : `kubectl logs job/<nom>` puis relance du script. Identifiants S3 expirés (> 12 h, message « accès refusé ») : redémarrer le service Onyxia ou recopier les variables de *Mon compte → Connexion au stockage*, puis relancer. Les Jobs s'effacent seuls après 24 h (`ttlSecondsAfterFinished`).
