# Index des preuves (captures d'écran)

Règles de l'énoncé : chaque capture doit montrer **l'identifiant / namespace Onyxia** et **la date et l'heure**, sinon elle n'est pas comptée.
Nommage : `NN_description.png`, numérotation continue. Chaque capture est citée dans le rapport (section 8).

Comment elles ont été prises : captures du **vrai terminal du service vscode-pyspark** d'Onyxia (compte d'Arthur), le **2026-11-17 entre 07:44 et 08:28, heure de Paris**, à partir d'un **clone propre** du dépôt (capture 01). Sur chaque capture :
- la commande commence par `echo "namespace : $KUBERNETES_NAMESPACE"; date` (deux premières lignes affichées) ;
- l'invite du terminal affiche en permanence le badge du namespace **`user-a-benatar-ece`** et la date-heure (`export TZ=Europe/Paris; PS1='\[\e[1;97;45m\] $KUBERNETES_NAMESPACE \[\e[0m\] \[\e[1;34m\]\D{%Y-%m-%d %H:%M:%S %Z}\[\e[0m\] \w\$ '`) ;
- les scripts affichent aussi `namespace | bucket | date` dans leur bandeau d'étape.

Images du dépôt en 1600 px de large ; versions pleine résolution (3200 px) dans le bucket Hugging Face privé du projet.

| N° | Fichier | Étape | Qui | Montre | Rapport | État |
|---|---|---|---|---|---|---|
| 01 | `01_clone_depot.png` | 0 – reproduction | Arthur | `git clone` du dépôt public dans le service, historique des commits, `src/` et `k8s/` | §8, reproduction | fait |
| 02 | `02_ingest_job_logs.png` | 2 – ingestion raw | Arthur | `bash src/01_ingest_raw.sh` : Job `ingest-raw-tgv` Complete, logs `download` (taille 2 909 048 o, sha256) et `upload` | §3, composant Kubernetes | fait |
| 03 | `03_s3_raw_listing.png` | 2 – ingestion raw | Arthur | `aws s3 ls` du bucket et de `raw/` (CSV + `.meta`), métadonnées de l'objet, en-tête du CSV relu sur S3 | §8, bucket raw | fait |
| 04 | `04_spark_job_logs.png` | 3 – transformation | Arthur | `bash src/02_transform.sh` : Job `transform-spark-tgv`, rapport qualité R01–R11, top 10, contexte annuel, listing de `processed/` | §3–5, composant Spark | fait |
| 05 | `05_duckdb_verification.png` | 4 – vérification | Arthur | `bash src/03_verify.sh` : DuckDB relit le brut, compare au gold Spark, `VERIFICATION OK` sur 121 liaisons | §5, vérification indépendante | fait |
| 06 | `06_benchmark_job.png` | 5 – mesures | Arthur | `bash src/04_benchmark.sh` : Job `benchmark-tgv`, tableaux pandas / DuckDB / Spark ×1 ×10 ×100, petits fichiers | §3 et §7, alternatives mesurées | fait |
| 07 | `07_spark_executors_quota.png` | alternative rejetée | Arthur | `spark-submit --master k8s://…` : pods executors refusés (« exceeded quota … requests.memory=896Mi, limited 200Mi ») | §3, choix de Spark local[2] | fait |
| 08 | `08_relance_run_pipeline.png` | relance | Arthur | `bash src/run_pipeline.sh 3` : reprise à l'étape 3, ConfigMaps « unchanged », Job recréé, vérification OK, 86 s | README, relance idempotente | fait |
| 09 | `09_kubectl_jobs_quota.png` | Kubernetes | Arthur | `kubectl get jobs,pods` (3 Jobs Complete), ConfigMaps / Secret du pipeline, ressources du Job Spark, quota du namespace | §3, traces Kubernetes | fait |
| 10 | `10_s3_processed_listing.png` | bucket final | Arthur | `aws s3 ls --recursive` : `raw/` et `processed/` (silver par année, gold, quality, verification, benchmark, `_run_info`) | §8, bucket processed | fait |

Note sur la capture 10 : le préfixe `tmp_benchmark/` est la sortie temporaire du test « petits fichiers » (étape 5), vidée à la fin du Job. Il reste listé parce que le bucket a le versioning activé (lab TP3) et que SeaweedFS conserve les marqueurs de suppression ; il ne contient plus aucune donnée.
