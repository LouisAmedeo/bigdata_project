"""Étape 3 : nettoyage, contrôle qualité et agrégations avec Spark DataFrame.

raw (CSV sur S3)  ->  processed/silver/regularite_mensuelle/   (Parquet, partitionné par annee)
                  ->  processed/quality/rapport_qualite/        (une ligne par règle, nb de lignes touchées)
                  ->  processed/gold/degradation_liaisons/      (réponse à la question, Parquet + CSV)
                  ->  processed/gold/ponctualite_annuelle/      (contexte : toutes liaisons, par année)

Lancement : par le Job Kubernetes k8s/02-job-spark-transform.yaml (src/02_transform.sh),
ou directement : spark-submit --master 'local[2]' src/02_transform_spark.py
Idempotent : toutes les sorties sont écrites en mode overwrite sur des chemins fixes.

Variables d'environnement : BUCKET (défaut $KUBERNETES_NAMESPACE), RAW_PATH, PROCESSED_PATH
(par défaut s3a://$BUCKET/bigdata_project/...), identifiants S3 AWS_* (ceux d'Onyxia).
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

T0 = time.time()
BUCKET = os.environ.get("BUCKET") or os.environ.get("KUBERNETES_NAMESPACE")
PREFIX = os.environ.get("PROJECT_PREFIX", "bigdata_project")
RAW_PATH = os.environ.get(
    "RAW_PATH",
    f"s3a://{BUCKET}/{PREFIX}/raw/regularite_tgv/extraction=2026-09/regularite-mensuelle-tgv-aqst.csv")
OUT = os.environ.get("PROCESSED_PATH", f"s3a://{BUCKET}/{PREFIX}/processed").rstrip("/")

# Périodes comparées (bornes incluses) et seuil de couverture
AVANT = ("2018-01", "2019-12")          # avant Covid
APRES = ("2024-01", "2025-12")          # deux dernières années civiles complètes
MIN_MOIS = 18                           # liaison retenue si >= 18 mois sur 24 dans CHAQUE période
# Réglages mesurés dans le rapport : 200 partitions de shuffle (défaut Spark) + écriture directe
# = 1 799 fichiers Parquet de ~15 Ko pour 12 907 lignes ; 8 partitions + repartition("annee") = 9 fichiers.
SHUFFLE_PARTITIONS = os.environ.get("SHUFFLE_PARTITIONS", "8")
SILVER_REPARTITION = os.environ.get("SILVER_REPARTITION", "1") == "1"

CAUSES = ["externe", "infra", "gestion_trafic", "materiel_roulant", "gestion_gare",
          "prise_en_charge_voyageurs"]

# Schéma explicite : 26 colonnes du CSV SNCF (évite l'inférence et le nom de colonne pollué par le BOM)
SCHEMA = StructType([
    StructField("date", StringType()), StructField("service", StringType()),
    StructField("gare_depart", StringType()), StructField("gare_arrivee", StringType()),
    StructField("duree_moyenne", IntegerType()), StructField("nb_train_prevu", IntegerType()),
    StructField("nb_annulation", IntegerType()), StructField("commentaire_annulation", StringType()),
    StructField("nb_train_depart_retard", IntegerType()), StructField("retard_moyen_depart", DoubleType()),
    StructField("retard_moyen_tous_trains_depart", DoubleType()),
    StructField("commentaire_retards_depart", StringType()),
    StructField("nb_train_retard_arrivee", IntegerType()), StructField("retard_moyen_arrivee", DoubleType()),
    StructField("retard_moyen_tous_trains_arrivee", DoubleType()),
    StructField("commentaires_retard_arrivee", StringType()),
    StructField("nb_train_retard_sup_15", IntegerType()),
    StructField("retard_moyen_trains_retard_sup15", DoubleType()),
    StructField("nb_train_retard_sup_30", IntegerType()), StructField("nb_train_retard_sup_60", IntegerType()),
] + [StructField(f"prct_cause_{c}", DoubleType()) for c in CAUSES])


def spark_session():
    b = (SparkSession.builder.appName("tgv-ponctualite-transform")
         .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS))
    if RAW_PATH.startswith("s3a://") or OUT.startswith("s3a://"):
        endpoint = os.environ.get("AWS_ENDPOINT_URL") or "https://" + os.environ["AWS_S3_ENDPOINT"]
        b = (b.config("spark.hadoop.fs.s3a.endpoint", endpoint.rstrip("/"))
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
              .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]))
        if os.environ.get("AWS_SESSION_TOKEN"):   # identifiants temporaires STS d'Onyxia
            b = (b.config("spark.hadoop.fs.s3a.session.token", os.environ["AWS_SESSION_TOKEN"])
                  .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"))
    return b.getOrCreate()


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}] {msg}", flush=True)


spark = spark_session()
spark.sparkContext.setLogLevel("ERROR")   # traces lisibles : nos propres messages + tableaux
log(f"Spark {spark.version} master={spark.sparkContext.master} | raw={RAW_PATH} | out={OUT}")

# ---------------------------------------------------------------- 1. lecture du brut
reader = (spark.read.option("header", True).option("sep", ";").option("quote", '"')
          .option("escape", '"').option("encoding", "UTF-8"))
raw = reader.option("multiLine", True).schema(SCHEMA).csv(RAW_PATH).cache()
n_raw = raw.count()
# Contrôle : lecture naïve ligne à ligne (multiLine=false) -> les commentaires multi-lignes cassent des lignes
n_naif = reader.option("multiLine", False).schema(SCHEMA).csv(RAW_PATH).count()
n_multiligne = raw.filter(F.col("commentaires_retard_arrivee").contains("\n")
                          | F.col("commentaire_annulation").contains("\n")
                          | F.col("commentaire_retards_depart").contains("\n")).count()
log(f"lignes lues : {n_raw} (lecture naïve sans multiLine : {n_naif})")

# ---------------------------------------------------------------- 2. règles de qualité
c = F.col
somme_causes = sum(F.coalesce(c(f"prct_cause_{k}"), F.lit(0.0)) for k in CAUSES)
df = (raw
      .withColumn("gare_depart", F.upper(F.trim(c("gare_depart"))))
      .withColumn("gare_arrivee", F.upper(F.trim(c("gare_arrivee"))))
      .withColumn("somme_causes", somme_causes)
      .withColumn("q_prevu_zero", c("nb_train_prevu") <= 0)
      .withColumn("q_annul_sup_prevu", c("nb_annulation") > c("nb_train_prevu"))
      .withColumn("q_retard_sup_circules",
                  c("nb_train_retard_arrivee") > c("nb_train_prevu") - c("nb_annulation"))
      .withColumn("q_sup15_sup_retard", c("nb_train_retard_sup_15") > c("nb_train_retard_arrivee"))
      .withColumn("q_sup30_sup60_incoherents",
                  (c("nb_train_retard_sup_30") < 0) | (c("nb_train_retard_sup_60") < 0)
                  | (c("nb_train_retard_sup_60") > c("nb_train_retard_sup_30"))
                  | (c("nb_train_retard_sup_30") > c("nb_train_retard_sup_15")))
      .withColumn("q_causes_vides", (c("somme_causes") == 0) & (c("nb_train_retard_arrivee") > 0))
      .withColumn("q_causes_somme_fausse", (c("somme_causes") > 0) & (F.abs(c("somme_causes") - 100) > 1))
      .withColumn("q_retard_moyen_negatif", c("retard_moyen_arrivee") < 0))
w_cle = Window.partitionBy("date", "gare_depart", "gare_arrivee")
df = df.withColumn("q_doublon_services", F.count(F.lit(1)).over(w_cle) > 1)

# Traitements appliqués (documentés dans le rapport qualité)
df = (df
      .withColumn("exploitable", ~(c("q_prevu_zero") | c("q_annul_sup_prevu") | c("q_retard_sup_circules")))
      .withColumn("nb_train_retard_sup_15", F.least(c("nb_train_retard_sup_15"), c("nb_train_retard_arrivee")))
      .withColumn("nb_train_retard_sup_30",
                  F.when(c("q_sup30_sup60_incoherents"), F.lit(None).cast("int")).otherwise(c("nb_train_retard_sup_30")))
      .withColumn("nb_train_retard_sup_60",
                  F.when(c("q_sup30_sup60_incoherents"), F.lit(None).cast("int")).otherwise(c("nb_train_retard_sup_60")))
      .withColumn("retard_moyen_arrivee",
                  F.when(c("q_retard_moyen_negatif"), F.lit(None).cast("double")).otherwise(c("retard_moyen_arrivee"))))
# Causes renormalisées à 100 % ; si aucune cause n'est renseignée -> part "non renseignée" = 100 %
for k in CAUSES:
    df = df.withColumn(f"prct_cause_{k}",
                       F.when(c("somme_causes") > 0, 100.0 * F.coalesce(c(f"prct_cause_{k}"), F.lit(0.0)) / c("somme_causes"))
                        .otherwise(F.lit(0.0)))
df = df.withColumn("prct_cause_non_renseignee", F.when(c("somme_causes") > 0, 0.0).otherwise(100.0))

silver = (df
          .withColumn("mois", F.to_date(F.concat(c("date"), F.lit("-01"))))
          .withColumn("annee", F.year("mois"))
          .withColumn("liaison", F.concat_ws(" -> ", "gare_depart", "gare_arrivee"))
          .withColumn("nb_train_circule", c("nb_train_prevu") - c("nb_annulation"))
          .drop("somme_causes")
          .cache())

regles = [
    ("R01", "BOM UTF-8 en tête de fichier", 1, "schéma explicite, ligne d'en-tête ignorée"),
    ("R02", "commentaires sur plusieurs lignes (lecture naïve)", n_multiligne,
     f"lecture multiLine=true ({n_raw} lignes au lieu de {n_naif})"),
]
flags = [
    ("R03", "q_prevu_zero", "aucun train prévu (mois Covid 2020)", "ligne exclue des taux"),
    ("R04", "q_annul_sup_prevu", "annulations > trains prévus", "ligne exclue des taux"),
    ("R05", "q_retard_sup_circules", "trains en retard > trains circulés", "ligne exclue des taux"),
    ("R06", "q_sup15_sup_retard", "retards >15 min > retards totaux", "plafonné aux retards totaux"),
    ("R07", "q_sup30_sup60_incoherents", "retards >30/>60 min négatifs ou non emboîtés (2025-01..03)",
     "colonnes >30/>60 mises à NULL"),
    ("R08", "q_causes_vides", "trains en retard mais aucune cause renseignée", "part « non renseignée » = 100 %"),
    ("R09", "q_causes_somme_fausse", "somme des 6 causes différente de 100 % (±1)", "causes renormalisées à 100 %"),
    ("R10", "q_doublon_services", "même liaison et même mois en National ET International (2025-07..09)",
     "sommés au niveau liaison-mois"),
    ("R11", "q_retard_moyen_negatif", "retard moyen des trains en retard < 0", "valeur mise à NULL"),
]
compte = silver.agg(*[F.sum(c(col).cast("int")).alias(col) for _, col, _, _ in flags]).first().asDict()
rows = regles + [(rid, desc, int(compte[col] or 0), trt) for rid, col, desc, trt in flags]

# ---------------------------------------------------------------- 3. liaison x mois (services sommés)
cause_cols = CAUSES + ["non_renseignee"]
lm = (silver.filter("exploitable")
      .withColumn("dans_avant", c("date").between(*AVANT))
      .withColumn("dans_apres", c("date").between(*APRES))
      .groupBy("liaison", "gare_depart", "gare_arrivee", "date", "annee", "dans_avant", "dans_apres")
      .agg(F.sum("nb_train_circule").alias("circ"), F.sum("nb_train_retard_arrivee").alias("ret"),
           F.sum("nb_train_retard_sup_15").alias("ret15"),
           *[F.sum(c("nb_train_retard_arrivee") * c(f"prct_cause_{k}") / 100.0).alias(f"ret_{k}") for k in cause_cols]))


def periode(nom, flag):
    p = (lm.filter(c(flag)).groupBy("liaison", "gare_depart", "gare_arrivee")
         .agg(F.countDistinct("date").alias(f"n_mois_{nom}"), F.sum("circ").alias(f"trains_circules_{nom}"),
              F.sum("ret").alias(f"trains_retard_{nom}"), F.sum("ret15").alias(f"trains_retard15_{nom}"),
              *[F.sum(f"ret_{k}").alias(f"_ret_{k}_{nom}") for k in cause_cols]))
    p = (p.withColumn(f"ponctualite_{nom}", 100.0 * (1 - c(f"trains_retard_{nom}") / c(f"trains_circules_{nom}")))
          .withColumn(f"taux_retard15_{nom}", 100.0 * c(f"trains_retard15_{nom}") / c(f"trains_circules_{nom}")))
    for k in cause_cols:   # points de retard attribués à chaque cause (somme = 100 - ponctualité)
        p = p.withColumn(f"pts_{k}_{nom}", 100.0 * c(f"_ret_{k}_{nom}") / c(f"trains_circules_{nom}")).drop(f"_ret_{k}_{nom}")
    return p


g = periode("avant", "dans_avant").join(periode("apres", "dans_apres"), ["liaison", "gare_depart", "gare_arrivee"], "full")
n_liaisons_total = g.count()
g = g.filter((c("n_mois_avant") >= MIN_MOIS) & (c("n_mois_apres") >= MIN_MOIS))
g = (g.withColumn("delta_ponctualite_pts", c("ponctualite_apres") - c("ponctualite_avant"))
      .withColumn("delta_retard15_pts", c("taux_retard15_apres") - c("taux_retard15_avant")))
for k in cause_cols:   # contribution de chaque cause à la hausse du taux de retard (somme = -delta_ponctualite)
    g = g.withColumn(f"contrib_{k}_pts", c(f"pts_{k}_apres") - c(f"pts_{k}_avant"))
contrib = F.array(*[F.struct(c(f"contrib_{k}_pts").alias("v"), F.lit(k).alias("k")) for k in cause_cols])
g = (g.withColumn("cause_principale", F.array_max(contrib)["k"])
      .withColumn("contrib_cause_principale_pts", F.array_max(contrib)["v"])
      .withColumn("rang_degradation", F.row_number().over(Window.orderBy(c("delta_ponctualite_pts").asc(), c("liaison")))))
gold_cols = (["rang_degradation", "liaison", "gare_depart", "gare_arrivee",
              "n_mois_avant", "trains_circules_avant", "trains_retard_avant", "ponctualite_avant",
              "n_mois_apres", "trains_circules_apres", "trains_retard_apres", "ponctualite_apres",
              "delta_ponctualite_pts", "taux_retard15_avant", "taux_retard15_apres", "delta_retard15_pts",
              "cause_principale", "contrib_cause_principale_pts"]
             + [f"contrib_{k}_pts" for k in cause_cols]
             + [f"pts_{k}_{p}" for p in ("avant", "apres") for k in cause_cols])
gold = g.select(*gold_cols).orderBy("rang_degradation").cache()
n_gold = gold.count()

annuel = (lm.groupBy("annee").agg(F.sum("circ").alias("trains_circules"), F.sum("ret").alias("trains_retard"),
                                  F.countDistinct("liaison").alias("n_liaisons"),
                                  *[F.sum(f"ret_{k}").alias(f"_r_{k}") for k in cause_cols])
          .withColumn("ponctualite", 100.0 * (1 - c("trains_retard") / c("trains_circules"))))
for k in cause_cols:
    annuel = annuel.withColumn(f"pts_{k}", 100.0 * c(f"_r_{k}") / c("trains_circules")).drop(f"_r_{k}")
annuel = annuel.orderBy("annee")

# ---------------------------------------------------------------- 4. écritures (overwrite = relance idempotente)
t_ecr = time.time()
silver_out = silver.drop("q_doublon_services")
if SILVER_REPARTITION:   # un fichier par année au lieu d'un par (partition de shuffle x année)
    silver_out = silver_out.repartition("annee")
silver_out.write.mode("overwrite").partitionBy("annee").parquet(f"{OUT}/silver/regularite_mensuelle")
t_silver = round(time.time() - t_ecr, 1)
gold.coalesce(1).write.mode("overwrite").parquet(f"{OUT}/gold/degradation_liaisons")
gold.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/gold/degradation_liaisons_csv")
annuel.coalesce(1).write.mode("overwrite").parquet(f"{OUT}/gold/ponctualite_annuelle")
qual = spark.createDataFrame(rows, "regle string, description string, lignes_touchees int, traitement string")
qual.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/quality/rapport_qualite")

info = {"fin_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "spark": spark.version,
        "master": spark.sparkContext.master, "raw": RAW_PATH, "lignes_brutes": n_raw,
        "lignes_exploitables": silver.filter("exploitable").count(), "liaisons_total": n_liaisons_total,
        "liaisons_comparees": n_gold, "periode_avant": AVANT, "periode_apres": APRES,
        "shuffle_partitions": SHUFFLE_PARTITIONS, "silver_repartition": SILVER_REPARTITION,
        "ecriture_silver_s": t_silver, "duree_s": round(time.time() - T0, 1)}
spark.createDataFrame([(json.dumps(info, ensure_ascii=False),)], "info string").coalesce(1) \
     .write.mode("overwrite").text(f"{OUT}/_run_info")

# ---------------------------------------------------------------- 5. résumé (trace d'exécution)
log("rapport qualité :")
qual.show(truncate=60)
log(f"{n_gold} liaisons comparées sur {n_liaisons_total} (>= {MIN_MOIS} mois dans chaque période)")
log("top 10 des liaisons qui ont le plus perdu en ponctualité (2018-2019 -> 2024-2025) :")
(gold.select("rang_degradation", "liaison", F.round("ponctualite_avant", 1).alias("ponct_18_19"),
             F.round("ponctualite_apres", 1).alias("ponct_24_25"), F.round("delta_ponctualite_pts", 1).alias("delta_pts"),
             "cause_principale", F.round("contrib_cause_principale_pts", 1).alias("contrib_pts"))
     .show(10, truncate=False))
log("contexte toutes liaisons (agrégat non officiel, un train peut compter dans plusieurs liaisons) :")
annuel.select("annee", "n_liaisons", "trains_circules", F.round("ponctualite", 2).alias("ponctualite")).show()
log(f"écriture silver : {t_silver} s (shuffle={SHUFFLE_PARTITIONS}, repartition={SILVER_REPARTITION})")
log(f"terminé en {info['duree_s']} s -> {OUT}")
spark.stop()
sys.exit(0)
