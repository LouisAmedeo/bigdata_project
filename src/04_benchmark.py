"""Étape 5 : mesures pour justifier les choix techniques (lancé par src/04_benchmark.sh).

Même calcul pour chaque moteur : ponctualité par liaison sur 2018-2019 et 2024-2025, variation,
liaison la plus dégradée. Moteurs : pandas, DuckDB, Spark local[2] ; formats : CSV brut, Parquet ;
échelles : x1, x10, x100 (lignes du brut répliquées : mêmes résultats attendus, volume multiplié).
Chaque mesure tourne dans un sous-processus neuf (3 passages, médiane) avec le pic de mémoire
résidente (processus + enfants, la JVM pour Spark) échantillonné par psutil.

Usage (dans le Job k8s/03-job-benchmark.yaml, via src/04_benchmark.sh) :
  python 04_benchmark.py <dossier_de_travail> <csv_brut> <silver_parquet_dir> <sortie.csv>
"""
import csv
import json
import os
import statistics
import subprocess
import sys
import threading
import time

COLS = ["date", "gare_depart", "gare_arrivee", "nb_train_prevu", "nb_annulation", "nb_train_retard_arrivee"]
REPET = 3


# ------------------------------------------------------------------ calcul, une version par moteur
def calc_pandas(path, fmt):
    import numpy as np
    import pandas as pd
    if fmt == "csv":
        df = pd.read_csv(path, sep=";", usecols=COLS, encoding="utf-8-sig")
    else:
        df = pd.read_parquet(path, columns=COLS)
    circ = df.nb_train_prevu - df.nb_annulation
    df = df[(df.nb_train_prevu > 0) & (df.nb_annulation <= df.nb_train_prevu) & (df.nb_train_retard_arrivee <= circ)]
    df = df.assign(circ=df.nb_train_prevu - df.nb_annulation,
                   periode=np.select([df.date.between("2018-01", "2019-12"), df.date.between("2024-01", "2025-12")],
                                     ["avant", "apres"], ""),
                   liaison=df.gare_depart.str.strip().str.upper() + " -> " + df.gare_arrivee.str.strip().str.upper())
    df = df[df.periode != ""]
    g = df.groupby(["liaison", "periode"]).agg(n_mois=("date", "nunique"), circ=("circ", "sum"),
                                               ret=("nb_train_retard_arrivee", "sum"))
    g["ponct"] = 100 * (1 - g.ret / g.circ)
    w = g.unstack("periode")
    w = w[(w[("n_mois", "avant")] >= 18) & (w[("n_mois", "apres")] >= 18)]
    d = (w[("ponct", "apres")] - w[("ponct", "avant")]).sort_values()
    return len(d), d.index[0], float(d.iloc[0])


SQL = """
WITH b AS (SELECT date, upper(trim(gare_depart)) || ' -> ' || upper(trim(gare_arrivee)) AS liaison,
                  nb_train_prevu - nb_annulation AS circ, nb_train_retard_arrivee AS ret,
                  CASE WHEN date BETWEEN '2018-01' AND '2019-12' THEN 'avant'
                       WHEN date BETWEEN '2024-01' AND '2025-12' THEN 'apres' END AS periode
           FROM {src}
           WHERE nb_train_prevu > 0 AND nb_annulation <= nb_train_prevu
             AND nb_train_retard_arrivee <= nb_train_prevu - nb_annulation),
p AS (SELECT liaison, periode, count(DISTINCT date) AS n_mois, 100 * (1 - sum(ret) / sum(circ)) AS ponct
      FROM b WHERE periode IS NOT NULL GROUP BY ALL)
SELECT count(*) OVER () AS n, a.liaison, b.ponct - a.ponct AS delta
FROM p a JOIN p b ON a.liaison = b.liaison AND a.periode = 'avant' AND b.periode = 'apres'
WHERE a.n_mois >= 18 AND b.n_mois >= 18 ORDER BY delta, a.liaison LIMIT 1
"""


def calc_duckdb(path, fmt):
    import duckdb
    con = duckdb.connect()
    con.execute("SET threads = 2")   # même budget CPU que Spark local[2] (limite du pod)
    src = (f"read_csv('{path}', delim=';', header=true, quote='\"')" if fmt == "csv"
           else f"read_parquet('{path}/**/*.parquet')")
    n, liaison, delta = con.execute(SQL.format(src=src)).fetchone()
    con.close()
    return n, liaison, float(delta)


_SPARK = None


def calc_spark(path, fmt):
    from pyspark.sql import functions as F
    c = F.col
    if fmt == "csv":
        df = (_SPARK.read.option("header", True).option("sep", ";").option("quote", '"').option("escape", '"')
              .option("multiLine", True).option("inferSchema", False).csv(path).select(*COLS))
        for k in COLS[3:]:
            df = df.withColumn(k, c(k).cast("int"))
    else:
        df = _SPARK.read.parquet(path).select(*COLS)
    df = (df.filter((c("nb_train_prevu") > 0) & (c("nb_annulation") <= c("nb_train_prevu"))
                    & (c("nb_train_retard_arrivee") <= c("nb_train_prevu") - c("nb_annulation")))
            .withColumn("periode", F.when(c("date").between("2018-01", "2019-12"), "avant")
                                    .when(c("date").between("2024-01", "2025-12"), "apres"))
            .filter(c("periode").isNotNull())
            .withColumn("liaison", F.concat_ws(" -> ", F.upper(F.trim("gare_depart")), F.upper(F.trim("gare_arrivee"))))
            .groupBy("liaison", "periode")
            .agg(F.countDistinct("date").alias("n_mois"),
                 (100 * (1 - F.sum("nb_train_retard_arrivee") / F.sum(c("nb_train_prevu") - c("nb_annulation")))).alias("ponct")))
    a, b = df.filter("periode = 'avant'").alias("a"), df.filter("periode = 'apres'").alias("b")
    j = (a.join(b, "liaison").filter((c("a.n_mois") >= 18) & (c("b.n_mois") >= 18))
          .select("liaison", (c("b.ponct") - c("a.ponct")).alias("delta")).orderBy("delta", "liaison").collect())
    return len(j), j[0]["liaison"], float(j[0]["delta"])


# ------------------------------------------------------------------ une mesure (sous-processus)
def une_mesure(moteur, fmt, path):
    import psutil
    global _SPARK
    proc, pic, stop = psutil.Process(), [0], threading.Event()

    def echantillonne():
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss + sum(ch.memory_info().rss for ch in proc.children(recursive=True))
                pic[0] = max(pic[0], rss)
            except psutil.Error:
                pass
            time.sleep(0.05)

    th = threading.Thread(target=echantillonne, daemon=True)
    th.start()
    demarrage = 0.0
    if moteur == "spark":
        # le tas de la JVM se fixe au lancement : builder.config("spark.driver.memory") arriverait trop tard
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 1g pyspark-shell"
        t = time.perf_counter()
        from pyspark.sql import SparkSession
        _SPARK = (SparkSession.builder.master("local[2]").appName("bench").config("spark.ui.enabled", "false")
                  .config("spark.sql.shuffle.partitions", "8").config("spark.driver.memory", "1g").getOrCreate())
        _SPARK.sparkContext.setLogLevel("ERROR")
        demarrage = time.perf_counter() - t
    f = {"pandas": calc_pandas, "duckdb": calc_duckdb, "spark": calc_spark}[moteur]
    temps, res = [], None
    for _ in range(REPET):
        t = time.perf_counter()
        res = f(path, fmt)
        temps.append(time.perf_counter() - t)
    if _SPARK is not None:
        _SPARK.stop()
    stop.set()
    th.join()
    print(json.dumps({"premier_s": temps[0], "median_s": statistics.median(temps), "demarrage_s": demarrage,
                      "pic_rss_mo": pic[0] / 2**20, "n_liaisons": res[0], "top1": res[1], "delta_top1": res[2]}))


# ------------------------------------------------------------------ orchestration
def taille(p):
    if os.path.isfile(p):
        return os.path.getsize(p)
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(p) for f in fs if not f.startswith(("_", ".")))


def replique(csv_brut, n, dest):
    """Fichier xN : en-tête + N copies des lignes de données (champs multi-lignes compris, copie d'octets)."""
    if n == 1:
        return csv_brut
    with open(csv_brut, "rb") as f:
        entete = f.readline()
        corps = f.read()
    if not corps.endswith(b"\n"):
        corps += b"\n"
    with open(dest, "wb") as o:
        o.write(entete)
        for _ in range(n):
            o.write(corps)
    return dest


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--une":
        return une_mesure(sys.argv[2], sys.argv[3], sys.argv[4])
    work, csv_brut, silver, sortie = sys.argv[1:5]
    import duckdb
    os.makedirs(work, exist_ok=True)
    lignes = []
    plan = [(1, "pandas"), (1, "duckdb"), (1, "spark"), (10, "pandas"), (10, "duckdb"), (10, "spark"),
            (100, "pandas"), (100, "duckdb"), (100, "spark")]
    fichiers = {}
    for n in (1, 10, 100):
        t = time.perf_counter()
        c = replique(csv_brut, n, os.path.join(work, f"brut_x{n}.csv"))
        if n == 1:
            p = silver   # x1 : le vrai silver produit par Spark (Parquet partitionné par année)
        else:        # xN : conversion CSV -> Parquet par DuckDB (mesurée)
            p = os.path.join(work, f"parquet_x{n}")
            os.makedirs(p, exist_ok=True)
            duckdb.sql(f"COPY (SELECT * FROM read_csv('{c}', delim=';', header=true, quote='\"')) "
                       f"TO '{p}/data.parquet' (FORMAT parquet)")
        fichiers[n] = (c, p, time.perf_counter() - t)
        print(f"x{n} : CSV {taille(c)/2**20:.1f} Mo, Parquet {taille(p)/2**20:.1f} Mo (préparation {fichiers[n][2]:.1f} s)", flush=True)
    for n, moteur in plan:
        for fmt in ("csv", "parquet"):
            path = fichiers[n][0] if fmt == "csv" else fichiers[n][1]
            r = subprocess.run([sys.executable, __file__, "--une", moteur, fmt, path], capture_output=True, text=True)
            if r.returncode != 0:
                mes = {"erreur": (r.stderr.strip().splitlines() or ["?"])[-1][:120]}
            else:
                mes = json.loads(r.stdout.strip().splitlines()[-1])
            ligne = {"echelle": f"x{n}", "moteur": moteur, "format": fmt, "taille_mo": round(taille(path) / 2**20, 2),
                     **{k: (round(v, 3) if isinstance(v, float) else v) for k, v in mes.items()}}
            lignes.append(ligne)
            print(json.dumps(ligne, ensure_ascii=False), flush=True)
    champs = ["echelle", "moteur", "format", "taille_mo", "premier_s", "median_s", "demarrage_s", "pic_rss_mo",
              "n_liaisons", "top1", "delta_top1", "erreur"]
    with open(sortie, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=champs)
        w.writeheader()
        for ligne in lignes:
            w.writerow({k: ligne.get(k, "") for k in champs})


if __name__ == "__main__":
    main()
