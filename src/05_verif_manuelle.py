"""Vérif à la main d'un résultat du gold, en Python pur (module csv, pas de Spark/DuckDB/pandas).

On prend une liaison (par défaut la n°1 du classement), on liste ses mois dans le CSV brut,
on refait le calcul ligne par ligne et on compare à la valeur trouvée par Spark (capture 05).

Usage :
    bash src/00_download.sh                      # si le brut n'est pas encore en local
    python3 src/05_verif_manuelle.py             # liaison PARIS EST -> STUTTGART
    python3 src/05_verif_manuelle.py "GRENOBLE -> PARIS LYON" -6.965
"""
import csv
import glob
import hashlib
import sys

LIAISON = sys.argv[1] if len(sys.argv) > 1 else "PARIS EST -> STUTTGART"
ATTENDU = float(sys.argv[2]) if len(sys.argv) > 2 else -23.011   # delta_spark de la capture 05 (arrondi)
AVANT = ("2018-01", "2019-12")
APRES = ("2024-01", "2025-12")
CAUSES = ["externe", "infra", "gestion_trafic", "materiel_roulant", "gestion_gare",
          "prise_en_charge_voyageurs"]

fichier = sorted(glob.glob("data/raw/regularite-mensuelle-tgv-aqst_*.csv"))[-1]
sha = hashlib.sha256(open(fichier, "rb").read()).hexdigest()
print(f"brut : {fichier}\nsha256 : {sha}")

# utf-8-sig pour virer le BOM, le module csv gère les commentaires sur plusieurs lignes
with open(fichier, encoding="utf-8-sig", newline="") as f:
    lignes = list(csv.DictReader(f, delimiter=";"))
print(f"{len(lignes)} lignes lues\n")

dep, arr = [s.strip() for s in LIAISON.split("->")]


def num(x):
    return float(x) if x.strip() else 0.0


def calcul(nom, debut, fin):
    circ = ret = 0
    ret_cause = dict.fromkeys(CAUSES + ["non_renseignee"], 0.0)
    mois = set()
    print(f"--- période {nom} ({debut} -> {fin})")
    print(f"{'mois':8} {'service':13} {'prévus':>6} {'annul':>5} {'circulés':>8} {'retards':>7}  remarque")
    for r in lignes:
        if r["gare_depart"].strip().upper() != dep or r["gare_arrivee"].strip().upper() != arr:
            continue
        if not debut <= r["date"] <= fin:
            continue
        prevu, annul, retard = int(r["nb_train_prevu"]), int(r["nb_annulation"]), int(r["nb_train_retard_arrivee"])
        # mêmes règles que Spark : on vire les lignes incohérentes (R03, R04, R05)
        if prevu <= 0 or annul > prevu or retard > prevu - annul:
            print(f"{r['date']:8} {r['service']:13} {prevu:6} {annul:5} {'-':>8} {retard:7}  EXCLUE (incohérente)")
            continue
        circ += prevu - annul
        ret += retard
        mois.add(r["date"])
        # retards répartis par cause, % renormalisés à 100 (R08, R09)
        s = sum(num(r[f"prct_cause_{k}"]) for k in CAUSES)
        for k in CAUSES:
            ret_cause[k] += retard * (num(r[f"prct_cause_{k}"]) / s if s > 0 else 0)
        ret_cause["non_renseignee"] += 0 if s > 0 else retard
        print(f"{r['date']:8} {r['service']:13} {prevu:6} {annul:5} {prevu - annul:8} {retard:7}")
    ponct = 100 * (1 - ret / circ)
    print(f"=> {len(mois)} mois, {circ} trains circulés, {ret} en retard -> ponctualité = {ponct:.3f} %\n")
    return ponct, {k: 100 * v / circ for k, v in ret_cause.items()}


p_av, pts_av = calcul("avant", *AVANT)
p_ap, pts_ap = calcul("après", *APRES)
delta = p_ap - p_av

print(f"liaison : {LIAISON}")
print(f"ponctualité 2018-2019 : {p_av:.3f} %   2024-2025 : {p_ap:.3f} %")
print(f"delta = {p_ap:.3f} - {p_av:.3f} = {delta:.3f} points\n")

print("contribution de chaque cause à la baisse (points de retard en plus) :")
contrib = {k: pts_ap[k] - pts_av[k] for k in pts_av}
for k, v in sorted(contrib.items(), key=lambda x: -x[1]):
    print(f"  {k:28} {v:+7.3f}")
print(f"  {'total':28} {sum(contrib.values()):+7.3f}  (= -delta)\n")

ecart = abs(round(delta, 3) - ATTENDU)
print(f"Spark (capture 05) : {ATTENDU}   à la main : {delta:.3f}   écart : {ecart:.3f}")
if ecart > 0.001:
    sys.exit("VERIF MANUELLE KO")
print("VERIF MANUELLE OK : même résultat que Spark et DuckDB")
