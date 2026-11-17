"""Figure du rapport : les 10 liaisons qui ont le plus perdu en ponctualité (2018-19 -> 2024-25).

Recalcule le classement depuis le CSV brut local (mêmes règles que Spark), vérifie que le n°1
redonne bien -23,011 pts, puis écrit docs/top10_degradation.pdf (vectoriel, pour LaTeX/Overleaf).

Usage :
    bash src/00_download.sh                # si le brut n'est pas encore en local
    python3 src/06_figure_top10.py         # besoin de matplotlib
"""
import collections
import csv
import glob

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

AVANT, APRES, MIN_MOIS = ("2018-01", "2019-12"), ("2024-01", "2025-12"), 18
CAUSES = ["externe", "infra", "gestion_trafic", "materiel_roulant", "gestion_gare",
          "prise_en_charge_voyageurs"]
NOMS = {"externe": "externe", "infra": "infrastructure", "gestion_trafic": "gestion du trafic",
        "materiel_roulant": "matériel roulant", "gestion_gare": "gestion en gare",
        "prise_en_charge_voyageurs": "prise en charge voyageurs", "nr": "non renseignée"}

fichier = sorted(glob.glob("data/raw/regularite-mensuelle-tgv-aqst_*.csv"))[-1]
with open(fichier, encoding="utf-8-sig", newline="") as f:
    lignes = list(csv.DictReader(f, delimiter=";"))


def num(x):
    return float(x) if x.strip() else 0.0


# cumuls par liaison et par période (mêmes exclusions que Spark : R03, R04, R05)
agg = collections.defaultdict(lambda: collections.defaultdict(
    lambda: {"circ": 0, "ret": 0, "mois": set(), **{k: 0.0 for k in CAUSES + ["nr"]}}))
for r in lignes:
    prevu, annul, ret = int(r["nb_train_prevu"]), int(r["nb_annulation"]), int(r["nb_train_retard_arrivee"])
    if prevu <= 0 or annul > prevu or ret > prevu - annul:
        continue
    d = r["date"]
    per = "av" if AVANT[0] <= d <= AVANT[1] else "ap" if APRES[0] <= d <= APRES[1] else None
    if per is None:
        continue
    g = agg[(r["gare_depart"].strip().upper(), r["gare_arrivee"].strip().upper())][per]
    g["circ"] += prevu - annul
    g["ret"] += ret
    g["mois"].add(d)
    s = sum(num(r[f"prct_cause_{k}"]) for k in CAUSES)
    for k in CAUSES:   # retards répartis au prorata des causes (renormalisées)
        g[k] += ret * num(r[f"prct_cause_{k}"]) / s if s > 0 else 0
    g["nr"] += 0 if s > 0 else ret

res = []
for (dep, arr), v in agg.items():
    if len(v["av"]["mois"]) < MIN_MOIS or len(v["ap"]["mois"]) < MIN_MOIS:
        continue
    p_av = 100 * (1 - v["av"]["ret"] / v["av"]["circ"])
    p_ap = 100 * (1 - v["ap"]["ret"] / v["ap"]["circ"])
    contrib = {k: 100 * v["ap"][k] / v["ap"]["circ"] - 100 * v["av"][k] / v["av"]["circ"] for k in CAUSES + ["nr"]}
    cause = max(contrib, key=contrib.get)
    res.append((p_ap - p_av, dep, arr, p_av, p_ap, cause, contrib[cause]))
res.sort()
assert len(res) == 121 and round(res[0][0], 3) == -23.011, "classement différent du gold Spark"
top = res[:10]


def joli(gare):   # PARIS EST -> Paris Est, MARNE LA VALLEE -> Marne la Vallee
    return " ".join(m.lower() if m in ("LA", "LES", "DE", "DU") else m.capitalize() for m in gare.split())


# ---------------------------------------------------------------- figure (format impression)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8})
fig, ax = plt.subplots(figsize=(7.1, 2.35))
y = range(len(top))[::-1]
pertes = [-t[0] for t in top]
ax.barh(list(y), pertes, height=0.62, color="#3b6ea8")
for yi, t, p in zip(y, top, pertes):
    ax.text(p + 0.3, yi, f"−{p:.1f} pts ({t[3]:.1f} → {t[4]:.1f} %) · {NOMS[t[5]]} +{t[6]:.1f}".replace(".", ","),
            va="center", fontsize=7.3, color="#222")
ax.set_yticks(list(y))
ax.set_yticklabels([f"{joli(t[1])} → {joli(t[2])}" for t in top], fontsize=7.6)
ax.set_xlim(0, 46)
ax.set_xticks(range(0, 30, 5))   # graduations jusqu'à 25, la place à droite sert aux étiquettes
ax.set_xlabel("perte de ponctualité à l'arrivée, en points (2018-2019 → 2024-2025)", fontsize=7.6, color="#444")
ax.xaxis.grid(True, color="#e3e3e3", linewidth=0.6)
ax.set_axisbelow(True)
for s in ("top", "right", "left"):
    ax.spines[s].set_visible(False)
ax.spines["bottom"].set_color("#999")
ax.tick_params(axis="y", length=0)
ax.tick_params(axis="x", colors="#555", labelsize=7)
fig.tight_layout(pad=0.3)
fig.savefig("docs/top10_degradation.pdf")
print("docs/top10_degradation.pdf écrit ; n°1 :", joli(top[0][1]), "->", joli(top[0][2]), round(top[0][0], 3))
