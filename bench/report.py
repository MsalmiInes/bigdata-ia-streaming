#!/usr/bin/env python3
"""
Phase 8 - Synthese du benchmark : lit bench/results/*.csv et spark/results/bench_*.json
et produit docs/benchmark_resultats.md + docs/figures/bench_*.png.

Usage (depuis la racine du projet) : bash bench/report.sh
Regle : pour chaque (jeu, etape), on retient la derniere mesure reussie (a defaut, la derniere tentative).
Aucun chiffre n'est invente : une valeur absente est affichee "-".
"""
import csv
import glob
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES = os.path.join(ROOT, "bench", "results")
SPK = os.path.join(ROOT, "spark", "results")
DOCS = os.path.join(ROOT, "docs")
FIG = os.path.join(DOCS, "figures")
ORDER = ["small", "medium", "large"]
PIG = ["pig_01_clean_ratings", "pig_02_clean_reference", "pig_03_clean_logs_events", "pig_04_transform"]
HIVE = ["hive_01_tables_csv", "hive_02_tables_orc_parquet", "hive_03_features_ml"]
RAM_GB = 8.0


def read_rows(name):
    path = os.path.join(RES, name)
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def last_good(rows, key, ok=lambda r: True):
    """Derniere ligne par cle ; une ligne reussie n'est jamais remplacee par un echec plus recent."""
    out = {}
    for r in rows:                                   # l'ordre du fichier = ordre chronologique
        k = key(r)
        cur = out.get(k)
        if cur is None or ok(r) or not ok(cur):
            out[k] = r
    return out


timings = last_good(read_rows("timings.csv"), lambda r: (r["size"], r["stage"]), lambda r: r["status"] == "ok")
sizes = last_good(read_rows("sizes.csv"), lambda r: r["size"])
counts = last_good(read_rows("counts.csv"), lambda r: (r["size"], r["table"]))
fmts = last_good(read_rows("formats.csv"), lambda r: (r["size"], r["metric"], r["variant"]))


def jload(pattern):
    out = {}
    for path in glob.glob(os.path.join(SPK, pattern)):
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
        out[(d["size"], d.get("workers"))] = d
    return out


als = jload("bench_als_*_*w.json")
pandas_res = {k[0]: v for k, v in jload("bench_pandas_*.json").items()}
present = [s for s in ORDER if s in sizes or any(k[0] == s for k in timings) or s in pandas_res]
if not present:
    sys.exit("Aucun resultat : lancez d'abord bash bench/run.sh")


def T(size, stage):
    r = timings.get((size, stage))
    return fnum(r["seconds"]) if r and r["status"] == "ok" else None


def total(size, stages):
    v = [T(size, s) for s in stages]
    return round(sum(v), 1) if all(x is not None for x in v) else None


def f(x, nd=1):
    return "-" if x is None else f"{x:,.{nd}f}".replace(",", " ")


def ratio(a, b):
    return None if a is None or b is None or b == 0 else a / b


def table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def vol(size, key="total_rows"):
    r = sizes.get(size)
    return fnum(r[key]) if r else None


L = []
w = L.append
w("# Benchmark : apport du Big Data (phase 8)\n")
w("Toutes les valeurs viennent des mesures de `bench/run.sh` ; \"-\" = non mesuré ou échec. "
  "Cluster simulé avec Docker sur un seul PC (Windows 11 + WSL2, 6 CPU, 8 Go).\n")

# ------------------------------------------------------------------ 1) volumes
w("## 1. Volumes de données\n")
w(table(["Jeu", "Lignes (7 tables)", "Notes", "CSV (Mo)"],
        [[s, f(vol(s), 0), f(vol(s, "ratings_rows"), 0), f(vol(s, "csv_mb"))] for s in present if s in sizes]))

# ------------------------------------------------------------------ 2) temps par etape
w("\n## 2. Temps d'exécution par étape (secondes)\n")
rows = []
labels = [("Chargement des CSV dans HDFS", "load")] + [(f"Pig {s[4:6]} : {s[7:]}", s) for s in PIG] + \
         [("**Pig : total (4 scripts)**", None)] + [(f"Hive {s[5:7]} : {s[8:]}", s) for s in HIVE] + \
         [("**Hive : total (3 scripts)**", None)]
for lab, st in labels:
    if st is None:
        stages = PIG if "Pig" in lab else HIVE
        rows.append([lab] + [f(total(s, stages)) for s in present])
    else:
        rows.append([lab] + [f(T(s, st)) for s in present])
w(table(["Étape"] + present, rows))

w("\n### Débit et croissance\n")
drows = []
for lab, stages in (("Pig", PIG), ("Hive", HIVE)):
    drows.append([f"{lab} : lignes traitées par seconde"] +
                 [f(ratio(vol(s), total(s, stages)), 0) for s in present])
w(table(["Indicateur"] + present, drows))
w("\nCroissance entre jeux consécutifs (volume ×V, temps ×T ; T < V signifie que le temps fixe de démarrage "
  "des jobs pèse moins quand le volume augmente) :\n")
grow = []
for a, b in zip(present, present[1:]):
    for lab, stages in (("Pig", PIG), ("Hive", HIVE)):
        v, t = ratio(vol(b), vol(a)), ratio(total(b, stages), total(a, stages))
        if v and t:
            grow.append([f"{a} → {b}", lab, f"×{v:.1f}", f"×{t:.1f}"])
w(table(["Passage", "Outil", "Volume", "Temps"], grow) if grow else "_Pas assez de jeux mesurés._")

# ------------------------------------------------------------------ 3) formats
w("\n## 3. Formats de stockage : CSV, ORC, Parquet\n")
w("Taille sur disque de la table `ratings` (une copie dans HDFS) :\n")
frows, fig_fmt = [], {}
for s in present:
    c, o, p = (fnum(fmts.get((s, "size_mb", f"ratings:{x}"), {}).get("value")) for x in ("csv", "orc", "parquet"))
    if c is not None:
        fig_fmt[s] = (c, o, p)
        frows.append([s, f(c, 2), f(o, 2), f(p, 2), f"×{c / o:.1f}" if o else "-", f"×{c / p:.1f}" if p else "-"])
w(table(["Jeu", "CSV (Mo)", "ORC (Mo)", "Parquet (Mo)", "CSV / ORC", "CSV / Parquet"], frows) if frows
  else "_Mesures absentes (phase formats)._")

w("\nTemps de requête Hive sur `ratings` (moyenne de plusieurs exécutions, durée de connexion beeline retirée) :\n")
qrows = []
for s in present:
    base = fnum(fmts.get((s, "baseline", "connexion"), {}).get("value")) or 0.0
    def net(metric, var):
        v = fnum(fmts.get((s, metric, var), {}).get("value"))
        return None if v is None else max(v - base, 0.0)
    for var in ("csv", "orc", "parquet"):
        a, b = net("q_filter", var), net("q_group", var)
        if a is not None or b is not None:
            qrows.append([s, var, f(a), f(b)])
    p1, p2 = net("prune", "partition"), net("prune", "sans_partition")
    if p1 is not None or p2 is not None:
        qrows.append([s, "ORC, filtre sur la partition (année)", f(p1), "-"])
        qrows.append([s, "ORC, même filtre sans la partition", f(p2), "-"])
w(table(["Jeu", "Format / cas", "A : filtre + moyenne (s)", "B : regroupement par film (s)"], qrows)
  if qrows else "_Mesures absentes (phase formats)._")

# ------------------------------------------------------------------ 4) ALS
w("\n## 4. Spark ALS : 1 worker contre 2 workers\n")
w("Même demande à chaque exécution (4 cœurs demandés, 2 par exécuteur, 1 Go chacun, 10 itérations, rang 12). "
  "Avec 1 worker Spark n'obtient que 2 cœurs, avec 2 workers il en obtient 4. "
  "Temps d'entraînement : moyenne des répétitions, après un échauffement non compté.\n")
arows, fig_als = [], {}
for s in present:
    a1, a2 = als.get((s, 1)), als.get((s, 2))
    if not (a1 or a2):
        continue
    t1, t2 = (a["fit_s_mean"] if a else None for a in (a1, a2))
    sp = ratio(t1, t2)
    fig_als[s] = (t1, t2, sp)
    arows.append([s, f((a1 or a2)["n_train"], 0), f(t1), f(t2), f"×{sp:.2f}" if sp else "-",
                  f"{100 * sp / 2:.0f} %" if sp else "-",
                  f"{a1['rmse']:.4f}" if a1 else "-", f"{a2['rmse']:.4f}" if a2 else "-",
                  f"{a1['executors']} / {a2['executors']}" if a1 and a2 else "-"])
w(table(["Jeu", "Notes (train)", "1 worker (s)", "2 workers (s)", "Accélération", "Efficacité",
         "RMSE 1w", "RMSE 2w", "Exécuteurs 1w / 2w"], arows) if arows else "_Mesures absentes (phase spark)._")
rrows = []
for s in present:
    a1, a2 = als.get((s, 1)), als.get((s, 2))
    if a1 or a2:
        rrows.append([s, f((a1 or {}).get("read_cache_s")), f((a2 or {}).get("read_cache_s")),
                      f((a1 or {}).get("reco_s")), f((a2 or {}).get("reco_s"))])
if rrows:
    w("\nLecture ORC + mise en cache, et génération du top-5 de tous les utilisateurs :\n")
    w(table(["Jeu", "Lecture 1w (s)", "Lecture 2w (s)", "Top-5 1w (s)", "Top-5 2w (s)"], rrows))

# ------------------------------------------------------------------ 5) pandas
w("\n## 5. Cluster ou une seule machine ? Pig contre pandas\n")
w("Même nettoyage et mêmes transformations que les scripts Pig 01 à 04, avec pandas en mémoire. "
  "Les nombres de lignes propres sont comparés à ceux de Pig.\n")
prow, fig_pd = [], {}
for s in present:
    p = pandas_res.get(s)
    if not p:
        continue
    pt, pg = p["total_s"], total(s, PIG)
    fig_pd[s] = pt
    same = []
    for t in ("ratings", "watch_logs", "users", "movies"):
        c = counts.get((s, t))
        if c and c["pig_clean"] != "":
            same.append(c["pig_clean"] == str(p["clean_rows"].get(t)))
    prow.append([s, f(pt), f(pg), f"×{ratio(pg, pt):.0f}" if ratio(pg, pt) else "-", f(p["peak_rss_mb"], 0),
                 "identiques" if same and all(same) else ("DIFFÉRENTS" if same else "-")])
w(table(["Jeu", "pandas (s)", "Pig (s)", "Pig / pandas", "Mémoire max pandas (Mo)", "Lignes propres pandas = Pig"], prow)
  if prow else "_Mesures absentes (phase spark)._")

# ------------------------------------------------------------------ 6) lecture automatique
w("\n## 6. Lecture des résultats\n")
notes = []
for a, b in zip(present, present[1:]):
    v, tp = ratio(vol(b), vol(a)), ratio(total(b, PIG), total(a, PIG))
    if v and tp:
        notes.append(f"**Pig, {a} → {b}** : le volume est multiplié par {v:.1f}, le temps par {tp:.1f}. "
                     + ("Le temps croît moins vite que le volume : le coût fixe de démarrage des jobs MapReduce domine encore."
                        if tp < v * 0.8 else "Le temps croît à peu près comme le volume : le coût de traitement des données domine."))
    v, th = ratio(vol(b), vol(a)), ratio(total(b, HIVE), total(a, HIVE))
    if v and th:
        notes.append(f"**Hive, {a} → {b}** : volume ×{v:.1f}, temps ×{th:.1f}.")
for s in present:
    pg, pt = total(s, PIG), fig_pd.get(s)
    if pg and pt:
        if pg > pt:
            notes.append(f"**{s}** : pandas traite le même nettoyage {pg / pt:.0f} fois plus vite que Pig "
                         f"({pt:.1f} s contre {pg:.0f} s). À ce volume une seule machine suffit : "
                         "le cluster n'apporte pas de gain de vitesse.")
        else:
            notes.append(f"**{s}** : Pig traite le même nettoyage {pt / pg:.1f} fois plus vite que pandas "
                         f"({pg:.0f} s contre {pt:.1f} s) : le cluster commence à être rentable.")
    sp = fig_als.get(s, (None, None, None))[2]
    if sp:
        notes.append(f"**ALS, {s}** : 2 workers donnent une accélération de ×{sp:.2f} "
                     f"(efficacité {100 * sp / 2:.0f} %). "
                     + ("Le gain reste limité : les 2 workers partagent les mêmes CPU et le même disque, "
                        "et les échanges entre exécuteurs coûtent du temps." if sp < 1.5 else
                        "Le calcul se répartit bien entre les exécuteurs."))
    if s in fig_fmt and fig_fmt[s][1]:
        notes.append(f"**Stockage, {s}** : ORC occupe {fig_fmt[s][0] / fig_fmt[s][1]:.1f} fois moins de place que le CSV"
                     + (f", Parquet {fig_fmt[s][0] / fig_fmt[s][2]:.1f} fois moins." if fig_fmt[s][2] else "."))
pts = [(vol(s, "ratings_rows"), pandas_res[s]["peak_rss_mb"]) for s in present if s in pandas_res and vol(s, "ratings_rows")]
if len(pts) >= 2:
    (x1, y1), (x2, y2) = pts[-2], pts[-1]
    if x2 > x1 and y2 > y1:
        slope = (y2 - y1) / (x2 - x1)
        limit = x2 + (RAM_GB * 1024 * 0.7 - y2) / slope
        notes.append(f"**Limite d'une seule machine (extrapolation linéaire)** : pandas utilise {y2:.0f} Mo pour "
                     f"{x2 / 1e6:.1f} M de notes ; en extrapolant, 70 % des {RAM_GB:.0f} Go de ce PC seraient atteints "
                     f"vers {limit / 1e6:.0f} millions de notes. C'est au-delà de ce seuil, ou quand les données ne tiennent "
                     "plus sur une machine, que Pig, Hive et Spark deviennent nécessaires. Ordre de grandeur, pas une mesure.")
w("\n".join(f"- {n}" for n in notes) if notes else "_Pas assez de mesures pour une lecture automatique._")

w("\n## 7. Limites de ce benchmark\n")
w("- Le « cluster » est simulé sur un seul PC : les workers partagent CPU, mémoire et disque. Les gains de vitesse "
  "mesurés ne sont donc pas ceux d'un vrai cluster de plusieurs machines.\n"
  "- Données synthétiques, avec environ 1 % de lignes sales ajoutées volontairement.\n"
  "- Peu de répétitions (3 pour ALS comme pour Hive) : les écarts de quelques pourcents ne sont pas significatifs.\n"
  "- Les temps Hive de la section 3 incluent le démarrage de la session beeline, retiré par soustraction d'une requête `SELECT 1`.\n")

out_md = os.path.join(DOCS, "benchmark_resultats.md")
os.makedirs(DOCS, exist_ok=True)
with open(out_md, "w", encoding="utf-8") as fh:
    fh.write("\n".join(L) + "\n")
print("\n".join(L))
print(f"\nSauvegarde : docs/benchmark_resultats.md")

# ------------------------------------------------------------------ graphiques
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as exc:
    print(f"(graphiques ignores : matplotlib indisponible : {exc})")
    sys.exit(0)
os.makedirs(FIG, exist_ok=True)
C = {"pig": "#4C72B0", "hive": "#55A868", "als": "#C44E52", "pd": "#8172B2"}

# 1) temps par etape et par jeu
series = [("Pig (4 scripts)", [total(s, PIG) for s in present], C["pig"]),
          ("Hive (3 scripts)", [total(s, HIVE) for s in present], C["hive"]),
          ("ALS, 1 worker", [fig_als.get(s, (None,))[0] for s in present], C["als"]),
          ("pandas, 1 machine", [fig_pd.get(s) for s in present], C["pd"])]
series = [(lab, [v if v and v > 0 else None for v in vals], col) for lab, vals, col in series]   # echelle log : pas de 0
series = [x for x in series if any(v is not None for v in x[1])]
if series:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    wd = 0.8 / len(series)
    for i, (lab, vals, col) in enumerate(series):
        first = True
        for j, v in enumerate(vals):
            if v is None:                              # etape non mesuree ou en echec : pas de barre
                continue
            x = j + (i - (len(series) - 1) / 2) * wd
            ax.bar([x], [v], wd * 0.95, label=lab if first else None, color=col)
            ax.text(x, v * 1.05, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
            first = False
    ax.set_xticks(range(len(present)))
    ax.set_xticklabels([f"{s}\n({vol(s, 'ratings_rows') / 1e6:.1f} M notes)" if vol(s, "ratings_rows") else s for s in present])
    ax.set_yscale("log"); ax.set_ylabel("secondes (échelle logarithmique)")
    ax.set_title("Temps par étape selon le volume de données"); ax.grid(axis="y", alpha=0.3)
    pos = [v for _, vals, _ in series for v in vals if v is not None]
    ax.set_ylim(min(pos) / 2, max(pos) * 4)
    ax.legend(loc="upper left", ncol=2, fontsize=9, frameon=False)
    fig.tight_layout(); fig.savefig(os.path.join(FIG, "bench_temps_etapes.png"), dpi=150); plt.close(fig)

# 2) formats
if fig_fmt:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    names = ["CSV", "ORC", "Parquet"]; cols = ["#999999", C["hive"], C["pig"]]
    for i, (n, col) in enumerate(zip(names, cols)):
        xs = [j + (i - 1) * 0.27 for j in range(len(fig_fmt))]
        ys = [v[i] if v[i] is not None else 0 for v in fig_fmt.values()]
        bars = ax.bar(xs, ys, 0.26, label=n, color=col)
        for b, v in zip(bars, ys):
            if v:
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(fig_fmt))); ax.set_xticklabels(list(fig_fmt))
    ax.set_ylabel("Mo (table ratings)"); ax.set_title("Taille sur disque : CSV, ORC, Parquet")
    ax.set_ylim(top=max(v for vv in fig_fmt.values() for v in vv if v) * 1.3)
    ax.legend(loc="upper left", frameon=False); ax.grid(axis="y", alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(FIG, "bench_formats.png"), dpi=150); plt.close(fig)

# 3) ALS 1 vs 2 workers
if fig_als:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    keys = list(fig_als)
    for i, (lab, col) in enumerate((("1 worker", C["als"]), ("2 workers", C["hive"]))):
        xs = [j + (i - 0.5) * 0.36 for j in range(len(keys))]
        ys = [fig_als[k][i] if fig_als[k][i] is not None else 0 for k in keys]
        bars = ax.bar(xs, ys, 0.34, label=lab, color=col)
        for b, v in zip(bars, ys):
            if v:
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f} s", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(keys)))
    ax.set_xticklabels([f"{k}\naccélération ×{fig_als[k][2]:.2f}" if fig_als[k][2] else k for k in keys])
    ax.set_ylabel("temps d'entraînement ALS (s)"); ax.set_title("ALS : 1 worker contre 2 workers")
    ax.set_ylim(top=max(v for vv in fig_als.values() for v in vv[:2] if v) * 1.3)
    ax.legend(loc="upper left", frameon=False); ax.grid(axis="y", alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(FIG, "bench_als_workers.png"), dpi=150); plt.close(fig)
print("Graphiques : " + ", ".join(sorted(os.path.basename(p) for p in glob.glob(os.path.join(FIG, "bench_*.png")))))
