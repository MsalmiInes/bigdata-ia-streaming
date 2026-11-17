#!/usr/bin/env python3
"""Phase 6 - Synthese des resultats Spark (lit spark/results/*.json) -> affichage + docs/spark_resultats.txt.
Usage (depuis la racine du projet) : python3 spark/report.py"""
import json
import os
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RES = os.path.join(ROOT, "spark", "results")
OUT = os.path.join(ROOT, "docs", "spark_resultats.txt")
lines = []


def p(s=""):
    lines.append(s)
    print(s)


def load(name):
    path = os.path.join(RES, name)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


data, als, churn = load("data_check.json"), load("als_metrics.json"), load("churn_metrics.json")
ok = ko = 0
p("=" * 74)
p("PHASE 6 - SPARK MLLIB : RESULTATS")
p("=" * 74)

if data:
    p(f"\nCluster : Spark {data['spark_version']} | {data['master']} | executeurs : {data['executors']}")
    p(f"Donnees : {data['n_ratings']} notes | {data['users_with_ratings']} utilisateurs | "
      f"{data['movies_with_ratings']} films | densite {100 * data['density']:.1f} %")

if als:
    K = als["k"]
    p(f"\n--- 1) RECOMMANDATION (ALS) : {als['n_train']} notes d'entrainement, {als['n_test']} de test ---")
    p(f"\nPrediction de la note (RMSE, plus bas = mieux) :")
    for k, v in als["baselines_rmse"].items():
        p(f"  {k:<30} {v:.4f}")
    p(f"  {'ALS ' + str(als['best_params']):<30} {als['als_rmse']:.4f}   (MAE {als['als_mae']:.4f})")
    best_base = min(als["baselines_rmse"].values())
    p(f"  -> gain de {100 * (1 - als['als_rmse'] / best_base):.1f} % sur la meilleure baseline")
    p(f"\nClassement des films (film pertinent = note >= {als['relevant_threshold']}) :")
    p(f"  {'systeme':<40} {'P@' + str(K):>8} {'R@' + str(K):>8} {'NDCG@' + str(K):>8}")
    for name, m in als["ranking"].items():
        nd = f"{m['ndcg']:.4f}" if m["ndcg"] is not None else "-"
        p(f"  {name:<40} {m['precision']:>8.4f} {m['recall']:>8.4f} {nd:>8}")
    p(f"  -> ALS : {als['lift_vs_random']:.1f} fois meilleur que le hasard (Precision@{K})")
    p(f"\nTemps (reference pour le benchmark de la phase 8) :")
    p(f"  entrainement ALS {als['bench_fit_params']} : {als['bench_fit_seconds']} s | "
      f"executeurs : {als['cluster']['executors']}")
    if "cv_seconds" in als:
        p(f"  validation croisee ({len(als['cv_grid'])} combinaisons x 3 plis) : {als['cv_seconds']} s")
    for k, v in als["checks"].items():
        p(f"  [{'OK' if v else 'KO'}] {k}")
        ok, ko = ok + bool(v), ko + (not v)

if churn:
    p(f"\n--- 2) PREDICTION DU CHURN (bonus) : {churn['n']} utilisateurs, {100 * churn['prevalence']:.1f} % de resilies ---")
    b = churn["baselines"]
    p(f"\n  Hasard : AUC 0.500 | score d'une seule variable (nb de notes) : AUC {b['auc_one_variable_nb_ratings']:.3f}")
    p(f"\n  {'modele':<24} {'AUC test':>9} {'AUC-PR':>7} {'rappel':>7} {'precision':>10} {'AUC CV-5':>9}")
    for name, m in churn["models"].items():
        p(f"  {name:<24} {m['auc_roc']:>9.3f} {m['auc_pr']:>7.3f} {m['recall']:>7.2f} {m['precision']:>10.2f} {m['cv5_auc_all']:>9.3f}")
    p(f"\n  Variables les plus importantes (Random Forest) : "
      + ", ".join(f"{x['feature']} ({x['importance']:.2f})" for x in churn["feature_importance"][:3]))
    best, noise = churn["best_cv5_auc"], 2 * churn["auc_noise_level"]
    if best < 0.6:
        p(f"\n  LECTURE : AUC {best:.3f} (+/- {noise:.2f}) : pas de signal exploitable. Le desabonnement des donnees")
        p("  synthetiques est quasi aleatoire : a presenter comme une limite du jeu de donnees, pas du modele.")
    else:
        p(f"\n  LECTURE : AUC {best:.3f} : signal exploitable.")
    for k, v in churn["checks"].items():
        p(f"  [{'OK' if v else 'KO'}] {k}")
        ok, ko = ok + bool(v), ko + (not v)

if not (als or churn or data):
    sys.exit("Aucun resultat dans spark/results/ : lancez d'abord bash spark/run_all.sh")
p(f"\nResultat : {ok} controle(s) OK / {ko} KO")
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
print(f"\nSauvegarde : docs/spark_resultats.txt")
sys.exit(0 if ko == 0 else 1)
