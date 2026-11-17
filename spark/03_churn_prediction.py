"""
Phase 6 - etape 3 (BONUS) : prediction du desabonnement (churn) avec Spark MLlib.

  - cible : churned (1 = abonnement resilie)
  - modeles : regression logistique et Random Forest (poids de classes equilibres)
  - reglage par validation croisee, evaluation sur un jeu de test, AUC-ROC / AUC-PR
  - comparaison a deux references : le hasard (AUC = 0,5) et un score d'une seule variable (nb de notes)

Usage : python3 03_churn_prediction.py [--seed 42]
"""
import argparse
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

from common import HDFS_OUT, banner, cluster_info, get_spark, read_ml, save_json, say, timer

ap = argparse.ArgumentParser()
ap.add_argument("--seed", type=int, default=42)
SEED = ap.parse_args().seed

COLS = ["user_id", "age", "age_missing", "is_male", "monthly_price", "nb_ratings", "avg_rating",
        "nb_views", "total_minutes", "avg_minutes_per_view", "nb_distinct_movies",
        "share_mobile", "share_tv", "share_web", "share_tablet", "nb_tags", "nb_events", "churned"]
FEATURES = [c for c in COLS if c not in ("user_id", "churned")]

spark = get_spark("phase6-03-churn")
df = read_ml(spark, "user_features", COLS).withColumn("churned", F.col("churned").cast("int"))

banner("1) Donnees et decoupage (80 % train / 20 % test, stratifie)")
n = df.count()
n1 = df.filter("churned = 1").count()
n0 = n - n1
# poids de classes : chaque classe pese autant au total (evite un modele qui predit toujours "reste")
df = df.withColumn("w", F.when(F.col("churned") == 1, n / (2.0 * n1)).otherwise(n / (2.0 * n0))).cache()
train = df.sampleBy("churned", {0: 0.8, 1: 0.8}, seed=SEED).cache()
test = df.join(train.select("user_id"), "user_id", "left_anti").cache()
n_train, n_test = train.count(), test.count()
info = cluster_info(spark)
say(f"{n} utilisateurs | resilies : {n1} ({100 * n1 / n:.1f} %) | train : {n_train} | test : {n_test}")
say(f"{len(FEATURES)} variables : {', '.join(FEATURES)}")
results = {"n": n, "n_churned": n1, "prevalence": n1 / n, "n_train": n_train, "n_test": n_test,
           "features": FEATURES, "cluster": info, "seed": SEED}

auc_ev = BinaryClassificationEvaluator(labelCol="churned", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
pr_ev = BinaryClassificationEvaluator(labelCol="churned", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
assembler = VectorAssembler(inputCols=FEATURES, outputCol="features_raw")


def make_lr(reg=0.01):
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    lr = LogisticRegression(featuresCol="features", labelCol="churned", weightCol="w", maxIter=100,
                            elasticNetParam=0.0, regParam=reg)
    return Pipeline(stages=[assembler, scaler, lr]), lr


def make_rf(trees=100, depth=5):
    rf = RandomForestClassifier(featuresCol="features_raw", labelCol="churned", weightCol="w", seed=SEED,
                                numTrees=trees, maxDepth=depth, minInstancesPerNode=5)
    return Pipeline(stages=[assembler, rf]), rf


def holdout(model, name):
    pred = model.transform(test).cache()
    cm = {(r["churned"], int(r["prediction"])): r["count"] for r in pred.groupBy("churned", "prediction").count().collect()}
    tp, fp, fn, tn = cm.get((1, 1), 0), cm.get((0, 1), 0), cm.get((1, 0), 0), cm.get((0, 0), 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {"auc_roc": float(auc_ev.evaluate(pred)), "auc_pr": float(pr_ev.evaluate(pred)),
            "accuracy": (tp + tn) / n_test, "precision": prec, "recall": rec,
            "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn}


def cv_all(pipe, est, folds=5):
    """AUC moyen en validation croisee sur TOUS les utilisateurs (estimation plus stable qu'un seul test).
    La grille contient une seule combinaison (une grille vide fait echouer CrossValidator avec un Pipeline)."""
    one = ParamGridBuilder().addGrid(est.featuresCol, [est.getFeaturesCol()]).build()
    cv = CrossValidator(estimator=pipe, estimatorParamMaps=one, evaluator=auc_ev, numFolds=folds, seed=SEED)
    return float(cv.fit(df).avgMetrics[0])


# ------------------------------------------------------------------ references
banner("2) References")
prevalence_test = test.filter("churned = 1").count() / n_test
base_acc = 1 - prevalence_test
one_var = BinaryClassificationEvaluator(labelCol="churned", rawPredictionCol="score", metricName="areaUnderROC")
auc_one = float(one_var.evaluate(test.withColumn("score", -F.col("nb_ratings").cast("double"))))
say(f"  Hasard                                   : AUC = 0.500")
say(f"  Toujours predire 'reste abonne'          : accuracy = {base_acc:.3f}, rappel = 0")
say(f"  Score d'une seule variable (-nb_ratings) : AUC = {auc_one:.3f}")
results["baselines"] = {"auc_random": 0.5, "accuracy_majority": base_acc, "auc_one_variable_nb_ratings": auc_one}

# ------------------------------------------------------------------ modeles
banner("3) Entrainement et reglage (validation croisee 3 plis sur le train)")
models = {}

pipe, lr = make_lr()
grid = ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.01, 0.1, 1.0]).build()
with timer("regression logistique") as t:
    cvm = CrossValidator(estimator=pipe, estimatorParamMaps=grid, evaluator=auc_ev, numFolds=3, seed=SEED).fit(train)
best_reg = grid[max(range(len(grid)), key=lambda i: cvm.avgMetrics[i])][lr.regParam]
models["Regression logistique"] = (cvm.bestModel, {"regParam": best_reg}, float(max(cvm.avgMetrics)), t["s"],
                                   make_lr(best_reg))

pipe, rf = make_rf()
grid = (ParamGridBuilder().addGrid(rf.numTrees, [50, 100]).addGrid(rf.maxDepth, [3, 5, 8]).build())
with timer("Random Forest") as t:
    cvm = CrossValidator(estimator=pipe, estimatorParamMaps=grid, evaluator=auc_ev, numFolds=3, seed=SEED).fit(train)
bi = max(range(len(grid)), key=lambda i: cvm.avgMetrics[i])
best_rf = {"numTrees": grid[bi][rf.numTrees], "maxDepth": grid[bi][rf.maxDepth]}
models["Random Forest"] = (cvm.bestModel, best_rf, float(cvm.avgMetrics[bi]), t["s"],
                           make_rf(best_rf["numTrees"], best_rf["maxDepth"]))

# ------------------------------------------------------------------ evaluation
banner("4) Evaluation sur le jeu de test et validation croisee 5 plis sur tous les utilisateurs")
results["models"] = {}
say(f"  {'modele':<24} {'AUC test':>9} {'AUC-PR':>7} {'rappel':>7} {'precision':>10} {'AUC CV-5':>9}")
for name, (m, params, cv_train_auc, secs, fresh) in models.items():
    h = holdout(m, name)
    h.update({"params": params, "cv3_auc_train": cv_train_auc, "train_seconds": secs, "cv5_auc_all": cv_all(*fresh)})
    results["models"][name] = h
    say(f"  {name:<24} {h['auc_roc']:>9.3f} {h['auc_pr']:>7.3f} {h['recall']:>7.2f} {h['precision']:>10.2f} "
        f"{h['cv5_auc_all']:>9.3f}   {params}")
say(f"\n  Prevalence du churn (reference AUC-PR) : {n1 / n:.3f}")

rf_model = models["Random Forest"][0].stages[-1]
imp = sorted(zip(FEATURES, rf_model.featureImportances.toArray()), key=lambda x: -x[1])
say("\n  Importance des variables (Random Forest), top 5 :")
for f, v in imp[:5]:
    say(f"    {f:<22} {v:.3f}")
results["feature_importance"] = [{"feature": f, "importance": float(v)} for f, v in imp]

models["Random Forest"][0].write().overwrite().save(f"{HDFS_OUT}/models/churn_rf")
say(f"\n  modele Random Forest sauvegarde -> {HDFS_OUT}/models/churn_rf")

# ------------------------------------------------------------------ interpretation automatique
best_cv = max(m["cv5_auc_all"] for m in results["models"].values())
se = 0.5 / (n ** 0.5)          # ordre de grandeur de l'incertitude sur un AUC (n utilisateurs)
results["best_cv5_auc"] = best_cv
results["auc_noise_level"] = se
banner("5) Lecture des resultats")
say(f"  Meilleur AUC (validation croisee 5 plis) : {best_cv:.3f}  (incertitude de l'ordre de +/- {2 * se:.2f})")
if best_cv < 0.6:
    say("  -> Signal tres faible : le modele ne fait pas mieux que le hasard de facon fiable.")
    say("     Resultat attendu avec ces donnees synthetiques : le desabonnement a ete genere presque au hasard,")
    say("     il n'existe donc pas de motif a apprendre. A presenter comme une limite du jeu de donnees.")
else:
    say("  -> Signal exploitable.")
checks = {"modeles entraines et evalues": len(results["models"]) == 2,
          "AUC defini pour chaque modele": all(0 <= m["auc_roc"] <= 1 for m in results["models"].values()),
          "calcul distribue (au moins 1 executeur)": info["executors"] >= 1 or info["master"].startswith("local")}
for k_, v in checks.items():
    say(f"  [{'OK' if v else 'KO'}] {k_}")
results["checks"] = checks
save_json("churn_metrics.json", results)
spark.stop()
sys.exit(0 if all(checks.values()) else 1)
