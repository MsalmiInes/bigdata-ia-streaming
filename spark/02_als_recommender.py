"""
Phase 6 - etape 2 : systeme de recommandation de films avec ALS (Spark MLlib).

  1. decoupage train (80 %) / test (20 %)
  2. baselines : moyenne globale, par film, par utilisateur, biais utilisateur + film
  3. ALS : reglage (rank, regParam) par validation croisee, puis evaluation sur le test (RMSE, MAE)
  4. classement : Precision@K, Recall@K, NDCG@K, compares a deux baselines + au hasard
  5. modele final entraine sur toutes les notes -> top-K de chaque utilisateur (films deja notes exclus)

Usage : python3 02_als_recommender.py [--fast] [--k 5] [--relevant 4.0] [--seed 42]
  --fast : saute la validation croisee (1 seul entrainement, parametres par defaut) : 1 a 2 minutes
"""
import argparse
import os
import sys

import numpy as np
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (HDFS_OUT, RESULTS_DIR, banner, cluster_info, get_spark, read_ml, save_json, say, timer)

ap = argparse.ArgumentParser()
ap.add_argument("--fast", action="store_true")
ap.add_argument("--k", type=int, default=5)
ap.add_argument("--relevant", type=float, default=4.0, help="note minimale pour qu'un film soit 'pertinent'")
ap.add_argument("--seed", type=int, default=42)
args = ap.parse_args()
K, THR, SEED = args.k, args.relevant, args.seed

DEFAULT = {"rank": 12, "regParam": 0.1, "maxIter": 10}
GRID = {"rank": [8, 12, 16], "regParam": [0.02, 0.05, 0.1], "maxIter": [10]}

spark = get_spark("phase6-02-als")
# Points de controle : sans eux, l'historique de calcul d'ALS devient trop profond (StackOverflowError)
spark.sparkContext.setCheckpointDir(f"{HDFS_OUT}/tmp/spark_checkpoints")
results = {"k": K, "relevant_threshold": THR, "seed": SEED, "fast": args.fast}


def make_als(**p):
    return ALS(userCol="user_id", itemCol="movie_id", ratingCol="rating", coldStartStrategy="drop",
               nonnegative=False, implicitPrefs=False, seed=SEED, checkpointInterval=5, **p)


rmse_ev = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse")
mae_ev = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="mae")


def rmse_of(df, col):
    return float(df.select(F.sqrt(F.avg((F.col(col) - F.col("rating")) ** 2))).first()[0])


def topk_als(model, seen, k):
    """Top-k films par utilisateur, hors films deja vus dans `seen`."""
    max_seen = seen.groupBy("user_id").count().agg(F.max("count")).first()[0]
    recs = (model.recommendForAllUsers(k + int(max_seen))
            .select("user_id", F.explode("recommendations").alias("r"))
            .select("user_id", F.col("r.movie_id").alias("movie_id"), F.col("r.rating").alias("score")))
    recs = recs.join(seen.select("user_id", "movie_id"), ["user_id", "movie_id"], "left_anti")
    w = Window.partitionBy("user_id").orderBy(F.col("score").desc(), "movie_id")
    return recs.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= k)


def topk_global(score_df, seen, users, k):
    """Classement identique pour tous (baseline non personnalisee), hors films deja vus."""
    max_seen = seen.groupBy("user_id").count().agg(F.max("count")).first()[0]
    top = score_df.orderBy(F.col("score").desc(), "movie_id").limit(k + int(max_seen))
    cand = users.crossJoin(top).join(seen.select("user_id", "movie_id"), ["user_id", "movie_id"], "left_anti")
    w = Window.partitionBy("user_id").orderBy(F.col("score").desc(), "movie_id")
    return cand.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= k)


def rank_metrics(recs, relevant, k):
    """Precision@k, Recall@k, NDCG@k moyens sur les utilisateurs ayant au moins 1 film pertinent en test."""
    n_rel = relevant.groupBy("user_id").agg(F.count("*").alias("n_rel"))
    hits = (recs.join(relevant.withColumn("hit", F.lit(1)), ["user_id", "movie_id"], "left")
            .groupBy("user_id")
            .agg(F.sum(F.coalesce(F.col("hit"), F.lit(0))).alias("hits"),
                 F.sum(F.coalesce(F.col("hit"), F.lit(0)) / F.log2(F.col("rank") + 1)).alias("dcg")))
    per_user = (n_rel.join(hits, "user_id", "left").fillna(0, ["hits", "dcg"])
                .withColumn("idcg", F.expr(
                    f"aggregate(transform(sequence(1, cast(least({k}, n_rel) as int)), i -> 1D / log2(i + 1)), "
                    f"0D, (a, b) -> a + b)")))
    row = per_user.agg(F.avg(F.col("hits") / k).alias("p"), F.avg(F.col("hits") / F.col("n_rel")).alias("r"),
                       F.avg(F.col("dcg") / F.col("idcg")).alias("n")).first()
    return {"precision": float(row["p"]), "recall": float(row["r"]), "ndcg": float(row["n"])}


# ------------------------------------------------------------------ 1) donnees
banner("1) Donnees et decoupage train / test")
ratings = (read_ml(spark, "ratings", ["user_id", "movie_id", "rating"])
           .select(F.col("user_id").cast("int"), F.col("movie_id").cast("int"), F.col("rating").cast("double")))
movies = read_ml(spark, "movie_features", ["movie_id", "title", "genres", "year", "nb_ratings", "avg_rating",
                                           "nb_views", "total_minutes"]).select("movie_id", "title", "genres")
train, test = ratings.randomSplit([0.8, 0.2], seed=SEED)
train, test = train.cache(), test.cache()
n_train, n_test = train.count(), test.count()
users_train = train.select("user_id").distinct().cache()
n_users, n_movies = users_train.count(), train.select("movie_id").distinct().count()
info = cluster_info(spark)
say(f"train : {n_train} notes | test : {n_test} notes | {n_users} utilisateurs | {n_movies} films")
say(f"Spark {info['spark_version']} | executeurs : {info['executors']} | master : {info['master']}")
results.update({"n_train": n_train, "n_test": n_test, "n_users": n_users, "n_movies": n_movies, "cluster": info})

# ------------------------------------------------------------------ 2) baselines RMSE
banner("2) Baselines de prediction de note (RMSE sur le jeu de test)")
gmean = float(train.agg(F.avg("rating")).first()[0])
um = train.groupBy("user_id").agg(F.avg("rating").alias("u_mean"))
im = train.groupBy("movie_id").agg(F.avg("rating").alias("m_mean"))
t = (test.join(um, "user_id", "left").join(im, "movie_id", "left")
     .withColumn("p_global", F.lit(gmean))
     .withColumn("p_movie", F.coalesce("m_mean", F.lit(gmean)))
     .withColumn("p_user", F.coalesce("u_mean", F.lit(gmean)))
     .withColumn("p_bias", F.least(F.greatest(
         F.lit(gmean) + (F.coalesce("u_mean", F.lit(gmean)) - gmean) + (F.coalesce("m_mean", F.lit(gmean)) - gmean),
         F.lit(0.5)), F.lit(5.0))))
base = {"moyenne globale": rmse_of(t, "p_global"), "moyenne par film": rmse_of(t, "p_movie"),
        "moyenne par utilisateur": rmse_of(t, "p_user"),
        "biais utilisateur + film": rmse_of(t, "p_bias")}
for k_, v in base.items():
    say(f"  {k_:<28} RMSE = {v:.4f}")
results["baselines_rmse"] = base

# ------------------------------------------------------------------ 3) ALS
banner("3) ALS : entrainement et reglage")
with timer("entrainement ALS, parametres fixes " + str(DEFAULT)) as tm:
    m_fixed = make_als(**DEFAULT).fit(train)
    m_fixed.userFactors.count()              # force l'execution complete
results["bench_fit_seconds"] = tm["s"]
results["bench_fit_params"] = DEFAULT
say(f"  [reference benchmark phase 8] {tm['s']} s sur {n_train} notes")

if args.fast:
    best_params, model = dict(DEFAULT), m_fixed
    say("  --fast : validation croisee sautee, parametres par defaut utilises")
else:
    als = make_als()
    grid = (ParamGridBuilder().addGrid(als.rank, GRID["rank"]).addGrid(als.regParam, GRID["regParam"])
            .addGrid(als.maxIter, GRID["maxIter"]).build())
    say(f"  validation croisee 3 plis sur {len(grid)} combinaisons (rank x regParam)...")
    cv = CrossValidator(estimator=als, estimatorParamMaps=grid, evaluator=rmse_ev, numFolds=3,
                        seed=SEED, parallelism=1)
    with timer("validation croisee") as tcv:
        cv_model = cv.fit(train)
    results["cv_seconds"] = tcv["s"]
    cv_rows = []
    for pm, m in zip(grid, cv_model.avgMetrics):
        cv_rows.append({**{p.name: v for p, v in pm.items()}, "cv_rmse": float(m)})
    cv_rows.sort(key=lambda r: r["cv_rmse"])
    say(f"\n  {'rank':>5} {'regParam':>9} {'maxIter':>8} {'RMSE (CV)':>10}")
    for r in cv_rows:
        say(f"  {r['rank']:>5} {r['regParam']:>9} {r['maxIter']:>8} {r['cv_rmse']:>10.4f}"
            + ("   <- meilleur" if r is cv_rows[0] else ""))
    best_params = {k_: cv_rows[0][k_] for k_ in ("rank", "regParam", "maxIter")}
    model = cv_model.bestModel
    results["cv_grid"] = cv_rows
results["best_params"] = best_params

pred = model.transform(test)
n_pred = pred.count()
als_rmse, als_mae = float(rmse_ev.evaluate(pred)), float(mae_ev.evaluate(pred))
results.update({"als_rmse": als_rmse, "als_mae": als_mae, "test_coverage": n_pred / n_test})
say(f"\nALS {best_params} -> RMSE test = {als_rmse:.4f} | MAE = {als_mae:.4f} "
    f"| notes du test predites : {100 * n_pred / n_test:.1f} %")
best_base = min(base.values())
say(f"Gain de RMSE : {100 * (1 - als_rmse / base['moyenne globale']):.1f} % vs moyenne globale ; "
    f"{100 * (1 - als_rmse / best_base):.1f} % vs meilleure baseline ({min(base, key=base.get)})")

# ------------------------------------------------------------------ 4) classement
banner(f"4) Classement : Precision@{K}, Recall@{K}, NDCG@{K} (film pertinent = note >= {THR} en test)")
relevant = test.filter(F.col("rating") >= THR).select("user_id", "movie_id").cache()
n_eval = relevant.select("user_id").distinct().count()
say(f"  {n_eval} utilisateurs evalues, {relevant.count()} films pertinents au total dans le test")

pop_df = train.groupBy("movie_id").agg(F.count("*").alias("score"))
bayes_df = train.groupBy("movie_id").agg(
    ((F.sum("rating") + 20 * gmean) / (F.count("*") + 20)).alias("score"))
systems = {
    f"ALS (rank={best_params['rank']}, reg={best_params['regParam']})": topk_als(model, train, K),
    "meilleures notes (moyenne ajustee)": topk_global(bayes_df, train, users_train, K),
    "plus populaires (nb de notes)": topk_global(pop_df, train, users_train, K),
}
rank_res = {}
for name, recs in systems.items():
    rank_res[name] = rank_metrics(recs, relevant, K)

# Au hasard : esperance exacte (candidats = films non vus en train)
rnd = (relevant.groupBy("user_id").agg(F.count("*").alias("n_rel"))
       .join(train.groupBy("user_id").agg(F.count("*").alias("n_seen")), "user_id")
       .withColumn("cand", F.lit(n_movies) - F.col("n_seen")))
rr = rnd.agg(F.avg(F.col("n_rel") / F.col("cand")).alias("p"), F.avg(K / F.col("cand")).alias("r")).first()
rank_res["au hasard (esperance exacte)"] = {"precision": float(rr["p"]), "recall": float(rr["r"]), "ndcg": None}

say(f"\n  {'systeme':<40} {'P@'+str(K):>8} {'R@'+str(K):>8} {'NDCG@'+str(K):>8}")
for name, m in rank_res.items():
    nd = f"{m['ndcg']:.4f}" if m["ndcg"] is not None else "   -"
    say(f"  {name:<40} {m['precision']:>8.4f} {m['recall']:>8.4f} {nd:>8}")
p_als = rank_res[list(systems)[0]]["precision"]
p_rnd = rank_res["au hasard (esperance exacte)"]["precision"]
say(f"\n  ALS est {p_als / p_rnd:.1f} fois meilleur que le hasard (Precision@{K})")
say("  Les valeurs absolues sont faibles car chaque utilisateur n'a que ~20 notes en test parmi ~400 films")
say("  candidats : ce qui compte est la comparaison entre systemes.")
results["ranking"] = rank_res
results["lift_vs_random"] = p_als / p_rnd

# ------------------------------------------------------------------ 5) modele final
banner("5) Modele final (toutes les notes) : recommandations pour chaque utilisateur")
with timer("entrainement du modele final") as tf:
    final = make_als(**best_params).fit(ratings)
    final.userFactors.count()
results["final_fit_seconds"] = tf["s"]
recs = topk_als(final, ratings, K)
recs = (recs.join(movies, "movie_id")
        .select("user_id", "rank", "movie_id", "title", "genres",
                F.round(F.least(F.greatest(F.col("score"), F.lit(0.5)), F.lit(5.0)), 2).alias("predicted_rating")))
recs.write.mode("overwrite").parquet(f"{HDFS_OUT}/results/recommendations")
final.write().overwrite().save(f"{HDFS_OUT}/models/als")
recs = spark.read.parquet(f"{HDFS_OUT}/results/recommendations")     # relecture : historique de calcul propre
n_recs = recs.count()
say(f"  {n_recs} recommandations ({K} par utilisateur) -> {HDFS_OUT}/results/recommendations (Parquet)")
say(f"  modele sauvegarde -> {HDFS_OUT}/models/als")

os.makedirs(RESULTS_DIR, exist_ok=True)
recs.orderBy("user_id", "rank").toPandas().to_csv(os.path.join(RESULTS_DIR, "recommendations_top.csv"), index=False)

uid = int(users_train.agg(F.min("user_id")).first()[0])
say(f"\n  Exemple : utilisateur {uid}")
say("  Ses films les mieux notes :")
for r in (ratings.filter(F.col("user_id") == uid).join(movies, "movie_id")
          .orderBy(F.col("rating").desc(), "movie_id").limit(5).collect()):
    say(f"    {r['rating']:.1f}  {r['title']:<12} {r['genres']}")
say(f"  Films recommandes :")
for r in recs.filter(F.col("user_id") == uid).orderBy("rank").collect():
    say(f"    {r['predicted_rating']:.2f}  {r['title']:<12} {r['genres']}")

# ------------------------------------------------------------------ controles
banner("6) Controles")
checks = {
    "ALS meilleur que toutes les baselines (RMSE)": als_rmse < best_base,
    "ALS meilleur que le hasard (Precision@K)": p_als > p_rnd,
    "ALS meilleur que 'plus populaires' (Precision@K)": p_als > rank_res["plus populaires (nb de notes)"]["precision"],
    "calcul distribue (au moins 1 executeur)": info["executors"] >= 1 or info["master"].startswith("local"),
    "recommandations pour tous les utilisateurs": n_recs >= K * int(users_train.count()) * 0.99,
}
for k_, v in checks.items():
    say(f"  [{'OK' if v else 'KO'}] {k_}")
results["checks"] = checks
save_json("als_metrics.json", results)
spark.stop()
sys.exit(0 if all(checks.values()) else 1)
