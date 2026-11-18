"""
Phase 8 - reference "une seule machine" : le meme nettoyage et les memes transformations que Pig
(scripts 01 a 04) mais avec pandas, en memoire, sans cluster.

Sert a mesurer a partir de quel volume un cluster devient utile : on compare le temps et la
MEMOIRE de pandas avec ceux de Pig.

Usage : python3 05_pandas_baseline.py --size small|medium|large
Lit /data/generated/<size>/*.csv (DATA_ROOT pour changer) ; ecrit results/bench_pandas_<size>.json
"""
import argparse
import os
import resource
import sys
import time

import pandas as pd

from common import save_json, say

ap = argparse.ArgumentParser()
ap.add_argument("--size", required=True)
args = ap.parse_args()
ROOT = os.environ.get("DATA_ROOT", "/data/generated")
D = os.path.join(ROOT, args.size)
if not os.path.isdir(D):
    sys.exit(f"[KO] dossier introuvable : {D}")


def rd(name, **kw):
    # '' = valeur manquante (comme PigStorage) ; on garde les autres textes tels quels
    return pd.read_csv(os.path.join(D, name + ".csv"), keep_default_na=False, na_values=[""], dtype=str, **kw)


def num(s, kind):
    v = pd.to_numeric(s, errors="coerce")
    return v.astype("Int64") if kind == "int" else v


res = {"size": args.size, "pandas": pd.__version__}
t_all = time.time()

# ---------------------------------------------------------------- lecture
t0 = time.time()
raw = {n: rd(n) for n in ["ratings", "movies", "users", "subscriptions", "tags", "watch_logs", "kafka_events"]}
res["read_s"] = round(time.time() - t0, 2)
res["raw_rows"] = {k: int(len(v)) for k, v in raw.items()}

# ---------------------------------------------------------------- nettoyage (memes regles que Pig 01-03)
t0 = time.time()
clean = {}

r = raw["ratings"].copy()
r["user_id"], r["movie_id"], r["rating"] = num(r.user_id, "int"), num(r.movie_id, "int"), num(r.rating, "float")
r = r.dropna(subset=["user_id", "movie_id", "rating", "timestamp"])
r = r[(r.rating >= 0.5) & (r.rating <= 5.0)].drop_duplicates()
clean["ratings"] = r

m = raw["movies"].copy()
m["movie_id"], m["year"] = num(m.movie_id, "int"), num(m.year, "int")
m["title"] = m.title.str.strip()
m = m.dropna(subset=["movie_id", "year"])
clean["movies"] = m[(m.year >= 1900) & (m.year <= 2030)].drop_duplicates()

u = raw["users"].copy()
u["user_id"], u["age"] = num(u.user_id, "int"), num(u.age, "int")
u["gender"] = u.gender.where(u.gender.isin(["M", "F"]), "U")
u["country"] = u.country.str.strip().str.upper()
u = u.dropna(subset=["user_id"])
clean["users"] = u[u.age.isna() | ((u.age >= 10) & (u.age <= 100))].drop_duplicates()

s = raw["subscriptions"].copy()
s["subscription_id"], s["user_id"] = num(s.subscription_id, "int"), num(s.user_id, "int")
s["monthly_price"] = num(s.monthly_price, "float")
s["status"] = s.status.str.strip().str.lower()
s = s.dropna(subset=["subscription_id", "user_id", "monthly_price"])
clean["subscriptions"] = s[(s.monthly_price > 0) & s.status.isin(["active", "cancelled"])].drop_duplicates()

t = raw["tags"].copy()
t["user_id"], t["movie_id"] = num(t.user_id, "int"), num(t.movie_id, "int")
t["tag"] = t.tag.str.strip().str.lower()
t = t.dropna(subset=["user_id", "movie_id", "tag"])
clean["tags"] = t.drop_duplicates()

lg = raw["watch_logs"].copy()
for c in ["user_id", "movie_id", "watch_duration_min"]:
    lg[c] = num(lg[c], "int")
lg["log_id"] = num(lg.log_id, "int")
lg["device"] = lg.device.str.strip().str.lower()
lg = lg.dropna(subset=["log_id", "user_id", "movie_id", "watch_duration_min"])
lg = lg[(lg.watch_duration_min >= 1) & (lg.watch_duration_min <= 600) & lg.device.isin(["mobile", "tv", "web", "tablet"])]
clean["watch_logs"] = lg.drop_duplicates()

e = raw["kafka_events"].copy()
e["user_id"], e["movie_id"] = num(e.user_id, "int"), num(e.movie_id, "int")
e["event_type"] = e.event_type.str.strip().str.lower()
e = e.dropna(subset=["user_id", "movie_id", "timestamp"])
clean["kafka_events"] = e[e.event_type.isin(["play", "pause", "like", "search"])]      # pas de DISTINCT
res["clean_s"] = round(time.time() - t0, 2)
res["clean_rows"] = {k: int(len(v)) for k, v in clean.items()}
del raw

# ---------------------------------------------------------------- transformations (comme Pig 04)
t0 = time.time()
r, m, u, s, lg = clean["ratings"], clean["movies"], clean["users"], clean["subscriptions"], clean["watch_logs"]
enriched = r.merge(m[["movie_id", "title", "genres", "year"]], on="movie_id", how="inner")
mr = r.groupby("movie_id").rating.agg(nb_ratings="count", avg_rating="mean").reset_index()
ml = lg.groupby("movie_id").watch_duration_min.agg(nb_views="count", total_minutes="sum").reset_index()
movie_stats = m.merge(mr, on="movie_id", how="left").merge(ml, on="movie_id", how="left")
ur = r.groupby("user_id").rating.agg(nb_ratings="count", avg_rating="mean").reset_index()
ul = lg.groupby("user_id").watch_duration_min.agg(nb_views="count", total_minutes="sum").reset_index()
uf = (u[["user_id", "age", "gender", "country"]]
      .merge(s[["user_id", "plan", "monthly_price", "status"]], on="user_id", how="left")
      .merge(ur, on="user_id", how="left").merge(ul, on="user_id", how="left"))
uf["churned"] = (uf.status == "cancelled").astype(int)
res["transform_s"] = round(time.time() - t0, 2)
res["transform_rows"] = {"ratings_enriched": int(len(enriched)), "movie_stats": int(len(movie_stats)),
                         "user_features": int(len(uf))}

res["total_s"] = round(time.time() - t_all, 2)
res["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)   # Linux : en Ko
say(f"pandas {args.size} : lecture {res['read_s']} s | nettoyage {res['clean_s']} s | "
    f"transformations {res['transform_s']} s | TOTAL {res['total_s']} s | memoire max {res['peak_rss_mb']} Mo")
say("lignes propres : " + ", ".join(f"{k}={v}" for k, v in res["clean_rows"].items()))
save_json(f"bench_pandas_{args.size}.json", res)
