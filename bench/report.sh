#!/usr/bin/env bash
# Phase 8 - Genere docs/benchmark_resultats.md et les graphiques docs/figures/bench_*.png
# Usage (depuis la racine du projet) : bash bench/report.sh
cd "$(dirname "$0")/.." || exit 1
mkdir -p docs/figures
if docker image inspect bigdata-spark:1.0 >/dev/null 2>&1; then
  docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e MPLCONFIGDIR=/tmp/mpl \
    -v "$PWD":/work -w /work --entrypoint python3 bigdata-spark:1.0 bench/report.py "$@" && exit 0
fi
echo "(image Docker indisponible : tentative avec le Python du PC, graphiques possibles seulement si matplotlib est installe)"
python3 bench/report.py "$@"
