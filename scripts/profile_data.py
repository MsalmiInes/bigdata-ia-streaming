#!/usr/bin/env python3
"""Profile des CSV : en-tetes, nombre de lignes, valeurs vides, doublons."""
import csv
import sys
from pathlib import Path

folder = Path(sys.argv[1] if len(sys.argv) > 1 else "data/generated/small")
files = sorted(folder.glob("*.csv"))
if not files:
    sys.exit(f"Aucun CSV trouve dans {folder.resolve()}")

print(f"{'FICHIER':<22}{'LIGNES':>10}{'VIDES':>8}{'DOUBLONS':>10}   COLONNES")
print("-" * 100)
for f in files:
    with open(f, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        rows = empties = dups = 0
        seen = set()
        for r in reader:
            rows += 1
            empties += sum(1 for c in r if c.strip() == "")
            key = tuple(r)
            if key in seen:
                dups += 1
            else:
                seen.add(key)
    print(f"{f.name:<22}{rows:>10}{empties:>8}{dups:>10}   {', '.join(header)}")
