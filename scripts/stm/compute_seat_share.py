"""
compute_seat_share.py
=====================
Computes faction-level female seat share per electoral term from
MDB_STAMMDATEN.XML and writes data/stm/female_share_ft.parquet.

Run from repo root:
    python scripts/stm/compute_seat_share.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks"))
from notebook_utils import parse_mdb_seats, TERM_UPPER_BOUNDARY, DATA_PATH

OUT_PATH = DATA_PATH / "stm" / "female_share_ft.parquet"

print("[xml] parsing MDB_STAMMDATEN.XML...")
df = parse_mdb_seats()
print(f"[xml] {len(df):,} MdB×term records")

female_share = (
    df[
        df["gender"].isin(["männlich", "weiblich"])
        & df["faction"].ne("Other")
        & df["wp"].le(TERM_UPPER_BOUNDARY)
    ]
    .groupby(["wp", "faction", "gender"])
    .size()
    .unstack(fill_value=0)
    .assign(female_share_ft=lambda d: d["weiblich"] / (d["männlich"] + d["weiblich"]))
    .reset_index()[["wp", "faction", "female_share_ft"]]
    .rename(columns={"wp": "electoral_term"})
)

print(
    female_share.pivot(
        index="electoral_term", columns="faction", values="female_share_ft"
    )
    .round(3)
    .to_string()
)

female_share.to_parquet(OUT_PATH, index=False)
print(f"[write] {OUT_PATH}")
