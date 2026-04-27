"""
export_topic_labels.py
=====================
Exports data/stm/topic_labels_annotated.csv from data/stm/topic_labels.parquet
with the manual-label schema for manual annotation of topics. This is the CSV
expected by notebooks/stm_analysis.qmd.

Run from repo root:
    python scripts/stm/export_topic_labels.py
"""

from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
STM_DIR = REPO_ROOT / "data" / "stm"
SOURCE_PATH = STM_DIR / "topic_labels.parquet"
TARGET_PATH = STM_DIR / "topic_labels_annotated.csv"

if not SOURCE_PATH.exists():
    raise FileNotFoundError(f"Missing source file: {SOURCE_PATH}")
elif TARGET_PATH.exists():
    confirm = input(f"Target file already exists: {TARGET_PATH}\nOverwrite? (Y/n): ")
    if confirm != "Y":
        print("Export cancelled.")
        exit()

labels = pd.read_parquet(SOURCE_PATH)
required = ["topic", "frex", "prob", "lift"]
missing = [col for col in required if col not in labels.columns]
if missing:
    raise ValueError(
        "topic_labels.parquet is missing required columns: " + ", ".join(missing)
    )

manual = pd.DataFrame(
    {
        "topic": labels["topic"],
        "label_de": "",
        "label_en": "",
        "is_procedural": "",
        "frex": labels["frex"].fillna(""),
        "prob": labels["prob"].fillna(""),
        "lift": labels["lift"].fillna(""),
    }
).sort_values("topic")

manual = manual[
    ["topic", "label_de", "label_en", "is_procedural", "frex", "prob", "lift"]
]

manual.to_csv(TARGET_PATH, index=False, encoding="utf-8")
print(f"[write] {TARGET_PATH}")
print("[schema] " + ", ".join(manual.columns))
