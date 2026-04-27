"""
lemmatize.py
============
Pulls Bundestag speeches from the local Open Discourse PostgreSQL database,
lemmatises them with spaCy (de_core_news_lg), and writes a Parquet file
that scripts/stm/train.R consumes directly.

Output columns:
  id, text (space-joined content lemmas), gender, faction, year

Usage:
  python scripts/stm/lemmatize.py

Env vars:
  DB_CON_STRING   postgresql://user:pass@host:port/db  [required]
  STM_SAMPLE      max speeches to sample; 0 = no limit  [default: 0]
  STM_SEED        RNG seed for sampling                [default: 42]
    DATA_PATH       root for data/stm/ output            [default: <repo_root>/data]
"""

import os
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine
import spacy
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

DB_CON_STRING = os.getenv("DB_CON_STRING")
if not DB_CON_STRING:
    raise SystemExit("DB_CON_STRING not set. Add it to .env or export it.")

N_SAMPLE = int(os.getenv("STM_SAMPLE", "0"))
SEED = int(os.getenv("STM_SEED", "42"))
DEFAULT_DATA_PATH = Path(__file__).resolve().parents[2] / "data"
DATA_PATH = Path(os.getenv("DATA_PATH", str(DEFAULT_DATA_PATH)))
OUT_DIR = DATA_PATH / "stm"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_LB = "1990-10-04"
_cfg = yaml.safe_load(
    (Path(__file__).resolve().parents[2] / "notebooks" / "config.yaml").read_text()
)
MAJOR_PARTIES = tuple(_cfg["analysis"]["factions"]["major_parties"])
PARTIES_SQL = ", ".join(f"'{p}'" for p in MAJOR_PARTIES)
MERGE_PDS_TO = _cfg["analysis"]["factions"]["merge_pds_to"]
FACTION_MERGE = (
    f"CASE WHEN f.abbreviation = 'PDS' THEN '{MERGE_PDS_TO}' ELSE f.abbreviation END"
)
FACTION_NORMALIZE = _cfg["analysis"]["factions"].get("normalize", {})

CONTENT_POS = {"NOUN", "VERB", "ADJ", "ADV"}

# ── Pull speeches ─────────────────────────────────────────────────────────────

print("[db] connecting...")
engine = create_engine(DB_CON_STRING)

query = f"""
    SELECT
        s.id::text                              AS id,
        s.speech_content                        AS text,
        p.gender,
        {FACTION_MERGE}                         AS faction,
        EXTRACT(year FROM s.date)::int          AS year,
        s.electoral_term
    FROM open_discourse.speeches    s
    JOIN open_discourse.politicians p ON s.politician_id = p.id
    JOIN open_discourse.factions    f ON s.faction_id    = f.id
    WHERE s.date             >= '{DATE_LB}'
      AND s.speech_content   IS NOT NULL
      AND length(s.speech_content) > 200
      AND p.gender           IN ('männlich', 'weiblich')
      AND f.abbreviation     IN ({PARTIES_SQL})
"""

print("[db] querying speeches...")
with engine.connect() as conn:
    df = pd.read_sql(query, conn)
print(f"[db] {len(df):,} speeches retrieved")

# ── Sample ────────────────────────────────────────────────────────────────────

if N_SAMPLE > 0 and len(df) > N_SAMPLE:
    df = df.sample(n=N_SAMPLE, random_state=SEED).reset_index(drop=True)
    print(f"[sample] reduced to {N_SAMPLE:,} speeches")

df["gender"] = df["gender"].map({"männlich": "male", "weiblich": "female"})
df["faction"] = df["faction"].map(lambda x: FACTION_NORMALIZE.get(x, x))

# ── Lemmatise with spaCy ──────────────────────────────────────────────────────
#
# I disable the parser and NER components — only the tagger and morphologiser
# are needed for lemmatisation

print("[spacy] loading de_core_news_lg...")
nlp = spacy.load("de_core_news_lg", disable=["parser", "ner"])

BATCH_SIZE = 256  # texts per batch


def lemmatise_batch(texts: list[str]) -> list[str]:
    results = []
    for doc in nlp.pipe(texts, batch_size=BATCH_SIZE):
        tokens = [
            t.lemma_.lower()
            for t in doc
            if t.pos_ in CONTENT_POS
            and not t.is_stop
            and not t.is_punct
            and len(t.lemma_) > 2
        ]
        results.append(" ".join(tokens))
    return results


print(f"[spacy] lemmatising {len(df):,} speeches in batches of {BATCH_SIZE}...")
lemmas: list[str] = []
texts = df["text"].tolist()
batches = range(0, len(texts), BATCH_SIZE)

for start in tqdm(batches, unit="batch", desc="lemmatising"):
    batch = texts[start : start + BATCH_SIZE]
    lemmas.extend(lemmatise_batch(batch))

df["text"] = lemmas

# Drop speeches that became empty after lemmatisation (e.g. pure procedural noise)
before = len(df)
df = df[df["text"].str.len() > 0].reset_index(drop=True)
print(f"[spacy] {len(df):,} speeches retained ({before - len(df)} dropped as empty)")

# ── Write output ──────────────────────────────────────────────────────────────

out_path = OUT_DIR / "lemmatized.parquet"
df[["id", "text", "gender", "faction", "year", "electoral_term"]].to_parquet(
    out_path, index=False
)
print(f"[done] → {out_path}  ({len(df):,} rows)")
