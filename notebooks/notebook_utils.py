import yaml
import os
from pathlib import Path
import json
import psycopg2
import pandas as pd
import numpy as np
from matplotlib.colors import to_hex, to_rgb
from datetime import datetime, date
from dotenv import load_dotenv
from plotnine import (
    ggplot,
    aes,
    geom_col,
    geom_line,
    geom_area,
    geom_histogram,
    geom_point,
    geom_errorbar,
    geom_errorbarh,
    geom_hline,
    geom_vline,
    geom_tile,
    geom_smooth,
    scale_x_continuous,
    scale_y_continuous,
    scale_fill_manual,
    scale_color_manual,
    scale_fill_gradient2,
    coord_flip,
    coord_equal,
    coord_cartesian,
    facet_wrap,
    labs,
    theme,
    theme_bw,
    element_text,
    element_line,
    element_blank,
    element_rect,
    annotate,
    geom_abline,
    scale_linetype_manual,
    scale_colour_manual,
    geom_text,
    geom_bar,
    scale_size_manual,
)
from plotnine.scales import scale_x_discrete

# ─── Paths ───────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
REPO_ROOT = _HERE if (_HERE / "data").exists() else _HERE.parent
DATA_PATH = REPO_ROOT / "data"
TABLES_DIR = os.path.join(REPO_ROOT, "writing", "include", "tables")

# ─── Shared config (config.yaml — also consumed by stm_analysis.qmd) ─────────

_cfg = yaml.safe_load((_HERE / "config.yaml").read_text())
NB_CFG = _cfg
TW = _cfg["figures"]["text_width"]  # text width in inches (150 mm)
DPI = _cfg["figures"]["dpi"]
RED = _cfg["colors"]["red"]
BLUE = _cfg["colors"]["blue"]
GRAY = _cfg["colors"]["gray"]
_AXIS_LINE = _cfg["colors"]["axis_line"]

# ─── DB connection ───────────────────────────────────────────────────────────────


def get_conn():
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        raise SystemExit(
            "Missing .env file. Copy the template and set the necessary variables. Then start the database. Find more info in the README."
        )
    load_dotenv()
    con_str = os.environ.get("DB_CON_STRING")
    if not con_str:
        raise SystemExit(
            "DB_CON_STRING is not set in .env.\n"
            "Edit .env and set it to your PostgreSQL connection URL."
        )
    try:
        conn = psycopg2.connect(con_str)
    except psycopg2.OperationalError as e:
        raise SystemExit(
            f"Cannot connect to the database ({e}).\n"
            "Make sure the Open Discourse PostgreSQL Docker container is running."
        ) from None
    print("Connected:", conn.get_dsn_parameters())
    return conn


# ─── Figures ───────────────────────────────────────────────────────────────

FIGURES = DATA_PATH / "figures_out"

thesis_theme = theme_bw() + theme(
    text=element_text(family="serif", size=9),
    plot_title=element_text(size=10, face="bold"),
    axis_title=element_text(size=9),
    axis_text=element_text(size=8),
    panel_grid_major_x=element_blank(),
    panel_grid_minor=element_blank(),
    panel_border=element_blank(),
    axis_line=element_line(colour=_AXIS_LINE, size=0.4),
    legend_position="top",
    legend_title=element_blank(),
    legend_text=element_text(size=9),
    legend_key_size=11,
)

# ─── Lower bound ───────────────────────────────────────────────────────────────
TERM_LOWER_BOUNDARY = _cfg["analysis"]["bounds"][
    "term_lower"
]  # first post-reunification electoral term (WP 12)
TERM_UPPER_BOUNDARY = _cfg["analysis"]["bounds"]["term_upper"]  # WP20 excluded
YEAR_LB_IDX = _cfg["analysis"]["bounds"][
    "year_lb_idx"
]  # 1990 − 1949; lower-bound index in the LDA year cube

# ─── Faction rules ─────────────────────────────────────────────────────────────
FACTION_MERGE = (
    "CASE WHEN f.abbreviation = 'PDS' "
    f"THEN '{_cfg['analysis']['factions']['merge_pds_to']}' "
    "ELSE f.abbreviation END"
)
MAJOR_PARTIES = tuple(_cfg["analysis"]["factions"]["major_parties"])
MAJOR_PARTIES_SQL = ", ".join(f"'{p}'" for p in MAJOR_PARTIES)

FACTION_NORMALIZE = _cfg["analysis"]["factions"].get("normalize", {})
FACTION_LABELS = _cfg["analysis"]["factions"]["labels"]


def normalize_faction(value):
    if pd.isna(value):
        return value
    return FACTION_NORMALIZE.get(value, value)


def normalize_factions(series):
    return series.map(normalize_faction)


def relabel_factions(series):
    return normalize_factions(series).map(lambda x: FACTION_LABELS.get(x, x))


# ─── Party colours ─────────────────────────────────────────────────────────────
PARTY_COLORS = _cfg["colors"]["parties"]

# ─── Gender labels & colours ───────────────────────────────────────────────────
GENDER_LABELS = _cfg["gender"]["labels"]
GENDER_COLORS = _cfg["colors"].get("genders", {"Male": GRAY, "Female": RED})

# ─── Bar style helpers ─────────────────────────────────────────────────────────
BAR_FILL_LIGHTEN = 0.62


def lighten_color(color, amount=BAR_FILL_LIGHTEN):
    """Blend a color toward white by `amount` in [0, 1]."""
    r, g, b = to_rgb(color)
    return to_hex((r + (1 - r) * amount, g + (1 - g) * amount, b + (1 - b) * amount))


def lighten_palette(values, amount=BAR_FILL_LIGHTEN):
    """Lighten a sequence/dict of colors while preserving its structure."""
    if isinstance(values, dict):
        return {k: lighten_color(v, amount=amount) for k, v in values.items()}
    if isinstance(values, (list, tuple, pd.Series, np.ndarray)):
        return [lighten_color(v, amount=amount) for v in values]
    return values


def bar_scale_fill_manual(values, amount=BAR_FILL_LIGHTEN, **kwargs):
    """Manual fill scale tuned for bars: same palette, less saturated."""
    return scale_fill_manual(values=lighten_palette(values, amount=amount), **kwargs)


def bar_geom_col(*args, **kwargs):
    """geom_col for bars with lighter fills and no outlines."""
    fill = kwargs.get("fill")
    if isinstance(fill, str):
        kwargs["fill"] = lighten_color(fill)
    kwargs["color"] = None
    return geom_col(*args, **kwargs)


def bar_geom_bar(*args, **kwargs):
    """geom_bar for bars with lighter fills and no outlines."""
    fill = kwargs.get("fill")
    if isinstance(fill, str):
        kwargs["fill"] = lighten_color(fill)
    kwargs["color"] = None
    return geom_bar(*args, **kwargs)


# ─── Interjection type labels ──────────────────────────────────────────────────
TYPE_LABELS = _cfg["interjection_labels"]

# ─── LDA shared constants ──────────────────────────────────────────────────────
LDA_DIMS = _cfg["lda"]["dims"]
LDA_N_TOPICS = _cfg["lda"]["n_topics"]
LDA_YEAR_STRIDE = LDA_DIMS["year"]

LDA_STRIDE_T = (
    LDA_DIMS["gender"]
    * LDA_DIMS["age"]
    * LDA_DIMS["party"]
    * LDA_DIMS["state"]
    * LDA_DIMS["job"]
    * LDA_DIMS["year"]
)
LDA_STRIDE_G = (
    LDA_DIMS["age"]
    * LDA_DIMS["party"]
    * LDA_DIMS["state"]
    * LDA_DIMS["job"]
    * LDA_DIMS["year"]
)
LDA_STRIDE_A = (
    LDA_DIMS["party"] * LDA_DIMS["state"] * LDA_DIMS["job"] * LDA_DIMS["year"]
)
LDA_STRIDE_P = LDA_DIMS["state"] * LDA_DIMS["job"] * LDA_DIMS["year"]
LDA_STRIDE_S = LDA_DIMS["job"] * LDA_DIMS["year"]
LDA_STRIDE_J = LDA_DIMS["year"]

LDA_STRIDE_T_PERSON = LDA_DIMS["person_politicians"] * LDA_DIMS["year"]
LDA_STRIDE_P_PERSON = LDA_DIMS["year"]

LDA_TOP_N = _cfg["lda"]["top_n"]
LDA_JOB_LABELS = {int(k): v for k, v in _cfg["lda"]["job_labels"].items()}
LDA_ACAD_COLORS = _cfg["lda"]["academic_colors"]

# ─── STM shared constants ──────────────────────────────────────────────────────
STM_TOP_N = _cfg["stm"]["top_n"]
STM_MODEL = _cfg["stm"]["model"]
STM_PLOT = _cfg["stm"]["plot"]


def make_wp_labels(series):
    """Convert an electoral_term int Series to an ordered WP Categorical."""
    labels = series.map(lambda x: f"WP{x}")
    return pd.Categorical(labels, categories=list(dict.fromkeys(labels)), ordered=True)


# ─── MDB Stammdaten ────────────────────────────────────────────────────────────

# Exact XML fraction name --> short label used throughout the project.
MDB_FACTION_MAP = {
    "Fraktion der Christlich Demokratischen Union/Christlich - Sozialen Union": "CDU/CSU",
    "Fraktion der CDU/CSU (Gast)": "CDU/CSU",
    "Fraktion der Sozialdemokratischen Partei Deutschlands": "SPD",
    "Fraktion der SPD (Gast)": "SPD",
    "Fraktion der Freien Demokratischen Partei": "FDP",
    "Fraktion der FDP (Gast)": "FDP",
    "Fraktion BÜNDNIS 90/DIE GRÜNEN": "Grüne",
    "Fraktion Die Grünen": "Grüne",
    "Fraktion Die Grünen/Bündnis 90": "Grüne",
    "Gruppe Bündnis 90/Die Grünen": "Grüne",
    "Fraktion DIE LINKE.": "DIE LINKE",
    "Fraktion Die Linke": "DIE LINKE",
    "Gruppe Die Linke": "DIE LINKE",
    "Fraktion der Partei des Demokratischen Sozialismus": "DIE LINKE",
    "Gruppe der Partei des Demokratischen Sozialismus": "DIE LINKE",
    "Gruppe der Partei des Demokratischen Sozialismus/Linke Liste": "DIE LINKE",
    "Fraktion Alternative für Deutschland": "AfD",
}


def parse_mdb_seats(term_lb: int = TERM_LOWER_BOUNDARY) -> pd.DataFrame:
    """Load MdB seat data from parquet. One row per MdB × electoral term.

    Columns: mdb_id, wp, gender, faction_raw, faction.
    Run `make load-mds` to (re)generate data/mds/mdb_stammdaten.parquet.
    """
    path = DATA_PATH / "mdbs" / "mdb_stammdaten.parquet"
    if not path.exists():
        raise SystemExit(
            f"Missing MdB data at {path}.\nFetch it with:\n  make load-mdbs"
        )
    df = pd.read_parquet(path)
    df = df[df["wp"] >= term_lb].reset_index(drop=True)
    df["faction"] = df["faction_raw"].map(MDB_FACTION_MAP).fillna("Other")
    return df
