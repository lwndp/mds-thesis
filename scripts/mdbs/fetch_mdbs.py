#!/usr/bin/env python3
"""Download and parse the Bundestag MdB Stammdaten XML.

Source: https://www.bundestag.de/resource/blob/472878/MdB-Stammdaten.zip

Output: data/mds/mdb_stammdaten.parquet
  One row per MdB × electoral term with raw XML fields.
"""

import io
import json
import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.request import urlopen

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

STAMMDATEN_URL = "https://www.bundestag.de/resource/blob/472878/MdB-Stammdaten.zip"
DEFAULT_DATA_PATH = Path(__file__).resolve().parents[2] / "data"
DATA_PATH = Path(os.getenv("DATA_PATH", str(DEFAULT_DATA_PATH)))


def _t(el: ET.Element | None, tag: str) -> str | None:
    if el is None:
        return None
    node = el.find(tag)
    return (node.text or "").strip() or None


def _download_xml() -> ET.Element:
    print(f"Downloading {STAMMDATEN_URL} ...")
    with urlopen(STAMMDATEN_URL) as resp:
        zip_bytes = resp.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xml_name = next(n for n in zf.namelist() if n.endswith(".XML"))
        with zf.open(xml_name) as f:
            return ET.parse(f).getroot()


def _current_name(mdb: ET.Element) -> ET.Element | None:
    """Return the most recent NAME element (HISTORIE_BIS empty = current)."""
    names = mdb.findall("NAMEN/NAME")
    if not names:
        return None
    return next((n for n in reversed(names) if not _t(n, "HISTORIE_BIS")), names[-1])


def _parse_institutions(wp_el: ET.Element) -> tuple[str, str]:
    """Return (faction_raw, institutions_json) for a WAHLPERIODE element."""
    faction_raw = ""
    institutions = []
    for inst in wp_el.findall("INSTITUTIONEN/INSTITUTION"):
        art = _t(inst, "INSART_LANG") or ""
        name = _t(inst, "INS_LANG") or ""
        if art == "Fraktion/Gruppe" and not faction_raw:
            faction_raw = name
        institutions.append(
            {
                "art": art,
                "name": name,
                "von": _t(inst, "MDBINS_VON"),
                "bis": _t(inst, "MDBINS_BIS"),
                "funktion": _t(inst, "FKT_LANG"),
                "funktion_von": _t(inst, "FKTINS_VON"),
                "funktion_bis": _t(inst, "FKTINS_BIS"),
            }
        )
    return faction_raw, json.dumps(institutions, ensure_ascii=False)


def _parse_rows(root: ET.Element) -> list[dict]:
    rows = []
    for mdb in tqdm(root.findall("MDB"), unit=" MdBs"):
        mdb_id = mdb.findtext("ID")
        name = _current_name(mdb)
        bio = mdb.find("BIOGRAFISCHE_ANGABEN")

        # Biographical fields — repeated for each WP row
        bio_fields = {
            "nachname": _t(name, "NACHNAME"),
            "vorname": _t(name, "VORNAME"),
            "ortszusatz": _t(name, "ORTSZUSATZ"),
            "adel": _t(name, "ADEL"),
            "praefix": _t(name, "PRAEFIX"),
            "anrede_titel": _t(name, "ANREDE_TITEL"),
            "akad_titel": _t(name, "AKAD_TITEL"),
            "gender": _t(bio, "GESCHLECHT"),
            "partei": _t(bio, "PARTEI_KURZ"),
            "geburtsdatum": _t(bio, "GEBURTSDATUM"),
            "geburtsort": _t(bio, "GEBURTSORT"),
            "geburtsland": _t(bio, "GEBURTSLAND"),
            "sterbedatum": _t(bio, "STERBEDATUM"),
            "familienstand": _t(bio, "FAMILIENSTAND"),
            "religion": _t(bio, "RELIGION"),
            "beruf": _t(bio, "BERUF"),
        }

        for wp_el in mdb.findall("WAHLPERIODEN/WAHLPERIODE"):
            faction_raw, institutions_json = _parse_institutions(wp_el)
            rows.append(
                {
                    "mdb_id": mdb_id,
                    "wp": int(wp_el.findtext("WP", 0)),
                    "mdbwp_von": _t(wp_el, "MDBWP_VON"),
                    "mdbwp_bis": _t(wp_el, "MDBWP_BIS"),
                    "wkr_nummer": _t(wp_el, "WKR_NUMMER"),
                    "wkr_name": _t(wp_el, "WKR_NAME"),
                    "wkr_land": _t(wp_el, "WKR_LAND"),
                    "liste": _t(wp_el, "LISTE"),
                    "mandatsart": _t(wp_el, "MANDATSART"),
                    "faction_raw": faction_raw,
                    "institutionen": institutions_json,
                    **bio_fields,
                }
            )
    return rows


def main():
    out_path = DATA_PATH / "mdbs" / "mdb_stammdaten.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        answer = input(f"{out_path} already exists. Overwrite? [Y/n] ").strip().lower()
        if answer != "y":
            print("Skipping.")
            return

    root = _download_xml()
    print(f"Parsing {len(root.findall('MDB'))} MdB records...")
    rows = _parse_rows(root)

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df)} rows → {out_path}")


if __name__ == "__main__":
    main()
