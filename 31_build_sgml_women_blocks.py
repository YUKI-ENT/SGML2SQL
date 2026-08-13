#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""sgml_rawdataから妊婦・授乳の原文blockを差分構築する。"""

from __future__ import annotations

import argparse
import logging
import os
import xml.etree.ElementTree as ET
from collections import Counter
from typing import List, Optional

import psycopg2
import psycopg2.extras

from _sgml_note_common import checked_table_name, load_config, normalize_text, sha256_text, stable_json_hash, table_base
from _sgml_women_common import PIPELINE_VERSION


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler(f"logs/{SCRIPT_BASENAME}.log", encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger(__name__)

PREGNANCY_TAGS = {"UseInPregnant", "UseInPregnantWomen", "Pregnant", "PregnantWomen"}
LACTATION_TAGS = {"UseInNursing", "UseInNursingMothers", "Nursing", "BreastFeeding", "LactatingWomen"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="妊婦・授乳原文blockを差分構築")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def local_name(tag: str) -> str:
    return tag.split("}")[-1] if tag else ""


def element_text(element: ET.Element) -> str:
    return normalize_text("".join(element.itertext()))


def heading(element: ET.Element) -> Optional[str]:
    for node in element.iter():
        if local_name(node.tag) in {"Header", "Title"}:
            value = element_text(node)
            if value:
                return value[:300]
    return None


def extract_population_blocks(xml_text: str) -> List[dict]:
    root = ET.fromstring(xml_text)
    raw: List[dict] = []
    for population_type, tags in (("PREGNANCY", PREGNANCY_TAGS), ("LACTATION", LACTATION_TAGS)):
        order = 0
        for element in root.iter():
            if local_name(element.tag) not in tags:
                continue
            text = element_text(element)
            if not text:
                continue
            order += 1
            raw.append({
                "population_type": population_type,
                "block_order": order,
                "section_type": local_name(element.tag),
                "section_code": "9.5" if population_type == "PREGNANCY" else "9.6",
                "heading_path": heading(element),
                "block_text": text,
                "block_xml": ET.tostring(element, encoding="unicode"),
            })
    occurrences: Counter[str] = Counter()
    for block in raw:
        content_hash = stable_json_hash({
            "population_type": block["population_type"],
            "section_type": block["section_type"],
            "heading_path": block["heading_path"],
            "block_text": block["block_text"],
        })
        occurrences[content_hash] += 1
        block["content_hash"] = content_hash
        block["block_uid"] = sha256_text(f"{content_hash}:{occurrences[content_hash]}")
    return raw


def create_tables(conn, state_table: str, block_table: str) -> None:
    state_base, block_base = table_base(state_table), table_base(block_table)
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {state_table} (
          package_insert_no text NOT NULL,
          population_type text NOT NULL,
          prepared_ym text,
          generic_name_ja text,
          raw_xml_hash text NOT NULL,
          semantic_hash text NOT NULL,
          has_section boolean NOT NULL,
          extractor_version text NOT NULL,
          processing_status text NOT NULL,
          error_message text,
          last_seen_at timestamptz NOT NULL DEFAULT now(),
          last_parsed_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (package_insert_no, population_type)
        );
        CREATE INDEX IF NOT EXISTS idx_{state_base}_semantic ON {state_table} (semantic_hash);
        CREATE TABLE IF NOT EXISTS {block_table} (
          block_id bigserial PRIMARY KEY,
          package_insert_no text NOT NULL,
          population_type text NOT NULL,
          block_uid text NOT NULL,
          prepared_ym text,
          generic_name_ja text,
          section_type text NOT NULL,
          section_code text,
          heading_path text,
          block_order integer NOT NULL,
          block_text text NOT NULL,
          block_xml xml,
          content_hash text NOT NULL,
          extractor_version text NOT NULL,
          is_current boolean NOT NULL DEFAULT true,
          first_seen_at timestamptz NOT NULL DEFAULT now(),
          last_seen_at timestamptz NOT NULL DEFAULT now(),
          retired_at timestamptz,
          UNIQUE (package_insert_no, population_type, block_uid)
        );
        CREATE INDEX IF NOT EXISTS idx_{block_base}_current ON {block_table} (is_current, population_type);
        CREATE INDEX IF NOT EXISTS idx_{block_base}_hash ON {block_table} (content_hash);
        """)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    src = checked_table_name(config.get("sgml_table", "public.sgml_rawdata"), "sgml_table")
    state = checked_table_name(config.get("sgml_women_state_table", "public.sgml_women_document_state"), "sgml_women_state_table")
    blocks = checked_table_name(config.get("sgml_women_block_table", "public.sgml_women_block"), "sgml_women_block_table")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limitは1以上にしてください")
    read_conn, write_conn = psycopg2.connect(**config["db"]), psycopg2.connect(**config["db"])
    create_tables(write_conn, state, blocks)
    where, params = ["doc_xml IS NOT NULL"], []
    if args.package_insert_no:
        where.append("package_insert_no=%s")
        params.append(args.package_insert_no)
    limit_sql = ""
    if args.limit is not None:
        limit_sql = " LIMIT %s"
        params.append(args.limit)
    scanned = changed = skipped = failed = 0
    try:
        with read_conn.cursor(name="cur_women_xml") as cur:
            cur.itersize = 200
            cur.execute(f"""SELECT DISTINCT ON (package_insert_no) package_insert_no, prepared_ym, generic_name_ja, doc_xml::text FROM {src} WHERE {' AND '.join(where)} ORDER BY package_insert_no, yj_code{limit_sql}""", params)
            for package_no, prepared_ym, generic_name, xml_text in cur:
                scanned += 1
                raw_hash = sha256_text(xml_text)
                try:
                    extracted = extract_population_blocks(xml_text)
                    for population in ("PREGNANCY", "LACTATION"):
                        selected = [item for item in extracted if item["population_type"] == population]
                        semantic_hash = stable_json_hash([item["block_uid"] for item in selected])
                        with write_conn.cursor() as wcur:
                            wcur.execute(f"SELECT raw_xml_hash, semantic_hash, extractor_version FROM {state} WHERE package_insert_no=%s AND population_type=%s", (package_no, population))
                            old = wcur.fetchone()
                        if old and old[1] == semantic_hash and old[2] == PIPELINE_VERSION and not args.force:
                            skipped += 1
                        else:
                            changed += 1
                            with write_conn.cursor() as wcur:
                                wcur.execute(f"UPDATE {blocks} SET is_current=false, retired_at=now() WHERE package_insert_no=%s AND population_type=%s AND is_current", (package_no, population))
                                rows = [(package_no, population, item["block_uid"], prepared_ym, generic_name, item["section_type"], item["section_code"], item["heading_path"], item["block_order"], item["block_text"], item["block_xml"], item["content_hash"], PIPELINE_VERSION, True, None) for item in selected]
                                if rows:
                                    psycopg2.extras.execute_values(wcur, f"""INSERT INTO {blocks} (package_insert_no,population_type,block_uid,prepared_ym,generic_name_ja,section_type,section_code,heading_path,block_order,block_text,block_xml,content_hash,extractor_version,is_current,retired_at) VALUES %s ON CONFLICT (package_insert_no,population_type,block_uid) DO UPDATE SET prepared_ym=EXCLUDED.prepared_ym,generic_name_ja=EXCLUDED.generic_name_ja,block_order=EXCLUDED.block_order,block_text=EXCLUDED.block_text,block_xml=EXCLUDED.block_xml,extractor_version=EXCLUDED.extractor_version,is_current=true,last_seen_at=now(),retired_at=NULL""", rows)
                        with write_conn.cursor() as wcur:
                            wcur.execute(f"""INSERT INTO {state} (package_insert_no,population_type,prepared_ym,generic_name_ja,raw_xml_hash,semantic_hash,has_section,extractor_version,processing_status,error_message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'success',NULL) ON CONFLICT (package_insert_no,population_type) DO UPDATE SET prepared_ym=EXCLUDED.prepared_ym,generic_name_ja=EXCLUDED.generic_name_ja,raw_xml_hash=EXCLUDED.raw_xml_hash,semantic_hash=EXCLUDED.semantic_hash,has_section=EXCLUDED.has_section,extractor_version=EXCLUDED.extractor_version,processing_status='success',error_message=NULL,last_seen_at=now(),last_parsed_at=now()""", (package_no, population, prepared_ym, generic_name, raw_hash, semantic_hash, bool(selected), PIPELINE_VERSION))
                    write_conn.commit()
                except Exception as exc:
                    write_conn.rollback()
                    failed += 1
                    log.warning("抽出失敗 package=%s: %s", package_no, exc)
        log.info("完了 scanned=%s changed=%s skipped=%s failed=%s", scanned, changed, skipped, failed)
    finally:
        read_conn.close()
        write_conn.close()


if __name__ == "__main__":
    main()

