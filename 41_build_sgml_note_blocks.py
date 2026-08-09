#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""sgml_rawdata.doc_xml から差分管理可能な汎用ノートブロックを構築する。"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    load_config,
    normalize_text,
    sha256_text,
    stable_json_hash,
    table_base,
)


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/{SCRIPT_BASENAME}.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


SECTION_CODES = {
    "Warnings": "1",
    "ContraIndications": "2",
    "CompositionAndProperty": "3",
    "IndicationsOrEfficacy": "4",
    "EfficacyRelatedPrecautions": "5",
    "InfoDoseAdmin": "6",
    "InfoPrecautionsDosage": "7",
    "ImportantPrecautions": "8",
    "UseInSpecificPopulations": "9",
    "Interactions": "10",
    "AdverseEvents": "11",
    "InfluenceOnLaboratoryValues": "12",
    "OverDosage": "13",
    "PrecautionsForApplication": "14",
    "OtherPrecautions": "15",
    "Pharmacokinetics": "16",
    "ResultsOfClinicalTrials": "17",
    "EfficacyPharmacology": "18",
    "PhyschemOfActIngredients": "19",
    "PrecautionsForHandling": "20",
    "ConditionsOfApproval": "21",
    "Package": "22",
    "MainLiterature": "23",
}

PK_SECTION_CODES = {
    "BloodLevel": "16.1",
    "Absorption": "16.2",
    "Distribution": "16.3",
    "Metabolism": "16.4",
    "Excretion": "16.5",
    "SpecificPopulation": "16.6",
    "DrugAndDrugInteractions": "16.7",
    "PharmacokineticsEtc": "16.8",
}

SKIP_TOP_LEVEL = {
    "PackageInsertNo",
    "CompanyIdentifier",
    "DateOfPreparationOrRevision",
    "Version",
    "JapaneseStandardCommodityClassificationNo",
    "TherapeuticClassification",
    "ApprovalEtc",
    "GenericName",
}

# ノート候補として利用価値が高い章だけを既定でブロック化する。
# 後から対象を増やす場合は config.json の note_included_top_sections で上書きする。
DEFAULT_INCLUDED_TOP_LEVEL = {
    "Warnings",
    "ContraIndications",
    "ImportantPrecautions",
    "UseInSpecificPopulations",
    "AdverseEvents",
    "InfluenceOnLaboratoryValues",
    "OverDosage",
    "OtherPrecautions",
    "Pharmacokinetics",
    "ResultsOfClinicalTrials",
    "EfficacyPharmacology",
}

LIST_TAGS = {"OrderedList", "UnorderedList", "SimpleList"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="sgml_rawdataから汎用ノートブロックを差分構築")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no", help="指定添付文書だけを処理")
    parser.add_argument("--limit", type=int, help="今回走査する添付文書数の上限")
    parser.add_argument("--max-block-length", type=int, help="長い意味ブロックを分割する最大文字数")
    parser.add_argument("--force", action="store_true", help="raw_xml_hashが同じ文書も再解析")
    return parser.parse_args()


def local_name(tag: str) -> str:
    return tag.split("}")[-1] if tag else ""


def element_text(element: ET.Element) -> str:
    return normalize_text("".join(element.itertext()))


def element_text_without_tables(element: ET.Element) -> str:
    """薬物動態ノート用に数値表・図を除き、本文と見出しだけを残す。"""
    parts: List[str] = []

    def visit(node: ET.Element) -> None:
        if local_name(node.tag) in {"TblBlock", "Graphic"}:
            return
        if node.text:
            parts.append(node.text)
        for child in list(node):
            visit(child)
            if child.tail:
                parts.append(child.tail)

    visit(element)
    return normalize_text("".join(parts))


def first_heading(element: ET.Element) -> Optional[str]:
    for node in element.iter():
        if local_name(node.tag) in {"Header", "TblCaption", "GraphicCaption"}:
            value = element_text(node)
            if value:
                return value[:300]
    return None


def choose_break(text: str, start: int, desired_end: int) -> int:
    if desired_end >= len(text):
        return len(text)
    lower = start + max(1, (desired_end - start) * 2 // 3)
    candidates = []
    for marker in ("。", "\n", "；", ";", " "):
        pos = text.rfind(marker, lower, desired_end)
        if pos >= lower:
            candidates.append(pos + len(marker))
    return max(candidates) if candidates else desired_end


def split_long_text(text: str, max_length: int) -> Iterable[str]:
    if len(text) <= max_length:
        yield text
        return
    start = 0
    while start < len(text):
        end = choose_break(text, start, min(start + max_length, len(text)))
        piece = text[start:end].strip()
        if piece:
            yield piece
        if end <= start:
            break
        start = end


def semantic_units(container: ET.Element) -> List[ET.Element]:
    children = list(container)
    if not children:
        return [container]
    units: List[ET.Element] = []
    for child in children:
        child_name = local_name(child.tag)
        if child_name in LIST_TAGS:
            items = [node for node in list(child) if local_name(node.tag) == "Item"]
            units.extend(items or [child])
        elif child_name in {"Detail", "TblBlock", "Graphic", "Item"}:
            units.append(child)
        else:
            nested_lists = [
                node for node in child.iter()
                if node is not child and local_name(node.tag) in LIST_TAGS
            ]
            if nested_lists:
                units.extend(semantic_units(child))
            else:
                text = element_text(child)
                if text:
                    units.append(child)
    return units or [container]


def extract_blocks(xml_text: str, max_length: int, included_top_sections: set[str]) -> List[dict]:
    root = ET.fromstring(xml_text)
    raw_blocks: List[dict] = []
    block_order = 0
    for top in list(root):
        top_type = local_name(top.tag)
        if top_type in SKIP_TOP_LEVEL or top_type not in included_top_sections:
            continue
        top_code = SECTION_CODES.get(top_type)
        containers: List[Tuple[ET.Element, str, Optional[str]]] = []
        if top_type == "Pharmacokinetics":
            for subsection in list(top):
                subsection_type = local_name(subsection.tag)
                containers.append((subsection, subsection_type, PK_SECTION_CODES.get(subsection_type, "16")))
            if not containers:
                containers.append((top, top_type, top_code))
        else:
            containers.append((top, top_type, top_code))

        for container, section_type, section_code in containers:
            for unit in semantic_units(container):
                text = element_text_without_tables(unit) if top_type == "Pharmacokinetics" else element_text(unit)
                if len(text) < 4:
                    continue
                heading = first_heading(unit)
                heading_parts = [top_type]
                if section_type != top_type:
                    heading_parts.append(section_type)
                if heading:
                    heading_parts.append(heading)
                heading_path = " > ".join(heading_parts)
                unit_xml = ET.tostring(unit, encoding="unicode")
                for part_no, piece in enumerate(split_long_text(text, max_length), start=1):
                    block_order += 1
                    effective_heading = heading_path
                    if len(text) > max_length:
                        effective_heading += f" > part {part_no}"
                    raw_blocks.append(
                        {
                            "top_section_type": top_type,
                            "section_type": section_type,
                            "section_code": section_code,
                            "heading_path": effective_heading,
                            "block_type": local_name(unit.tag),
                            "block_order": block_order,
                            "block_text": piece,
                            "block_xml": unit_xml if part_no == 1 else None,
                        }
                    )

    occurrences: Counter = Counter()
    for block in raw_blocks:
        content_key = {
            "top_section_type": block["top_section_type"],
            "section_type": block["section_type"],
            "section_code": block["section_code"],
            "heading_path": block["heading_path"],
            "block_text": block["block_text"],
        }
        content_hash = stable_json_hash(content_key)
        occurrences[content_hash] += 1
        block["content_hash"] = content_hash
        block["block_uid"] = sha256_text(f"{content_hash}:{occurrences[content_hash]}")
    return raw_blocks


def create_tables(conn, state_table: str, block_table: str) -> None:
    state_base = table_base(state_table)
    block_base = table_base(block_table)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {state_table} (
      package_insert_no      text PRIMARY KEY,
      prepared_ym            text,
      generic_name_ja        text,
      raw_xml_hash           text NOT NULL,
      semantic_manifest_hash text NOT NULL,
      extractor_version      text NOT NULL DEFAULT 'sgml-block-v7',
      block_count            integer NOT NULL,
      processing_status      text NOT NULL,
      error_message          text,
      last_seen_at           timestamptz NOT NULL DEFAULT now(),
      last_parsed_at         timestamptz NOT NULL DEFAULT now(),
      last_success_at        timestamptz
    );
    CREATE INDEX IF NOT EXISTS idx_{state_base}_raw_hash ON {state_table} (raw_xml_hash);

    CREATE TABLE IF NOT EXISTS {block_table} (
      block_id              bigserial PRIMARY KEY,
      package_insert_no     text NOT NULL,
      block_uid             text NOT NULL,
      prepared_ym           text,
      generic_name_ja       text,
      top_section_type      text NOT NULL,
      section_type          text NOT NULL,
      section_code          text,
      heading_path          text,
      block_type            text NOT NULL,
      block_order           integer NOT NULL,
      block_text            text NOT NULL,
      block_xml             xml,
      content_hash          text NOT NULL,
      extractor_version     text NOT NULL DEFAULT 'sgml-block-v7',
      is_current            boolean NOT NULL DEFAULT true,
      first_seen_at         timestamptz NOT NULL DEFAULT now(),
      last_seen_at          timestamptz NOT NULL DEFAULT now(),
      retired_at            timestamptz,
      UNIQUE (package_insert_no, block_uid)
    );
    CREATE INDEX IF NOT EXISTS idx_{block_base}_package ON {block_table} (package_insert_no);
    CREATE INDEX IF NOT EXISTS idx_{block_base}_content_hash ON {block_table} (content_hash);
    CREATE INDEX IF NOT EXISTS idx_{block_base}_current_section
      ON {block_table} (is_current, section_code, section_type);
    ALTER TABLE {state_table}
      ADD COLUMN IF NOT EXISTS extractor_version text NOT NULL DEFAULT 'sgml-block-v7';
    ALTER TABLE {block_table}
      ADD COLUMN IF NOT EXISTS extractor_version text NOT NULL DEFAULT 'sgml-block-v7';
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    src_table = checked_table_name(config.get("sgml_table", "public.sgml_rawdata"), "sgml_table")
    state_table = checked_table_name(
        config.get("sgml_note_document_state_table", "public.sgml_note_document_state"),
        "sgml_note_document_state_table",
    )
    block_table = checked_table_name(
        config.get("sgml_note_block_table", "public.sgml_note_block"),
        "sgml_note_block_table",
    )
    max_length = args.max_block_length or int(config.get("note_max_block_length", 3000))
    extractor_version = str(config.get("note_block_extractor_version", "sgml-block-v7"))
    configured_sections = config.get("note_included_top_sections")
    if configured_sections is None:
        included_top_sections = set(DEFAULT_INCLUDED_TOP_LEVEL)
    elif isinstance(configured_sections, list) and all(isinstance(item, str) for item in configured_sections):
        included_top_sections = set(configured_sections)
    else:
        raise ValueError("note_included_top_sections は章タグ名の配列にしてください")
    if max_length < 500:
        raise ValueError("max-block-length は500以上にしてください")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit は1以上にしてください")

    read_conn = psycopg2.connect(**config["db"])
    write_conn = psycopg2.connect(**config["db"])
    create_tables(write_conn, state_table, block_table)
    where = ["doc_xml IS NOT NULL"]
    params: List[object] = []
    if args.package_insert_no:
        where.append("package_insert_no = %s")
        params.append(args.package_insert_no)
    limit_sql = ""
    if args.limit is not None:
        limit_sql = " LIMIT %s"
        params.append(args.limit)
    query = f"""
      SELECT DISTINCT ON (package_insert_no)
             package_insert_no, prepared_ym, generic_name_ja, doc_xml::text
        FROM {src_table}
       WHERE {' AND '.join(where)}
       ORDER BY package_insert_no, yj_code
       {limit_sql}
    """
    insert_sql = f"""
      INSERT INTO {block_table}
      (package_insert_no, block_uid, prepared_ym, generic_name_ja,
       top_section_type, section_type, section_code, heading_path,
       block_type, block_order, block_text, block_xml, content_hash,
       extractor_version, is_current, retired_at)
      VALUES %s
      ON CONFLICT (package_insert_no, block_uid) DO UPDATE SET
        prepared_ym = EXCLUDED.prepared_ym,
        generic_name_ja = EXCLUDED.generic_name_ja,
        block_order = EXCLUDED.block_order,
        block_text = EXCLUDED.block_text,
        block_xml = COALESCE(EXCLUDED.block_xml, {table_base(block_table)}.block_xml),
        extractor_version = EXCLUDED.extractor_version,
        is_current = true,
        last_seen_at = now(),
        retired_at = NULL
    """

    scanned = skipped_raw = semantic_same = changed = failed = 0
    started = time.time()
    try:
        with read_conn.cursor(name="cur_sgml_note_xml") as scan:
            scan.itersize = 200
            scan.execute(query, params)
            for package_insert_no, prepared_ym, generic_name_ja, xml_text in scan:
                scanned += 1
                raw_hash = sha256_text(xml_text)
                try:
                    with write_conn.cursor() as cur:
                        cur.execute(
                            f"SELECT raw_xml_hash, semantic_manifest_hash, extractor_version FROM {state_table} WHERE package_insert_no = %s",
                            (package_insert_no,),
                        )
                        old = cur.fetchone()
                    if old and old[0] == raw_hash and old[2] == extractor_version and not args.force:
                        skipped_raw += 1
                        with write_conn.cursor() as cur:
                            cur.execute(
                                f"UPDATE {block_table} SET extractor_version=%s WHERE package_insert_no=%s AND is_current",
                                (extractor_version, package_insert_no),
                            )
                            cur.execute(
                                f"UPDATE {state_table} SET prepared_ym=%s, generic_name_ja=%s, last_seen_at=now() WHERE package_insert_no=%s",
                                (prepared_ym, generic_name_ja, package_insert_no),
                            )
                        write_conn.commit()
                        continue

                    blocks = extract_blocks(xml_text, max_length, included_top_sections)
                    manifest_hash = stable_json_hash(sorted(block["block_uid"] for block in blocks))
                    if old and old[1] == manifest_hash and not args.force:
                        semantic_same += 1
                        with write_conn.cursor() as cur:
                            cur.execute(
                                f"UPDATE {block_table} SET extractor_version=%s WHERE package_insert_no=%s AND is_current",
                                (extractor_version, package_insert_no),
                            )
                            cur.execute(
                                f"""UPDATE {state_table}
                                       SET prepared_ym=%s, generic_name_ja=%s, raw_xml_hash=%s,
                                           processing_status='success', error_message=NULL,
                                           extractor_version=%s,
                                           last_seen_at=now(), last_parsed_at=now(), last_success_at=now()
                                     WHERE package_insert_no=%s""",
                                (prepared_ym, generic_name_ja, raw_hash, extractor_version, package_insert_no),
                            )
                        write_conn.commit()
                        continue

                    with write_conn.cursor() as cur:
                        cur.execute(
                            f"""UPDATE {block_table}
                                   SET is_current=false, retired_at=now()
                                 WHERE package_insert_no=%s AND is_current""",
                            (package_insert_no,),
                        )
                        values = [
                            (
                                package_insert_no,
                                block["block_uid"],
                                prepared_ym,
                                generic_name_ja,
                                block["top_section_type"],
                                block["section_type"],
                                block["section_code"],
                                block["heading_path"],
                                block["block_type"],
                                block["block_order"],
                                block["block_text"],
                                block["block_xml"],
                                block["content_hash"],
                                extractor_version,
                                True,
                                None,
                            )
                            for block in blocks
                        ]
                        if values:
                            psycopg2.extras.execute_values(cur, insert_sql, values, page_size=500)
                        cur.execute(
                            f"""INSERT INTO {state_table}
                                (package_insert_no, prepared_ym, generic_name_ja,
                                 raw_xml_hash, semantic_manifest_hash, block_count,
                                 extractor_version, processing_status, error_message, last_success_at)
                              VALUES (%s,%s,%s,%s,%s,%s,%s,'success',NULL,now())
                              ON CONFLICT (package_insert_no) DO UPDATE SET
                                prepared_ym=EXCLUDED.prepared_ym,
                                generic_name_ja=EXCLUDED.generic_name_ja,
                                raw_xml_hash=EXCLUDED.raw_xml_hash,
                                semantic_manifest_hash=EXCLUDED.semantic_manifest_hash,
                                block_count=EXCLUDED.block_count,
                                extractor_version=EXCLUDED.extractor_version,
                                processing_status='success', error_message=NULL,
                                last_seen_at=now(), last_parsed_at=now(), last_success_at=now()""",
                            (
                                package_insert_no, prepared_ym, generic_name_ja,
                                raw_hash, manifest_hash, len(blocks), extractor_version,
                            ),
                        )
                    write_conn.commit()
                    changed += 1
                except Exception as exc:
                    write_conn.rollback()
                    failed += 1
                    log.exception("ブロック構築失敗 package=%s", package_insert_no)
                    with write_conn.cursor() as cur:
                        cur.execute(
                            f"""INSERT INTO {state_table}
                                (package_insert_no, prepared_ym, generic_name_ja,
                                 raw_xml_hash, semantic_manifest_hash, block_count,
                                 processing_status, error_message)
                              VALUES (%s,%s,%s,%s,%s,0,'error',%s)
                              ON CONFLICT (package_insert_no) DO UPDATE SET
                                prepared_ym=EXCLUDED.prepared_ym,
                                generic_name_ja=EXCLUDED.generic_name_ja,
                                processing_status='error', error_message=EXCLUDED.error_message,
                                last_seen_at=now(), last_parsed_at=now()""",
                            (package_insert_no, prepared_ym, generic_name_ja, raw_hash, sha256_text(""), str(exc)[:2000]),
                        )
                    write_conn.commit()
                if scanned % 500 == 0:
                    log.info(
                        "進捗 scanned=%s changed=%s raw_same=%s semantic_same=%s failed=%s",
                        scanned, changed, skipped_raw, semantic_same, failed,
                    )
    finally:
        read_conn.close()
        write_conn.close()
    log.info(
        "完了 scanned=%s changed=%s raw_same=%s semantic_same=%s failed=%s elapsed=%.1fs",
        scanned, changed, skipped_raw, semantic_same, failed, time.time() - started,
    )


if __name__ == "__main__":
    main()
