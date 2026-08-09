#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""現行ブロックから定義駆動でLLM投入候補を安価に抽出する。"""

from __future__ import annotations

import argparse
import logging
import os
from typing import List

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    load_config,
    load_definitions,
    normalize_text,
    select_definitions,
    sha256_text,
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ノート抽出候補をキーワードで差分構築")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--definitions", help="note definition JSON")
    parser.add_argument("--package-insert-no", help="指定添付文書だけを処理")
    parser.add_argument("--note-type", help="指定テーマだけを処理")
    parser.add_argument("--limit", type=int, help="走査する現行ブロック数の上限")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    definitions_path = args.definitions or config.get("sgml_note_definitions", "sgml_note_definitions.json")
    definitions = select_definitions(load_definitions(definitions_path), args.note_type)
    block_table = checked_table_name(
        config.get("sgml_note_block_table", "public.sgml_note_block"),
        "sgml_note_block_table",
    )
    candidate_table = checked_table_name(
        config.get("temp_sgml_note_candidate_table", "public.temp_sgml_note_candidate"),
        "temp_sgml_note_candidate_table",
    )
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit は1以上にしてください")

    conn = psycopg2.connect(**config["db"])
    candidate_base = table_base(candidate_table)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {candidate_table} (
      candidate_id         bigserial PRIMARY KEY,
      block_id             bigint NOT NULL,
      package_insert_no    text NOT NULL,
      content_hash         text NOT NULL,
      note_type            text NOT NULL,
      definition_version   text NOT NULL,
      candidate_hash       text NOT NULL,
      matched_terms        jsonb NOT NULL DEFAULT '[]'::jsonb,
      candidate_reasons    jsonb NOT NULL DEFAULT '[]'::jsonb,
      is_current           boolean NOT NULL DEFAULT true,
      first_seen_at        timestamptz NOT NULL DEFAULT now(),
      last_seen_at         timestamptz NOT NULL DEFAULT now(),
      retired_at           timestamptz,
      UNIQUE (block_id, note_type, definition_version)
    );
    CREATE INDEX IF NOT EXISTS idx_{candidate_base}_package
      ON {candidate_table} (package_insert_no);
    CREATE INDEX IF NOT EXISTS idx_{candidate_base}_current_type
      ON {candidate_table} (is_current, note_type, definition_version);
    CREATE INDEX IF NOT EXISTS idx_{candidate_base}_hash
      ON {candidate_table} (candidate_hash);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

        where = ["is_current"]
        params: List[object] = []
        if args.package_insert_no:
            where.append("package_insert_no = %s")
            params.append(args.package_insert_no)
        limit_sql = ""
        if args.limit is not None:
            limit_sql = " LIMIT %s"
            params.append(args.limit)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT block_id, package_insert_no, content_hash,
                            top_section_type, section_code, section_type, heading_path,
                            block_type, block_text
                       FROM {block_table}
                      WHERE {' AND '.join(where)}
                      ORDER BY block_id{limit_sql}""",
                params,
            )
            blocks = cur.fetchall()

        with conn.cursor() as cur:
            for definition in definitions:
                deactivate_where = ["note_type=%s", "is_current"]
                deactivate_params: List[object] = [definition["note_type"]]
                if args.package_insert_no:
                    deactivate_where.append("package_insert_no=%s")
                    deactivate_params.append(args.package_insert_no)
                if args.limit is not None:
                    deactivate_where.append("block_id = ANY(%s)")
                    deactivate_params.append([block["block_id"] for block in blocks])
                cur.execute(
                    f"""UPDATE {candidate_table}
                           SET is_current=false, retired_at=now()
                         WHERE {' AND '.join(deactivate_where)}""",
                    deactivate_params,
                )
        conn.commit()

        insert_sql = f"""
          INSERT INTO {candidate_table}
          (block_id, package_insert_no, content_hash, note_type,
           definition_version, candidate_hash, matched_terms,
           candidate_reasons, is_current, retired_at)
          VALUES %s
          ON CONFLICT (block_id, note_type, definition_version) DO UPDATE SET
            content_hash=EXCLUDED.content_hash,
            candidate_hash=EXCLUDED.candidate_hash,
            matched_terms=EXCLUDED.matched_terms,
            candidate_reasons=EXCLUDED.candidate_reasons,
            is_current=true, last_seen_at=now(), retired_at=NULL
        """
        rows = []
        counts = {definition["note_type"]: 0 for definition in definitions}
        for block in blocks:
            normalized = normalize_text(block["block_text"]).casefold()
            for definition in definitions:
                allowed_top_sections = set(definition.get("allowed_top_sections", []))
                if allowed_top_sections and block["top_section_type"] not in allowed_top_sections:
                    continue
                allowed_sections = set(definition.get("allowed_sections", []))
                if allowed_sections and block["section_code"] not in allowed_sections:
                    continue
                allowed_block_types = set(definition.get("allowed_block_types", []))
                if allowed_block_types and block["block_type"] not in allowed_block_types:
                    continue
                excluded_sections = set(definition.get("excluded_sections", []))
                if block["section_code"] in excluded_sections:
                    continue
                matched = sorted(
                    {
                        term
                        for term in definition.get("candidate_terms", [])
                        if normalize_text(str(term)).casefold() in normalized
                    }
                )
                if not matched:
                    continue
                candidate_required_groups = definition.get("candidate_required_term_groups", [])
                if candidate_required_groups and not all(
                    any(normalize_text(str(term)).casefold() in normalized for term in term_group)
                    for term_group in candidate_required_groups
                ):
                    continue
                reasons = ["keyword"]
                if block["section_code"] in set(definition.get("preferred_sections", [])):
                    reasons.append("preferred_section")
                candidate_hash = sha256_text(
                    f"{block['content_hash']}\n{definition['note_type']}\n{definition['definition_version']}"
                )
                rows.append(
                    (
                        block["block_id"],
                        block["package_insert_no"],
                        block["content_hash"],
                        definition["note_type"],
                        definition["definition_version"],
                        candidate_hash,
                        psycopg2.extras.Json(matched),
                        psycopg2.extras.Json(reasons),
                        True,
                        None,
                    )
                )
                counts[definition["note_type"]] += 1
        if rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
            conn.commit()
        log.info("完了 blocks=%s candidates=%s details=%s", len(blocks), len(rows), counts)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
