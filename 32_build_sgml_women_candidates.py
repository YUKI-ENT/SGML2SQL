#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""現在の妊婦・授乳blockを文単位に分割し、定型表現を仮分類する。"""

from __future__ import annotations

import argparse
import logging
import os
from collections import Counter
from typing import List

import psycopg2
import psycopg2.extras

from _sgml_note_common import checked_table_name, load_config, table_base
from _sgml_women_common import PIPELINE_VERSION, classify_statement, split_statements, statement_hash


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler(f"logs/{SCRIPT_BASENAME}.log", encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="妊婦・授乳の全文表現候補を作成")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--population-type", choices=["PREGNANCY", "LACTATION"])
    return parser.parse_args()


def create_table(conn, table: str) -> None:
    base = table_base(table)
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          candidate_id bigserial PRIMARY KEY,
          block_id bigint NOT NULL,
          package_insert_no text NOT NULL,
          population_type text NOT NULL,
          statement_order integer NOT NULL,
          statement_hash text NOT NULL,
          evidence_text text NOT NULL,
          expression_type text NOT NULL,
          rule_classification text,
          recommendation_target text,
          requires_llm boolean NOT NULL,
          definition_version text NOT NULL,
          is_current boolean NOT NULL DEFAULT true,
          first_seen_at timestamptz NOT NULL DEFAULT now(),
          last_seen_at timestamptz NOT NULL DEFAULT now(),
          retired_at timestamptz,
          UNIQUE (block_id, statement_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_{base}_current ON {table} (is_current, population_type, requires_llm);
        CREATE INDEX IF NOT EXISTS idx_{base}_package ON {table} (package_insert_no);
        """)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    block_table = checked_table_name(config.get("sgml_women_block_table", "public.sgml_women_block"), "sgml_women_block_table")
    candidate_table = checked_table_name(config.get("temp_sgml_women_candidate_table", "public.temp_sgml_women_candidate"), "temp_sgml_women_candidate_table")
    conn = psycopg2.connect(**config["db"])
    create_table(conn, candidate_table)
    where, params = ["is_current"], []
    if args.package_insert_no:
        where.append("package_insert_no=%s")
        params.append(args.package_insert_no)
    if args.population_type:
        where.append("population_type=%s")
        params.append(args.population_type)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT block_id,package_insert_no,population_type,block_text FROM {block_table} WHERE {' AND '.join(where)} ORDER BY block_id", params)
            blocks = [dict(row) for row in cur.fetchall()]
        block_ids = [row["block_id"] for row in blocks]
        if block_ids:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {candidate_table} SET is_current=false,retired_at=now() WHERE block_id=ANY(%s) AND is_current", (block_ids,))
        rows: List[tuple] = []
        counts: Counter[str] = Counter()
        for block in blocks:
            occurrences: Counter[str] = Counter()
            for order, evidence in enumerate(split_statements(block["block_text"]), start=1):
                occurrences[evidence] += 1
                classified = classify_statement(block["population_type"], evidence)
                digest = statement_hash(block["population_type"], evidence, occurrences[evidence])
                rows.append((block["block_id"], block["package_insert_no"], block["population_type"], order, digest, evidence, classified["expression_type"], classified["classification_code"], classified["recommendation_target"], classified["requires_llm"], PIPELINE_VERSION, True, None))
                counts[classified["classification_code"] or classified["expression_type"]] += 1
        if rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, f"""INSERT INTO {candidate_table} (block_id,package_insert_no,population_type,statement_order,statement_hash,evidence_text,expression_type,rule_classification,recommendation_target,requires_llm,definition_version,is_current,retired_at) VALUES %s ON CONFLICT (block_id,statement_hash) DO UPDATE SET statement_order=EXCLUDED.statement_order,evidence_text=EXCLUDED.evidence_text,expression_type=EXCLUDED.expression_type,rule_classification=EXCLUDED.rule_classification,recommendation_target=EXCLUDED.recommendation_target,requires_llm=EXCLUDED.requires_llm,definition_version=EXCLUDED.definition_version,is_current=true,last_seen_at=now(),retired_at=NULL""", rows, page_size=500)
        conn.commit()
        log.info("完了 blocks=%s statements=%s details=%s", len(blocks), len(rows), dict(counts))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

