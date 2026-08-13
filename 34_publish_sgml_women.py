#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""全原文表現とアプリ向け妊婦・授乳総合判定を公開する。"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import psycopg2
import psycopg2.extras

from _sgml_note_common import checked_table_name, load_config, stable_json_hash, table_base
from _sgml_women_common import CLASSIFICATION_META, assessment_for_codes


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler(f"logs/{SCRIPT_BASENAME}.log", encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="妊婦・授乳statementとsummaryを公開")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--population-type", choices=["PREGNANCY", "LACTATION"])
    parser.add_argument("--model")
    parser.add_argument("--prompt-version")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def create_tables(conn, statement_table: str, summary_table: str) -> None:
    statement_base, summary_base = table_base(statement_table), table_base(summary_table)
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {statement_table} (
          statement_id bigserial PRIMARY KEY,
          package_insert_no text NOT NULL,
          population_type text NOT NULL,
          prepared_ym text,
          generic_name_ja text,
          expression_type text NOT NULL,
          classification_code text,
          recommendation_target text,
          display_level text,
          assessment_text text,
          evidence_text text NOT NULL,
          source_block_id bigint NOT NULL,
          section_code text,
          section_type text,
          heading_path text,
          source_hash text NOT NULL,
          statement_hash text NOT NULL,
          extraction_method text NOT NULL,
          definition_version text NOT NULL,
          prompt_version text,
          model_name text,
          review_status text NOT NULL,
          is_current boolean NOT NULL DEFAULT true,
          first_published_at timestamptz NOT NULL DEFAULT now(),
          last_published_at timestamptz NOT NULL DEFAULT now(),
          superseded_at timestamptz,
          UNIQUE (package_insert_no, population_type, source_block_id, statement_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_{statement_base}_current ON {statement_table} (is_current, population_type, display_level);
        CREATE INDEX IF NOT EXISTS idx_{statement_base}_package ON {statement_table} (package_insert_no);
        CREATE TABLE IF NOT EXISTS {summary_table} (
          package_insert_no text NOT NULL,
          population_type text NOT NULL,
          prepared_ym text,
          generic_name_ja text,
          assessment_code text NOT NULL,
          display_level text NOT NULL,
          assessment_text text NOT NULL,
          reason_statement_id bigint,
          needs_review boolean NOT NULL,
          has_unclassified boolean NOT NULL,
          statement_count integer NOT NULL,
          definition_version text NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (package_insert_no, population_type)
        );
        CREATE INDEX IF NOT EXISTS idx_{summary_base}_display ON {summary_table} (population_type, display_level);
        """)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    state_table = checked_table_name(config.get("sgml_women_state_table", "public.sgml_women_document_state"), "sgml_women_state_table")
    block_table = checked_table_name(config.get("sgml_women_block_table", "public.sgml_women_block"), "sgml_women_block_table")
    candidate_table = checked_table_name(config.get("temp_sgml_women_candidate_table", "public.temp_sgml_women_candidate"), "temp_sgml_women_candidate_table")
    run_table = checked_table_name(config.get("temp_sgml_women_run_table", "public.temp_sgml_women_run"), "temp_sgml_women_run_table")
    fact_table = checked_table_name(config.get("temp_sgml_women_fact_table", "public.temp_sgml_women_fact"), "temp_sgml_women_fact_table")
    statement_table = checked_table_name(config.get("sgml_women_statement_table", "public.sgml_women_statement"), "sgml_women_statement_table")
    summary_table = checked_table_name(config.get("sgml_women_summary_table", "public.sgml_women_summary"), "sgml_women_summary_table")
    model = args.model or config.get("women_ollama_model", config.get("note_ollama_model", "gpt-oss:20b"))
    prompt_version = args.prompt_version or config.get("women_prompt_version", "sgml-women-v1")
    conn = psycopg2.connect(**config["db"])
    create_tables(conn, statement_table, summary_table)
    where, params = ["c.is_current", "b.is_current"], []
    state_where, state_params = ["processing_status='success'"], []
    if args.package_insert_no:
        where.append("c.package_insert_no=%s")
        params.append(args.package_insert_no)
        state_where.append("package_insert_no=%s")
        state_params.append(args.package_insert_no)
    if args.population_type:
        where.append("c.population_type=%s")
        params.append(args.population_type)
        state_where.append("population_type=%s")
        state_params.append(args.population_type)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""SELECT c.*,b.prepared_ym,b.generic_name_ja,b.section_code,b.section_type,b.heading_path,b.content_hash FROM {candidate_table} c JOIN {block_table} b ON b.block_id=c.block_id WHERE {' AND '.join(where)} ORDER BY c.package_insert_no,c.population_type,c.block_id,c.statement_order""", params)
            candidates = [dict(row) for row in cur.fetchall()]
            cur.execute(f"SELECT * FROM {state_table} WHERE {' AND '.join(state_where)} ORDER BY package_insert_no,population_type", state_params)
            states = [dict(row) for row in cur.fetchall()]
            cur.execute(f"""SELECT r.candidate_id,r.prompt_version,r.model_name,f.classification_code,f.recommendation_target,f.assessment_text FROM {run_table} r JOIN {fact_table} f ON f.run_id=r.run_id WHERE r.status='success' AND r.prompt_version=%s AND r.model_name=%s AND f.validation_status='AUTO_VALIDATED'""", (prompt_version, model))
            llm_facts = {row["candidate_id"]: dict(row) for row in cur.fetchall()}
        publication_rows = []
        for candidate in candidates:
            llm = llm_facts.get(candidate["candidate_id"]) if candidate["requires_llm"] else None
            code = llm["classification_code"] if llm else candidate["rule_classification"]
            target = llm["recommendation_target"] if llm else candidate["recommendation_target"]
            if code in CLASSIFICATION_META:
                display_level, default_text, _priority = CLASSIFICATION_META[code]
                assessment_text = llm["assessment_text"] if llm else default_text
            else:
                display_level, assessment_text = None, None
            method = "LLM" if llm else "RULE" if code and code != "UNCLASSIFIABLE" else "UNCLASSIFIED" if code else "SOURCE"
            review_status = "NEEDS_REVIEW" if code == "UNCLASSIFIABLE" else "AUTO_VALIDATED"
            publication_rows.append((candidate["package_insert_no"], candidate["population_type"], candidate["prepared_ym"], candidate["generic_name_ja"], candidate["expression_type"], code, target, display_level, assessment_text, candidate["evidence_text"], candidate["block_id"], candidate["section_code"], candidate["section_type"], candidate["heading_path"], candidate["content_hash"], candidate["statement_hash"], method, candidate["definition_version"], prompt_version if llm else None, model if llm else None, review_status, True, None))
        log.info("公開候補 statements=%s summaries=%s", len(publication_rows), len(states))
        if args.dry_run:
            return
        scope_packages = sorted({state["package_insert_no"] for state in states})
        with conn.cursor() as cur:
            deactivate = ["is_current"]
            deactivate_params: List[object] = []
            if scope_packages:
                deactivate.append("package_insert_no=ANY(%s)")
                deactivate_params.append(scope_packages)
            if args.population_type:
                deactivate.append("population_type=%s")
                deactivate_params.append(args.population_type)
            if scope_packages:
                cur.execute(f"UPDATE {statement_table} SET is_current=false,superseded_at=now() WHERE {' AND '.join(deactivate)}", deactivate_params)
            if publication_rows:
                psycopg2.extras.execute_values(cur, f"""INSERT INTO {statement_table} (package_insert_no,population_type,prepared_ym,generic_name_ja,expression_type,classification_code,recommendation_target,display_level,assessment_text,evidence_text,source_block_id,section_code,section_type,heading_path,source_hash,statement_hash,extraction_method,definition_version,prompt_version,model_name,review_status,is_current,superseded_at) VALUES %s ON CONFLICT (package_insert_no,population_type,source_block_id,statement_hash) DO UPDATE SET prepared_ym=EXCLUDED.prepared_ym,generic_name_ja=EXCLUDED.generic_name_ja,expression_type=EXCLUDED.expression_type,classification_code=EXCLUDED.classification_code,recommendation_target=EXCLUDED.recommendation_target,display_level=EXCLUDED.display_level,assessment_text=EXCLUDED.assessment_text,evidence_text=EXCLUDED.evidence_text,source_hash=EXCLUDED.source_hash,extraction_method=EXCLUDED.extraction_method,definition_version=EXCLUDED.definition_version,prompt_version=EXCLUDED.prompt_version,model_name=EXCLUDED.model_name,review_status=EXCLUDED.review_status,is_current=true,last_published_at=now(),superseded_at=NULL""", publication_rows, page_size=500)
        conn.commit()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            summary_where = ["is_current"]
            summary_params: List[object] = []
            if scope_packages:
                summary_where.append("package_insert_no=ANY(%s)")
                summary_params.append(scope_packages)
            if args.population_type:
                summary_where.append("population_type=%s")
                summary_params.append(args.population_type)
            cur.execute(f"SELECT statement_id,package_insert_no,population_type,classification_code FROM {statement_table} WHERE {' AND '.join(summary_where)}", summary_params)
            grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
            for row in cur.fetchall():
                grouped[(row["package_insert_no"], row["population_type"])].append(dict(row))
        summaries = []
        for state in states:
            key = (state["package_insert_no"], state["population_type"])
            items = grouped.get(key, [])
            codes = [item["classification_code"] for item in items]
            assessment_code, display_level, assessment_text = assessment_for_codes(codes, state["has_section"])
            reason = None
            matching = [item for item in items if item["classification_code"] == assessment_code]
            if matching:
                reason = matching[0]["statement_id"]
            has_unclassified = "UNCLASSIFIABLE" in codes
            summaries.append((state["package_insert_no"], state["population_type"], state["prepared_ym"], state["generic_name_ja"], assessment_code, display_level, assessment_text, reason, has_unclassified, has_unclassified, len(items), state["extractor_version"]))
        with conn.cursor() as cur:
            if summaries:
                psycopg2.extras.execute_values(cur, f"""INSERT INTO {summary_table} (package_insert_no,population_type,prepared_ym,generic_name_ja,assessment_code,display_level,assessment_text,reason_statement_id,needs_review,has_unclassified,statement_count,definition_version) VALUES %s ON CONFLICT (package_insert_no,population_type) DO UPDATE SET prepared_ym=EXCLUDED.prepared_ym,generic_name_ja=EXCLUDED.generic_name_ja,assessment_code=EXCLUDED.assessment_code,display_level=EXCLUDED.display_level,assessment_text=EXCLUDED.assessment_text,reason_statement_id=EXCLUDED.reason_statement_id,needs_review=EXCLUDED.needs_review,has_unclassified=EXCLUDED.has_unclassified,statement_count=EXCLUDED.statement_count,definition_version=EXCLUDED.definition_version,updated_at=now()""", summaries, page_size=500)
        conn.commit()
        log.info("公開完了 statements=%s summaries=%s", len(publication_rows), len(summaries))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

