#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""現在の定義・モデルで検証成功したファクトだけを sgml_note へ公開する。"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Dict, List, Tuple

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    load_config,
    load_definitions,
    select_definitions,
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
    parser = argparse.ArgumentParser(description="検証済みノートをsgml_noteへ差分公開")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--definitions", help="note definition JSON")
    parser.add_argument("--package-insert-no", help="指定添付文書だけを公開")
    parser.add_argument("--note-type", help="指定テーマだけを公開")
    parser.add_argument("--model", help="公開対象Ollamaモデルを一時的に上書き")
    parser.add_argument(
        "--fallback-model",
        action="append",
        default=[],
        help="主モデルにsuccessがない候補を補完するモデル。複数回指定可",
    )
    parser.add_argument("--prompt-version", help="公開対象プロンプト版を一時的に上書き")
    parser.add_argument("--dry-run", action="store_true", help="公開件数だけ表示して更新しない")
    parser.add_argument(
        "--publish-partial",
        action="store_true",
        help="未処理候補が残っていても、成功済みファクトだけを公開する",
    )
    return parser.parse_args()


def create_table(conn, note_table: str) -> None:
    note_base = table_base(note_table)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {note_table} (
      note_id              bigserial PRIMARY KEY,
      package_insert_no    text NOT NULL,
      prepared_ym          text,
      generic_name_ja      text,
      note_type            text NOT NULL,
      relation_type        text NOT NULL,
      subject_type         text NOT NULL,
      target_code          text,
      target_name          text,
      polarity             text NOT NULL,
      certainty            text NOT NULL,
      note_text            text NOT NULL,
      details_json         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
      evidence_text        text NOT NULL,
      source_block_id      bigint NOT NULL,
      section_code         text,
      section_type         text,
      heading_path         text,
      source_hash          text NOT NULL,
      definition_version   text NOT NULL,
      prompt_version       text NOT NULL,
      model_name           text NOT NULL,
      fact_hash            text NOT NULL,
      review_status        text NOT NULL,
      is_current           boolean NOT NULL DEFAULT true,
      first_published_at   timestamptz NOT NULL DEFAULT now(),
      last_published_at    timestamptz NOT NULL DEFAULT now(),
      superseded_at        timestamptz,
      UNIQUE (package_insert_no, note_type, fact_hash)
    );
    CREATE INDEX IF NOT EXISTS idx_{note_base}_package ON {note_table} (package_insert_no);
    CREATE INDEX IF NOT EXISTS idx_{note_base}_current_type ON {note_table} (is_current, note_type);
    CREATE INDEX IF NOT EXISTS idx_{note_base}_target ON {note_table} (target_code, relation_type);
    CREATE INDEX IF NOT EXISTS idx_{note_base}_details_gin ON {note_table} USING gin (details_json);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    definitions_path = args.definitions or config.get("sgml_note_definitions", "sgml_note_definitions.json")
    definitions = select_definitions(load_definitions(definitions_path), args.note_type)
    definitions_by_type = {item["note_type"]: item for item in definitions}
    block_table = checked_table_name(config.get("sgml_note_block_table", "public.sgml_note_block"), "sgml_note_block_table")
    candidate_table = checked_table_name(
        config.get("temp_sgml_note_candidate_table", "public.temp_sgml_note_candidate"),
        "temp_sgml_note_candidate_table",
    )
    run_table = checked_table_name(config.get("temp_sgml_note_run_table", "public.temp_sgml_note_run"), "temp_sgml_note_run_table")
    fact_table = checked_table_name(config.get("temp_sgml_note_fact_table", "public.temp_sgml_note_fact"), "temp_sgml_note_fact_table")
    note_table = checked_table_name(config.get("sgml_note_table", "public.sgml_note"), "sgml_note_table")
    model = args.model or config.get("note_ollama_model", config.get("pk_ollama_model", "gpt-oss:20b"))
    models = list(dict.fromkeys([model, *args.fallback_model]))
    prompt_version = args.prompt_version or config.get("note_prompt_version", "sgml-note-v4")

    conn = psycopg2.connect(**config["db"])
    create_table(conn, note_table)
    definition_versions = {(item["note_type"], item["definition_version"]) for item in definitions}
    note_types = [item["note_type"] for item in definitions]
    where = [
        "c.is_current",
        "b.is_current",
        "f.validation_status='AUTO_VALIDATED'",
        "c.note_type = ANY(%s)",
    ]
    params: List[object] = [prompt_version, models, models, note_types]
    if args.package_insert_no:
        where.append("c.package_insert_no=%s")
        params.append(args.package_insert_no)
    query = f"""
      SELECT c.package_insert_no, b.prepared_ym, b.generic_name_ja,
             c.note_type, c.definition_version,
             f.relation_type, f.subject_type, f.target_code, f.target_name,
             f.polarity, f.certainty, f.note_text, f.details_json,
             f.evidence_text, b.block_id, b.section_code, b.section_type,
             b.heading_path, b.content_hash, r.prompt_version, r.model_name,
             f.fact_hash
        FROM {candidate_table} c
        JOIN {block_table} b ON b.block_id=c.block_id
        JOIN LATERAL (
          SELECT selected_run.*
            FROM {run_table} selected_run
           WHERE selected_run.content_hash=c.content_hash
             AND selected_run.note_type=c.note_type
             AND selected_run.definition_version=c.definition_version
             AND selected_run.prompt_version=%s
             AND selected_run.model_name=ANY(%s)
             AND selected_run.status='success'
           ORDER BY array_position(%s::text[], selected_run.model_name)
           LIMIT 1
        ) r ON true
        JOIN {fact_table} f ON f.run_id=r.run_id
       WHERE {' AND '.join(where)}
       ORDER BY c.package_insert_no, c.note_type, f.fact_hash, b.block_id
    """
    try:
        coverage_where = ["c.is_current", "b.is_current", "c.note_type = ANY(%s)"]
        coverage_params: List[object] = [prompt_version, models, note_types]
        if args.package_insert_no:
            coverage_where.append("c.package_insert_no=%s")
            coverage_params.append(args.package_insert_no)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT c.package_insert_no, c.note_type, c.definition_version,
                            count(*) AS candidates,
                            count(*) FILTER (WHERE EXISTS (
                              SELECT 1
                                FROM {run_table} successful_run
                               WHERE successful_run.content_hash=c.content_hash
                                 AND successful_run.note_type=c.note_type
                                 AND successful_run.definition_version=c.definition_version
                                 AND successful_run.prompt_version=%s
                                 AND successful_run.model_name=ANY(%s)
                                 AND successful_run.status='success'
                            )) AS succeeded
                       FROM {candidate_table} c
                       JOIN {block_table} b ON b.block_id=c.block_id
                      WHERE {' AND '.join(coverage_where)}
                      GROUP BY c.package_insert_no, c.note_type, c.definition_version""",
                coverage_params,
            )
            coverage = [
                dict(row)
                for row in cur.fetchall()
                if (row["note_type"], row["definition_version"]) in definition_versions
            ]
        incomplete = [row for row in coverage if row["succeeded"] < row["candidates"]]
        if incomplete:
            missing = sum(row["candidates"] - row["succeeded"] for row in incomplete)
            log.warning("未処理または要確認の候補が残っています groups=%s missing=%s", len(incomplete), missing)
            if not args.dry_run and not args.publish_partial:
                raise RuntimeError(
                    "未処理候補があるため公開を中止しました。43を完了するか、"
                    "意図的な部分公開では --publish-partial を指定してください。"
                )

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            raw_rows = cur.fetchall()
        rows: Dict[Tuple[object, ...], dict] = {}

        def publication_quality(row: dict, definition: dict) -> Tuple[int, int, int, int]:
            model_score = len(models) - models.index(row["model_name"])
            priorities = definition.get("section_priority", [])
            try:
                section_score = len(priorities) - priorities.index(row["section_code"])
            except ValueError:
                section_score = 0
            evidence = row["evidence_text"] or ""
            sentence_score = sum(marker in evidence for marker in ("。", "、", "ことがある", "認められ"))
            return model_score, section_score, sentence_score, len(evidence)

        for row in raw_rows:
            if (row["note_type"], row["definition_version"]) not in definition_versions:
                continue
            definition = definitions_by_type[row["note_type"]]
            if definition.get("consolidate_by_relation"):
                key = (
                    row["package_insert_no"], row["note_type"], row["relation_type"],
                    row["target_code"], row["polarity"],
                )
            else:
                key = (row["package_insert_no"], row["note_type"], row["fact_hash"])
            candidate = dict(row)
            current = rows.get(key)
            if current is None or publication_quality(candidate, definition) > publication_quality(current, definition):
                rows[key] = candidate
        log.info("公開候補 notes=%s package=%s", len(rows), args.package_insert_no or "ALL")
        if args.dry_run:
            return

        deactivate_where = ["review_status='AUTO_VALIDATED'", "is_current", "note_type = ANY(%s)"]
        deactivate_params: List[object] = [note_types]
        if args.package_insert_no:
            deactivate_where.append("package_insert_no=%s")
            deactivate_params.append(args.package_insert_no)
        with conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {note_table}
                       SET is_current=false, superseded_at=now()
                     WHERE {' AND '.join(deactivate_where)}""",
                deactivate_params,
            )
            if rows:
                values = [
                    (
                        row["package_insert_no"], row["prepared_ym"], row["generic_name_ja"],
                        row["note_type"], row["relation_type"], row["subject_type"],
                        row["target_code"], row["target_name"], row["polarity"], row["certainty"],
                        row["note_text"], psycopg2.extras.Json(row["details_json"] or {}),
                        row["evidence_text"], row["block_id"], row["section_code"],
                        row["section_type"], row["heading_path"], row["content_hash"],
                        row["definition_version"], row["prompt_version"], row["model_name"],
                        row["fact_hash"], "AUTO_VALIDATED", True, None,
                    )
                    for row in rows.values()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {note_table}
                        (package_insert_no, prepared_ym, generic_name_ja,
                         note_type, relation_type, subject_type, target_code,
                         target_name, polarity, certainty, note_text, details_json,
                         evidence_text, source_block_id, section_code, section_type,
                         heading_path, source_hash, definition_version, prompt_version,
                         model_name, fact_hash, review_status, is_current, superseded_at)
                      VALUES %s
                      ON CONFLICT (package_insert_no, note_type, fact_hash) DO UPDATE SET
                        prepared_ym=EXCLUDED.prepared_ym,
                        generic_name_ja=EXCLUDED.generic_name_ja,
                        relation_type=EXCLUDED.relation_type,
                        subject_type=EXCLUDED.subject_type,
                        target_code=EXCLUDED.target_code,
                        target_name=EXCLUDED.target_name,
                        polarity=EXCLUDED.polarity,
                        certainty=EXCLUDED.certainty,
                        note_text=EXCLUDED.note_text,
                        details_json=EXCLUDED.details_json,
                        evidence_text=EXCLUDED.evidence_text,
                        source_block_id=EXCLUDED.source_block_id,
                        section_code=EXCLUDED.section_code,
                        section_type=EXCLUDED.section_type,
                        heading_path=EXCLUDED.heading_path,
                        source_hash=EXCLUDED.source_hash,
                        definition_version=EXCLUDED.definition_version,
                        prompt_version=EXCLUDED.prompt_version,
                        model_name=EXCLUDED.model_name,
                        review_status='AUTO_VALIDATED',
                        is_current=true,
                        last_published_at=now(),
                        superseded_at=NULL
                      WHERE {table_base(note_table)}.review_status <> 'HUMAN_REVIEWED'""",
                    values,
                    page_size=500,
                )
        conn.commit()
        log.info("公開完了 notes=%s", len(rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
