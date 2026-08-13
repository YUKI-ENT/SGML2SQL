#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""43の保存済みLLM応答を再送信せず、現在の規則で再検証する。"""

from __future__ import annotations

import argparse
import logging
import os
from collections import Counter
from typing import List, Optional

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    json_from_model_text,
    load_config,
    load_definitions,
    select_definitions,
    table_base,
    validate_facts,
)


log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="保存済みsgml_note応答を無課金で再検証")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--definitions", help="note definition JSON")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--note-type", help="指定テーマだけを再検証")
    parser.add_argument(
        "--model",
        action="append",
        default=[],
        help="対象モデル。複数回指定可。省略時はconfigのnote_ollama_model",
    )
    parser.add_argument(
        "--status",
        nargs="+",
        choices=["success", "review", "error"],
        default=["review", "error"],
        help="再検証する現在status",
    )
    parser.add_argument("--prompt-version")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="結果をDBへ反映。省略時は判定件数だけを表示",
    )
    return parser.parse_args()


def replace_facts(cur, fact_table: str, run_id: int, facts: List[dict]) -> None:
    cur.execute(f"DELETE FROM {fact_table} WHERE run_id=%s", (run_id,))
    if not facts:
        return
    rows = [
        (
            run_id,
            fact["fact_hash"],
            fact["relation_type"],
            fact["subject_type"],
            fact.get("target_code"),
            fact.get("target_name"),
            fact["polarity"],
            fact["certainty"],
            fact["note_text"],
            psycopg2.extras.Json(fact["details"]),
            fact["evidence_text"],
            "AUTO_VALIDATED",
        )
        for fact in facts
    ]
    psycopg2.extras.execute_values(
        cur,
        f"""INSERT INTO {fact_table}
            (run_id, fact_hash, relation_type, subject_type,
             target_code, target_name, polarity, certainty,
             note_text, details_json, evidence_text, validation_status)
          VALUES %s
          ON CONFLICT (run_id, fact_hash) DO NOTHING""",
        rows,
        page_size=100,
    )


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limitは1以上にしてください")
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    definitions_path = args.definitions or config.get(
        "sgml_note_definitions", "sgml_note_definitions.json"
    )
    definitions = select_definitions(load_definitions(definitions_path), args.note_type)
    definitions_by_key = {
        (definition["note_type"], definition["definition_version"]): definition
        for definition in definitions
    }
    models = args.model or [
        config.get("note_ollama_model", config.get("pk_ollama_model", "gpt-oss:20b"))
    ]
    prompt_version = args.prompt_version or config.get("note_prompt_version", "sgml-note-v4")
    block_table = checked_table_name(
        config.get("sgml_note_block_table", "public.sgml_note_block"),
        "sgml_note_block_table",
    )
    candidate_table = checked_table_name(
        config.get("temp_sgml_note_candidate_table", "public.temp_sgml_note_candidate"),
        "temp_sgml_note_candidate_table",
    )
    run_table = checked_table_name(
        config.get("temp_sgml_note_run_table", "public.temp_sgml_note_run"),
        "temp_sgml_note_run_table",
    )
    fact_table = checked_table_name(
        config.get("temp_sgml_note_fact_table", "public.temp_sgml_note_fact"),
        "temp_sgml_note_fact_table",
    )
    where = [
        "c.is_current",
        "b.is_current",
        "r.model_name=ANY(%s)",
        "r.prompt_version=%s",
        "r.status=ANY(%s)",
    ]
    params: List[object] = [models, prompt_version, args.status]
    if args.note_type:
        where.append("r.note_type=%s")
        params.append(args.note_type)
    if args.package_insert_no:
        where.append("c.package_insert_no=%s")
        params.append(args.package_insert_no)
    limit_sql = ""
    if args.limit is not None:
        limit_sql = " LIMIT %s"
        params.append(args.limit)

    conn = psycopg2.connect(**config["db"])
    outcomes: Counter[tuple[str, str, str]] = Counter()
    selected = updated = 0
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT DISTINCT ON (r.run_id)
                            r.run_id, r.model_name, r.status AS old_status,
                            r.note_type, r.definition_version, r.raw_response,
                            b.block_text
                       FROM {run_table} r
                       JOIN {candidate_table} c
                         ON c.content_hash=r.content_hash
                        AND c.note_type=r.note_type
                        AND c.definition_version=r.definition_version
                       JOIN {block_table} b ON b.block_id=c.block_id
                      WHERE {' AND '.join(where)}
                      ORDER BY r.run_id, c.candidate_id
                      {limit_sql}""",
                params,
            )
            runs = [dict(row) for row in cur.fetchall()]

        for run in runs:
            definition = definitions_by_key.get(
                (run["note_type"], run["definition_version"])
            )
            if definition is None:
                continue
            selected += 1
            parsed: Optional[dict] = None
            facts: List[dict] = []
            validation_errors: List[str] = []
            error_message: Optional[str] = None
            try:
                if not run["raw_response"]:
                    raise ValueError("raw_responseがありません")
                parsed = json_from_model_text(run["raw_response"])
                facts, validation_errors = validate_facts(
                    parsed, definition, run["block_text"]
                )
                if validation_errors:
                    new_status = "review"
                    error_message = "保存済みLLM応答が現在の検証規則に不合格です"
                else:
                    new_status = "success"
            except Exception as exc:
                new_status = "error"
                error_message = f"{type(exc).__name__}: {exc}"
                validation_errors = []
            outcomes[(run["model_name"], run["old_status"], new_status)] += 1
            if not args.execute:
                continue
            response_json = {"facts": facts} if parsed is not None else None
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {run_table}
                           SET status=%s,
                               response_json=%s,
                               validation_errors=%s,
                               error_message=%s,
                               updated_at=now()
                         WHERE run_id=%s""",
                    (
                        new_status,
                        psycopg2.extras.Json(response_json)
                        if response_json is not None
                        else None,
                        psycopg2.extras.Json(validation_errors),
                        error_message,
                        run["run_id"],
                    ),
                )
                replace_facts(cur, fact_table, run["run_id"], facts)
            conn.commit()
            updated += 1
        for (model, old_status, new_status), count in sorted(outcomes.items()):
            log.info(
                "再検証 model=%s %s->%s count=%s",
                model,
                old_status,
                new_status,
                count,
            )
        if args.execute:
            log.info("再検証反映完了 selected=%s updated=%s LLM_calls=0", selected, updated)
        else:
            log.warning(
                "dry-run selected=%s LLM_calls=0。反映する場合は--executeを指定してください",
                selected,
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()

