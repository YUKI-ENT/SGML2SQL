#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""33の保存済みLLM応答を再送信せず、現在の規則で再検証する。"""

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
    config_with_global_fallback,
    json_from_model_text,
    load_config,
)
from _sgml_women_common import validate_llm_response


log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="保存済みsgml_women応答を無課金で再検証")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--population-type", choices=["PREGNANCY", "LACTATION"])
    parser.add_argument(
        "--model",
        action="append",
        default=[],
        help="対象モデル。複数回指定可。省略時はwomen_ollama_model、未指定ならollama_model",
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


def replace_fact(
    cur, fact_table: str, run_id: int, fact: Optional[dict]
) -> None:
    cur.execute(f"DELETE FROM {fact_table} WHERE run_id=%s", (run_id,))
    if fact is None:
        return
    cur.execute(
        f"""INSERT INTO {fact_table}
              (run_id,classification_code,recommendation_target,
               assessment_text,evidence_text,validation_status)
            VALUES (%s,%s,%s,%s,%s,'AUTO_VALIDATED')""",
        (
            run_id,
            fact["classification_code"],
            fact["recommendation_target"],
            fact["assessment_text"],
            fact["evidence_text"],
        ),
    )


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limitは1以上にしてください")
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    models = args.model or [
        config_with_global_fallback(
            config, "women_ollama_model", "ollama_model", "gpt-oss:20b"
        )
    ]
    prompt_version = args.prompt_version or config.get(
        "women_prompt_version", "sgml-women-v1"
    )
    candidate_table = checked_table_name(
        config.get(
            "temp_sgml_women_candidate_table",
            "public.temp_sgml_women_candidate",
        ),
        "temp_sgml_women_candidate_table",
    )
    run_table = checked_table_name(
        config.get("temp_sgml_women_run_table", "public.temp_sgml_women_run"),
        "temp_sgml_women_run_table",
    )
    fact_table = checked_table_name(
        config.get("temp_sgml_women_fact_table", "public.temp_sgml_women_fact"),
        "temp_sgml_women_fact_table",
    )
    where = [
        "r.model_name=ANY(%s)",
        "r.prompt_version=%s",
        "r.status=ANY(%s)",
    ]
    params: List[object] = [models, prompt_version, args.status]
    if args.population_type:
        where.append("r.population_type=%s")
        params.append(args.population_type)
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
                f"""SELECT r.run_id,r.model_name,r.status AS old_status,
                           r.raw_response,c.evidence_text,c.population_type
                      FROM {run_table} r
                      JOIN LATERAL (
                           SELECT candidate.evidence_text,
                                  candidate.population_type,
                                  candidate.package_insert_no
                             FROM {candidate_table} candidate
                            WHERE candidate.is_current
                              AND candidate.requires_llm
                              AND candidate.statement_hash=r.statement_hash
                              AND candidate.population_type=r.population_type
                              AND candidate.definition_version=r.definition_version
                            ORDER BY candidate.candidate_id
                            LIMIT 1
                      ) c ON true
                     WHERE {' AND '.join(where)}
                     ORDER BY r.run_id
                     {limit_sql}""",
                params,
            )
            runs = [dict(row) for row in cur.fetchall()]

        for run in runs:
            selected += 1
            parsed: Optional[dict] = None
            valid: Optional[dict] = None
            validation_errors: List[str] = []
            error_message: Optional[str] = None
            try:
                if not run["raw_response"]:
                    raise ValueError("raw_responseがありません")
                parsed = json_from_model_text(run["raw_response"])
                valid, validation_errors = validate_llm_response(parsed, run)
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
            response_json = valid if valid is not None else parsed
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
                replace_fact(cur, fact_table, run["run_id"], valid)
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
            log.info(
                "再検証反映完了 selected=%s updated=%s LLM_calls=0",
                selected,
                updated,
            )
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    main()
