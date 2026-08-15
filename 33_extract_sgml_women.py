#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""ルール分類不能の妊婦・授乳表現だけをLLMで分類する。"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import List, Optional

import psycopg2
import psycopg2.extras

from _sgml_note_common import call_ollama, checked_table_name, config_with_global_fallback, json_from_model_text, load_config, sha256_text, table_base
from _sgml_women_common import CLASSIFICATION_META, PIPELINE_VERSION, validate_llm_response


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler(f"logs/{SCRIPT_BASENAME}.log", encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="分類不能な妊婦・授乳表現をLLM分類")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--population-type", choices=["PREGNANCY", "LACTATION"])
    parser.add_argument("--model")
    parser.add_argument("--prompt-version")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--wait-seconds", type=float)
    parser.add_argument(
        "--run-status",
        choices=["incomplete", "new", "review", "error"],
        default="incomplete",
        help=(
            "処理対象。incomplete=success以外、new=対象モデルで未処理のみ、"
            "review=要確認のみ、error=エラーのみ"
        ),
    )
    parser.add_argument(
        "--source-model",
        help="このモデルの実行結果statusを使って処理候補を絞る",
    )
    parser.add_argument(
        "--source-status",
        nargs="+",
        choices=["success", "review", "error"],
        help="--source-modelで選ぶstatus（省略時はreview error）",
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def should_process(cached_status: Optional[str], run_status: str, force: bool = False) -> bool:
    if force:
        return True
    if run_status == "new":
        return cached_status is None
    if run_status == "review":
        return cached_status == "review"
    if run_status == "error":
        return cached_status == "error"
    return cached_status != "success"


def build_prompt(candidate: dict) -> str:
    population = "妊婦・妊娠可能な女性" if candidate["population_type"] == "PREGNANCY" else "授乳婦・授乳"
    allowed = "|".join(CLASSIFICATION_META)
    return f"""あなたは日本の医療用医薬品添付文書から、妊婦・授乳に関する原文表現を分類する抽出器です。
入力された一文だけを根拠に分類してください。一般知識で補完しないでください。
既存分類に確実に該当しなければUNCLASSIFIABLEとしてください。
evidence_textは入力文を変更せず、そのまま返してください。

対象: {population}
許可分類: {allowed}
recommendation_targetはDRUG、BREASTFEEDING、又は引用符なしのJSON nullとしてください。

JSONだけを返してください。
{{
  "classification_code": "許可分類のいずれか",
  "recommendation_target": null,
  "assessment_text": "原文に忠実な短い要約",
  "evidence_text": "入力文の完全な引用"
}}

【入力文】
{candidate['evidence_text']}
"""


def create_tables(conn, run_table: str, fact_table: str) -> None:
    run_base, fact_base = table_base(run_table), table_base(fact_table)
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {run_table} (
          run_id bigserial PRIMARY KEY,
          candidate_id bigint NOT NULL,
          analysis_hash text NOT NULL,
          statement_hash text NOT NULL,
          population_type text NOT NULL,
          definition_version text NOT NULL,
          prompt_version text NOT NULL,
          model_name text NOT NULL,
          server_url text NOT NULL,
          status text NOT NULL,
          attempts integer NOT NULL DEFAULT 0,
          raw_response text,
          response_json jsonb,
          validation_errors jsonb NOT NULL DEFAULT '[]'::jsonb,
          error_message text,
          request_at timestamptz,
          response_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now(),
          UNIQUE (analysis_hash, model_name)
        );
        CREATE INDEX IF NOT EXISTS idx_{run_base}_status ON {run_table} (status, population_type);
        CREATE TABLE IF NOT EXISTS {fact_table} (
          temp_fact_id bigserial PRIMARY KEY,
          run_id bigint NOT NULL UNIQUE,
          classification_code text NOT NULL,
          recommendation_target text,
          assessment_text text NOT NULL,
          evidence_text text NOT NULL,
          validation_status text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_{fact_base}_classification ON {fact_table} (classification_code);
        """)
    conn.commit()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    candidate_table = checked_table_name(config.get("temp_sgml_women_candidate_table", "public.temp_sgml_women_candidate"), "temp_sgml_women_candidate_table")
    run_table = checked_table_name(config.get("temp_sgml_women_run_table", "public.temp_sgml_women_run"), "temp_sgml_women_run_table")
    fact_table = checked_table_name(config.get("temp_sgml_women_fact_table", "public.temp_sgml_women_fact"), "temp_sgml_women_fact_table")
    model = args.model or config_with_global_fallback(
        config, "women_ollama_model", "ollama_model", "gpt-oss:20b"
    )
    url = config_with_global_fallback(
        config,
        "women_ollama_url",
        "ollama_url",
        "http://localhost:11434/api/generate",
    )
    timeout = int(
        config_with_global_fallback(config, "women_ollama_timeout", "ollama_timeout", 600)
    )
    prompt_version = args.prompt_version or config.get("women_prompt_version", "sgml-women-v1")
    max_retries = args.max_retries if args.max_retries is not None else int(config.get("women_llm_max_retries", 2))
    wait_seconds = (
        args.wait_seconds
        if args.wait_seconds is not None
        else float(
            config_with_global_fallback(
                config, "women_llm_wait", "gpu_cooling_wait", 0
            )
        )
    )
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limitは1以上にしてください")
    if wait_seconds < 0 or max_retries < 0:
        raise ValueError("wait-secondsとmax-retriesは0以上にしてください")
    if args.force and args.run_status != "incomplete":
        raise ValueError("--forceと--run-status new/review/errorは同時に指定できません")
    if args.source_status and not args.source_model:
        raise ValueError("--source-statusを使う場合は--source-modelも指定してください")
    if args.source_model == model:
        raise ValueError("--source-modelは処理対象--modelと異なるモデルを指定してください")
    conn = psycopg2.connect(**config["db"])
    create_tables(conn, run_table, fact_table)
    where, params = ["is_current", "requires_llm"], []
    if args.package_insert_no:
        where.append("package_insert_no=%s")
        params.append(args.package_insert_no)
    if args.population_type:
        where.append("population_type=%s")
        params.append(args.population_type)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {candidate_table} WHERE {' AND '.join(where)} ORDER BY candidate_id", params)
            candidates = [dict(row) for row in cur.fetchall()]
        for candidate in candidates:
            candidate["analysis_hash"] = sha256_text(
                f"{candidate['statement_hash']}\n{PIPELINE_VERSION}\n{prompt_version}"
            )
        analysis_hashes = sorted({candidate["analysis_hash"] for candidate in candidates})
        if args.source_model:
            selected_source_statuses = set(args.source_status or ["review", "error"])
            source_statuses: dict[str, str] = {}
            if analysis_hashes:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT analysis_hash,status FROM {run_table} "
                        "WHERE model_name=%s AND analysis_hash=ANY(%s)",
                        (args.source_model, analysis_hashes),
                    )
                    source_statuses = {
                        analysis_hash: status for analysis_hash, status in cur.fetchall()
                    }
            candidates = [
                candidate
                for candidate in candidates
                if source_statuses.get(candidate["analysis_hash"])
                in selected_source_statuses
            ]
            analysis_hashes = sorted({candidate["analysis_hash"] for candidate in candidates})
            log.info(
                "元モデル絞込 source_model=%s source_status=%s analyses=%s candidates=%s",
                args.source_model,
                ",".join(sorted(selected_source_statuses)),
                len(analysis_hashes),
                len(candidates),
            )
        cached_statuses: dict[str, str] = {}
        if analysis_hashes:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT analysis_hash,status FROM {run_table} "
                    "WHERE model_name=%s AND analysis_hash=ANY(%s)",
                    (model, analysis_hashes),
                )
                cached_statuses = {
                    analysis_hash: status for analysis_hash, status in cur.fetchall()
                }
        status_counts = Counter(
            cached_statuses.get(analysis_hash, "new") for analysis_hash in analysis_hashes
        )
        eligible_count = sum(
            should_process(cached_statuses.get(analysis_hash), args.run_status, args.force)
            for analysis_hash in analysis_hashes
        )
        log.info(
            "開始 model=%s url=%s timeout=%ss wait=%ss prompt=%s mode=%s "
            "analyses=%s eligible=%s new=%s success=%s review=%s error=%s",
            model,
            url,
            timeout,
            wait_seconds,
            prompt_version,
            args.run_status,
            len(analysis_hashes),
            eligible_count,
            status_counts.get("new", 0),
            status_counts.get("success", 0),
            status_counts.get("review", 0),
            status_counts.get("error", 0),
        )
        called = cache_hits = success = review = error = 0
        for candidate in candidates:
            analysis_hash = candidate["analysis_hash"]
            cached_status = cached_statuses.get(analysis_hash)
            if not should_process(cached_status, args.run_status, args.force):
                cache_hits += 1
                continue
            if args.limit is not None and called >= args.limit:
                break
            base_prompt = build_prompt(candidate)
            prompt = base_prompt
            raw_response = None
            parsed = None
            valid = None
            validation_errors: List[str] = []
            error_message = None
            status = "error"
            request_at = datetime.now(timezone.utc)
            response_at = None
            attempts = 0
            for attempt in range(1, max_retries + 2):
                attempts += 1
                log.info("LLM送信 package=%s population=%s hash=%s attempt=%s/%s", candidate["package_insert_no"], candidate["population_type"], analysis_hash[:12], attempt, max_retries + 1)
                try:
                    raw_response, _outer = call_ollama(url, model, prompt, timeout)
                    parsed = json_from_model_text(raw_response)
                    valid, validation_errors = validate_llm_response(parsed, candidate)
                    response_at = datetime.now(timezone.utc)
                    if validation_errors:
                        status, error_message = "review", "LLM応答が検証不合格です"
                        if attempt <= max_retries:
                            prompt = base_prompt + "\n\n【前回応答の修正指示】\n" + "\n".join(f"- {item}" for item in validation_errors) + "\n分類できなければUNCLASSIFIABLEにしてください。\n" + (raw_response or "")
                            continue
                        break
                    status, error_message = "success", None
                    break
                except json.JSONDecodeError as exc:
                    response_at = datetime.now(timezone.utc)
                    error_message = f"{type(exc).__name__}: {exc}"
                    if attempt <= max_retries:
                        prompt = base_prompt + "\n\n前回応答を末尾カンマのない有効なJSONに修正してください。\n" + (raw_response or "")
                        continue
                except Exception as exc:
                    response_at = datetime.now(timezone.utc)
                    error_message = f"{type(exc).__name__}: {exc}"
                    log.warning("LLM処理失敗 hash=%s: %s", analysis_hash[:12], error_message)
                finally:
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)
            with conn.cursor() as cur:
                cur.execute(f"""INSERT INTO {run_table} (candidate_id,analysis_hash,statement_hash,population_type,definition_version,prompt_version,model_name,server_url,status,attempts,raw_response,response_json,validation_errors,error_message,request_at,response_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (analysis_hash,model_name) DO UPDATE SET candidate_id=EXCLUDED.candidate_id,status=EXCLUDED.status,attempts={table_base(run_table)}.attempts+EXCLUDED.attempts,raw_response=EXCLUDED.raw_response,response_json=EXCLUDED.response_json,validation_errors=EXCLUDED.validation_errors,error_message=EXCLUDED.error_message,request_at=EXCLUDED.request_at,response_at=EXCLUDED.response_at,updated_at=now() RETURNING run_id""", (candidate["candidate_id"], analysis_hash, candidate["statement_hash"], candidate["population_type"], PIPELINE_VERSION, prompt_version, model, url, status, attempts, raw_response, psycopg2.extras.Json(parsed) if parsed is not None else None, psycopg2.extras.Json(validation_errors), error_message, request_at, response_at))
                run_id = cur.fetchone()[0]
                cur.execute(f"DELETE FROM {fact_table} WHERE run_id=%s", (run_id,))
                if valid is not None:
                    cur.execute(f"INSERT INTO {fact_table} (run_id,classification_code,recommendation_target,assessment_text,evidence_text,validation_status) VALUES (%s,%s,%s,%s,%s,'AUTO_VALIDATED')", (run_id, valid["classification_code"], valid["recommendation_target"], valid["assessment_text"], valid["evidence_text"]))
            conn.commit()
            cached_statuses[analysis_hash] = status
            called += 1
            if status == "success": success += 1
            elif status == "review": review += 1
            else: error += 1
        log.info("完了 candidates=%s called=%s cache_hits=%s success=%s review=%s error=%s", len(candidates), called, cache_hits, success, review, error)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

