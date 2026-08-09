#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""候補ブロックをテーマ別にOllamaへ送り、検証済み一時ファクトを作る。"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    build_prompt,
    call_ollama,
    checked_table_name,
    json_from_model_text,
    load_config,
    load_definitions,
    select_definitions,
    sha256_text,
    table_base,
    validate_facts,
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
    parser = argparse.ArgumentParser(description="ノート候補をテーマ別LLMで抽出")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--definitions", help="note definition JSON")
    parser.add_argument("--package-insert-no", help="指定添付文書だけを処理")
    parser.add_argument("--note-type", help="指定テーマだけを処理")
    parser.add_argument("--limit", type=int, help="今回新規にLLMへ送る候補数の上限")
    parser.add_argument("--model", help="Ollamaモデルを一時的に上書き")
    parser.add_argument("--prompt-version", help="プロンプト版を一時的に上書き")
    parser.add_argument("--wait-seconds", type=float, help="LLM呼出し後の待機秒数")
    parser.add_argument("--max-retries", type=int, help="通信・JSON不正時の再試行回数")
    parser.add_argument("--force", action="store_true", help="成功済みキャッシュも再実行")
    return parser.parse_args()


def create_tables(conn, run_table: str, fact_table: str) -> None:
    run_base = table_base(run_table)
    fact_base = table_base(fact_table)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {run_table} (
      run_id                bigserial PRIMARY KEY,
      analysis_hash         text NOT NULL,
      content_hash          text NOT NULL,
      note_type             text NOT NULL,
      definition_version    text NOT NULL,
      prompt_version        text NOT NULL,
      model_name            text NOT NULL,
      server_url            text NOT NULL,
      status                text NOT NULL,
      request_at            timestamptz,
      response_at           timestamptz,
      attempts              integer NOT NULL DEFAULT 0,
      raw_response          text,
      response_json         jsonb,
      validation_errors     jsonb NOT NULL DEFAULT '[]'::jsonb,
      error_message         text,
      updated_at            timestamptz NOT NULL DEFAULT now(),
      UNIQUE (analysis_hash, model_name)
    );
    CREATE INDEX IF NOT EXISTS idx_{run_base}_status ON {run_table} (status);
    CREATE INDEX IF NOT EXISTS idx_{run_base}_type_version
      ON {run_table} (note_type, definition_version, prompt_version, model_name);

    CREATE TABLE IF NOT EXISTS {fact_table} (
      temp_fact_id          bigserial PRIMARY KEY,
      run_id                bigint NOT NULL,
      fact_hash             text NOT NULL,
      relation_type         text NOT NULL,
      subject_type          text NOT NULL,
      target_code           text,
      target_name           text,
      polarity              text NOT NULL,
      certainty             text NOT NULL,
      note_text             text NOT NULL,
      details_json          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
      evidence_text         text NOT NULL,
      validation_status     text NOT NULL,
      created_at            timestamptz NOT NULL DEFAULT now(),
      UNIQUE (run_id, fact_hash)
    );
    CREATE INDEX IF NOT EXISTS idx_{fact_base}_run ON {fact_table} (run_id);
    CREATE INDEX IF NOT EXISTS idx_{fact_base}_target ON {fact_table} (target_code, relation_type);
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
    run_table = checked_table_name(
        config.get("temp_sgml_note_run_table", "public.temp_sgml_note_run"),
        "temp_sgml_note_run_table",
    )
    fact_table = checked_table_name(
        config.get("temp_sgml_note_fact_table", "public.temp_sgml_note_fact"),
        "temp_sgml_note_fact_table",
    )
    model = args.model or config.get("note_ollama_model", config.get("pk_ollama_model", "gpt-oss:20b"))
    url = config.get("note_ollama_url", config.get("ollama_url", "http://localhost:11434/api/generate"))
    timeout = int(config.get("note_ollama_timeout", config.get("pk_ollama_timeout", 600)))
    prompt_version = args.prompt_version or config.get("note_prompt_version", "sgml-note-v4")
    wait_seconds = args.wait_seconds if args.wait_seconds is not None else float(config.get("note_llm_wait", 0))
    max_retries = args.max_retries if args.max_retries is not None else int(config.get("note_llm_max_retries", 2))
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit は1以上にしてください")
    if wait_seconds < 0 or max_retries < 0:
        raise ValueError("wait-seconds と max-retries は0以上にしてください")

    conn = psycopg2.connect(**config["db"])
    create_tables(conn, run_table, fact_table)
    where = ["c.is_current", "b.is_current"]
    params: List[object] = []
    if args.package_insert_no:
        where.append("c.package_insert_no=%s")
        params.append(args.package_insert_no)
    if args.note_type:
        where.append("c.note_type=%s")
        params.append(args.note_type)
    selected_pairs = {(item["note_type"], item["definition_version"]) for item in definitions}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT c.candidate_id, c.package_insert_no, c.content_hash,
                            c.note_type, c.definition_version,
                            b.block_text, b.section_code, b.section_type, b.heading_path,
                            b.generic_name_ja
                       FROM {candidate_table} c
                       JOIN {block_table} b ON b.block_id=c.block_id
                      WHERE {' AND '.join(where)}
                      ORDER BY c.candidate_id""",
                params,
            )
            candidates = [
                dict(row)
                for row in cur.fetchall()
                if (row["note_type"], row["definition_version"]) in selected_pairs
            ]

        called = succeeded = reviewed = errors = cache_hits = 0
        for candidate in candidates:
            definition = definitions_by_type[candidate["note_type"]]
            analysis_hash = sha256_text(
                f"{candidate['content_hash']}\n{candidate['note_type']}\n"
                f"{candidate['definition_version']}\n{prompt_version}"
            )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT run_id, status FROM {run_table} WHERE analysis_hash=%s AND model_name=%s",
                    (analysis_hash, model),
                )
                cached = cur.fetchone()
            if cached and cached["status"] == "success" and not args.force:
                cache_hits += 1
                continue
            if args.limit is not None and called >= args.limit:
                break

            prompt = build_prompt(definition, candidate)
            base_prompt = prompt
            request_at = datetime.now(timezone.utc)
            response_at: Optional[datetime] = None
            raw_response: Optional[str] = None
            parsed_response: Optional[dict] = None
            valid_facts: List[dict] = []
            validation_errors: List[str] = []
            error_message: Optional[str] = None
            status = "error"
            attempts = 0
            for attempt in range(1, max_retries + 2):
                attempts += 1
                log.info(
                    "LLM送信 package=%s type=%s hash=%s attempt=%s/%s",
                    candidate["package_insert_no"], candidate["note_type"], analysis_hash[:12],
                    attempt, max_retries + 1,
                )
                try:
                    raw_response, _outer = call_ollama(url, model, prompt, timeout)
                    parsed_response = json_from_model_text(raw_response)
                    valid_facts, validation_errors = validate_facts(
                        parsed_response, definition, candidate["block_text"]
                    )
                    response_at = datetime.now(timezone.utc)
                    if validation_errors:
                        status = "review"
                        error_message = "LLM応答に検証不合格のファクトがあります"
                        if attempt <= max_retries:
                            prompt = (
                                base_prompt
                                + "\n\n【前回応答の修正指示】\n"
                                + "前回応答には次の検証エラーがありました。"
                                + "不合格ファクトを修正又は削除し、JSON全体を返してください。\n"
                                + "\n".join(f"- {item}" for item in validation_errors)
                                + "\n特に evidence_text は入力本文から正確にコピーし、"
                                + "表の離れたセルを連結しないでください。\n"
                                + (raw_response or "")
                            )
                            continue
                        break
                    status = "success"
                    error_message = None
                    break
                except json.JSONDecodeError as exc:
                    response_at = datetime.now(timezone.utc)
                    status = "error"
                    error_message = f"{type(exc).__name__}: {exc}"
                    log.warning("LLM処理失敗 hash=%s: %s", analysis_hash[:12], error_message)
                    if attempt <= max_retries:
                        prompt = (
                            base_prompt
                            + "\n\n【前回応答のJSON構文修正指示】\n"
                            + f"前回応答はJSONとして解析できません: {exc}\n"
                            + "キーと文字列はダブルクォートで囲み、配列・オブジェクトの"
                            + "最後の要素にはカンマを付けないでください。内容を追加せず、"
                            + "同じ事実を有効なJSONだけで返してください。\n"
                            + "【前回応答】\n"
                            + (raw_response or "")
                        )
                        continue
                except Exception as exc:
                    response_at = datetime.now(timezone.utc)
                    status = "error"
                    error_message = f"{type(exc).__name__}: {exc}"
                    log.warning("LLM処理失敗 hash=%s: %s", analysis_hash[:12], error_message)
                finally:
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)

            response_json = {"facts": valid_facts} if parsed_response is not None else None
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {run_table}
                        (analysis_hash, content_hash, note_type, definition_version,
                         prompt_version, model_name, server_url, status,
                         request_at, response_at, attempts, raw_response,
                         response_json, validation_errors, error_message)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (analysis_hash, model_name) DO UPDATE SET
                        content_hash=EXCLUDED.content_hash,
                        note_type=EXCLUDED.note_type,
                        definition_version=EXCLUDED.definition_version,
                        prompt_version=EXCLUDED.prompt_version,
                        server_url=EXCLUDED.server_url,
                        status=EXCLUDED.status,
                        request_at=EXCLUDED.request_at,
                        response_at=EXCLUDED.response_at,
                        attempts={table_base(run_table)}.attempts + EXCLUDED.attempts,
                        raw_response=EXCLUDED.raw_response,
                        response_json=EXCLUDED.response_json,
                        validation_errors=EXCLUDED.validation_errors,
                        error_message=EXCLUDED.error_message,
                        updated_at=now()
                      RETURNING run_id""",
                    (
                        analysis_hash, candidate["content_hash"], candidate["note_type"],
                        candidate["definition_version"], prompt_version, model, url, status,
                        request_at, response_at, attempts, raw_response,
                        psycopg2.extras.Json(response_json) if response_json is not None else None,
                        psycopg2.extras.Json(validation_errors), error_message,
                    ),
                )
                run_id = cur.fetchone()[0]
                cur.execute(f"DELETE FROM {fact_table} WHERE run_id=%s", (run_id,))
                if valid_facts:
                    fact_rows = [
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
                        for fact in valid_facts
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        f"""INSERT INTO {fact_table}
                            (run_id, fact_hash, relation_type, subject_type,
                             target_code, target_name, polarity, certainty,
                             note_text, details_json, evidence_text, validation_status)
                          VALUES %s""",
                        fact_rows,
                        page_size=100,
                    )
            conn.commit()
            called += 1
            if status == "success":
                succeeded += 1
            elif status == "review":
                reviewed += 1
            else:
                errors += 1
        log.info(
            "完了 candidates=%s called=%s cache_hits=%s success=%s review=%s error=%s",
            len(candidates), called, cache_hits, succeeded, reviewed, errors,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
