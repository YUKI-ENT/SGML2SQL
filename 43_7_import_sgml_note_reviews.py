#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""編集済み手動review CSVを再検証し、human-review結果として登録する。"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    load_config,
    load_definitions,
    sha256_text,
    table_base,
    validate_facts,
)


log = logging.getLogger(__name__)
HUMAN_MODEL = "human-review"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="手動review CSVを検証してDBへ登録")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--definitions", help="note definition JSON")
    parser.add_argument("--cases", required=True)
    parser.add_argument("--facts", required=True)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def read_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return [dict(row) for row in csv.DictReader(stream)]


def included(value: str) -> bool:
    return value.strip().casefold() in {"true", "1", "yes", "y", "採用"}


def combined_file_hash(cases_path: Path, facts_path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(cases_path.read_bytes())
    digest.update(b"\0")
    digest.update(facts_path.read_bytes())
    return digest.hexdigest()


def create_audit_table(conn, audit_table: str) -> None:
    base = table_base(audit_table)
    with conn.cursor() as cur:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {audit_table} (
                  manual_review_id bigserial PRIMARY KEY,
                  analysis_hash text NOT NULL,
                  note_type text NOT NULL,
                  decision text NOT NULL,
                  reviewer text NOT NULL,
                  review_comment text,
                  source_model text,
                  source_run_id bigint,
                  manual_run_id bigint NOT NULL,
                  facts_json jsonb NOT NULL,
                  import_file_hash text NOT NULL,
                  imported_at timestamptz NOT NULL DEFAULT now(),
                  UNIQUE (analysis_hash, import_file_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_{base}_analysis
                  ON {audit_table} (analysis_hash, imported_at);"""
        )
    conn.commit()


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
            "HUMAN_REVIEWED",
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
    cases_path, facts_path = Path(args.cases), Path(args.facts)
    case_rows, fact_rows = read_csv(cases_path), read_csv(facts_path)
    if not case_rows:
        raise ValueError("cases CSVにデータがありません")
    required_case_fields = {
        "analysis_hash", "note_type", "definition_version", "prompt_version",
        "content_hash", "source_run_id", "source_model", "decision",
        "reviewer", "review_comment",
    }
    required_fact_fields = {
        "analysis_hash", "include", "relation_type", "subject_type", "target_code",
        "target_name", "polarity", "certainty", "note_text", "details_json",
        "evidence_text",
    }
    if not required_case_fields.issubset(case_rows[0]):
        raise ValueError("cases CSVの必須列が不足しています")
    if fact_rows and not required_fact_fields.issubset(fact_rows[0]):
        raise ValueError("facts CSVの必須列が不足しています")
    cases_by_hash: Dict[str, dict] = {}
    for row in case_rows:
        analysis_hash = row["analysis_hash"].strip()
        if not analysis_hash or analysis_hash in cases_by_hash:
            raise ValueError(f"cases CSVのanalysis_hashが空又は重複しています: {analysis_hash}")
        cases_by_hash[analysis_hash] = row
    facts_by_hash: Dict[str, List[dict]] = defaultdict(list)
    for row in fact_rows:
        facts_by_hash[row["analysis_hash"].strip()].append(row)

    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    definitions_path = args.definitions or config.get(
        "sgml_note_definitions", "sgml_note_definitions.json"
    )
    definitions = load_definitions(definitions_path)
    definitions_by_key = {
        (definition["note_type"], definition["definition_version"]): definition
        for definition in definitions
    }
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
    audit_table = checked_table_name(
        config.get("sgml_note_manual_review_table", "public.sgml_note_manual_review"),
        "sgml_note_manual_review_table",
    )
    import_hash = combined_file_hash(cases_path, facts_path)
    conn = psycopg2.connect(**config["db"])
    prepared: List[dict] = []
    errors: List[str] = []
    try:
        for row_no, case in enumerate(case_rows, start=2):
            decision = case["decision"].strip().upper()
            if decision == "PENDING" or not decision:
                continue
            analysis_hash = case["analysis_hash"].strip()
            if decision not in {"APPROVE", "EXCLUDE"}:
                errors.append(f"cases行{row_no}: decision={decision!r}は許可されていません")
                continue
            reviewer = case["reviewer"].strip()
            comment = case["review_comment"].strip()
            if not reviewer:
                errors.append(f"cases行{row_no}: reviewerが空です")
            if decision == "EXCLUDE" and not comment:
                errors.append(f"cases行{row_no}: EXCLUDEにはreview_commentが必要です")
            key = (case["note_type"].strip(), case["definition_version"].strip())
            definition = definitions_by_key.get(key)
            if definition is None:
                errors.append(f"cases行{row_no}: 現在のdefinitionに{key}がありません")
                continue
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT c.content_hash, c.note_type, c.definition_version,
                                b.block_text
                           FROM {candidate_table} c
                           JOIN {block_table} b ON b.block_id=c.block_id
                          WHERE c.is_current AND b.is_current
                            AND c.content_hash=%s
                            AND c.note_type=%s
                            AND c.definition_version=%s
                          ORDER BY c.candidate_id
                          LIMIT 1""",
                    (case["content_hash"].strip(), key[0], key[1]),
                )
                current = cur.fetchone()
                try:
                    source_run_id = int(case["source_run_id"])
                except ValueError:
                    source_run_id = -1
                cur.execute(
                    f"SELECT run_id, analysis_hash, model_name, status FROM {run_table} WHERE run_id=%s",
                    (source_run_id,),
                )
                source_run = cur.fetchone()
            if current is None:
                errors.append(f"cases行{row_no}: 対象候補が改訂又は無効化されています")
                continue
            expected_hash = sha256_text(
                f"{current['content_hash']}\n{current['note_type']}\n"
                f"{current['definition_version']}\n{case['prompt_version'].strip()}"
            )
            if expected_hash != analysis_hash:
                errors.append(f"cases行{row_no}: analysis_hashが現在の候補と一致しません")
                continue
            if (
                source_run is None
                or source_run["analysis_hash"] != analysis_hash
                or source_run["model_name"] != case["source_model"].strip()
            ):
                errors.append(f"cases行{row_no}: source runがDBと一致しません")
                continue
            selected_facts: List[dict] = []
            if decision == "APPROVE":
                for fact_row in facts_by_hash.get(analysis_hash, []):
                    if not included(fact_row["include"]):
                        continue
                    try:
                        details = json.loads(fact_row["details_json"].strip() or "{}")
                    except json.JSONDecodeError as exc:
                        errors.append(
                            f"analysis={analysis_hash[:12]} fact_no={fact_row.get('fact_no')}: "
                            f"details_json不正: {exc}"
                        )
                        continue
                    selected_facts.append(
                        {
                            "relation_type": fact_row["relation_type"].strip(),
                            "subject_type": fact_row["subject_type"].strip() or "DRUG",
                            "target_code": fact_row["target_code"].strip() or None,
                            "target_name": fact_row["target_name"].strip(),
                            "polarity": fact_row["polarity"].strip() or "POSITIVE",
                            "certainty": fact_row["certainty"].strip() or "EXPLICIT",
                            "note_text": fact_row["note_text"].strip(),
                            "details": details,
                            "evidence_text": fact_row["evidence_text"].strip(),
                        }
                    )
                if not selected_facts:
                    errors.append(f"cases行{row_no}: APPROVEですがinclude=TRUEのfactがありません")
                    continue
                valid_facts, validation_errors = validate_facts(
                    {"facts": selected_facts}, definition, current["block_text"]
                )
                if validation_errors:
                    errors.extend(
                        f"analysis={analysis_hash[:12]}: {item}" for item in validation_errors
                    )
                    continue
                selected_facts = valid_facts
            prepared.append(
                {
                    "case": case,
                    "decision": decision,
                    "reviewer": reviewer,
                    "comment": comment,
                    "source_run_id": source_run_id,
                    "content_hash": current["content_hash"],
                    "facts": selected_facts,
                }
            )

        if errors:
            for error in errors[:100]:
                log.error(error)
            raise ValueError(f"CSV検証エラーが{len(errors)}件あります。DBは変更していません")
        counts = defaultdict(int)
        for item in prepared:
            counts[item["decision"]] += 1
        log.info(
            "検証成功 approve=%s exclude=%s pending=%s LLM_calls=0",
            counts["APPROVE"],
            counts["EXCLUDE"],
            len(case_rows) - len(prepared),
        )
        if not args.execute:
            log.warning("dry-runです。反映する場合は--executeを指定してください")
            return

        create_audit_table(conn, audit_table)
        now = datetime.now(timezone.utc)
        for item in prepared:
            case = item["case"]
            response = {"facts": item["facts"], "manual_decision": item["decision"]}
            raw_response = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {run_table}
                        (analysis_hash, content_hash, note_type, definition_version,
                         prompt_version, model_name, server_url, status,
                         request_at, response_at, attempts, raw_response,
                         response_json, validation_errors, error_message)
                      VALUES (%s,%s,%s,%s,%s,%s,'manual://csv','success',
                              %s,%s,0,%s,%s,'[]'::jsonb,NULL)
                      ON CONFLICT (analysis_hash, model_name) DO UPDATE SET
                        content_hash=EXCLUDED.content_hash,
                        note_type=EXCLUDED.note_type,
                        definition_version=EXCLUDED.definition_version,
                        prompt_version=EXCLUDED.prompt_version,
                        server_url=EXCLUDED.server_url,
                        status='success', request_at=EXCLUDED.request_at,
                        response_at=EXCLUDED.response_at, attempts=0,
                        raw_response=EXCLUDED.raw_response,
                        response_json=EXCLUDED.response_json,
                        validation_errors='[]'::jsonb, error_message=NULL,
                        updated_at=now()
                      RETURNING run_id""",
                    (
                        case["analysis_hash"].strip(),
                        item["content_hash"],
                        case["note_type"].strip(),
                        case["definition_version"].strip(),
                        case["prompt_version"].strip(),
                        HUMAN_MODEL,
                        now,
                        now,
                        raw_response,
                        psycopg2.extras.Json(response),
                    ),
                )
                manual_run_id = cur.fetchone()[0]
                replace_facts(cur, fact_table, manual_run_id, item["facts"])
                cur.execute(
                    f"""INSERT INTO {audit_table}
                        (analysis_hash, note_type, decision, reviewer, review_comment,
                         source_model, source_run_id, manual_run_id, facts_json,
                         import_file_hash)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (analysis_hash, import_file_hash) DO NOTHING""",
                    (
                        case["analysis_hash"].strip(),
                        case["note_type"].strip(),
                        item["decision"],
                        item["reviewer"],
                        item["comment"],
                        case["source_model"].strip(),
                        item["source_run_id"],
                        manual_run_id,
                        psycopg2.extras.Json(item["facts"]),
                        import_hash,
                    ),
                )
        conn.commit()
        log.info("手動review反映完了 cases=%s LLM_calls=0", len(prepared))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()

