#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""未解決sgml_note候補を手動確認用の2つのCSVへ出力する。"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import psycopg2
import psycopg2.extras

from _sgml_note_common import (
    checked_table_name,
    json_from_model_text,
    load_config,
    sha256_text,
)


log = logging.getLogger(__name__)
HUMAN_MODEL = "human-review"

CASE_FIELDS = [
    "analysis_hash",
    "note_type",
    "definition_version",
    "prompt_version",
    "content_hash",
    "source_run_id",
    "source_model",
    "source_status",
    "package_insert_no",
    "generic_name_ja",
    "section_code",
    "section_type",
    "heading_path",
    "validation_errors",
    "error_message",
    "block_text",
    "decision",
    "reviewer",
    "review_comment",
]

FACT_FIELDS = [
    "analysis_hash",
    "fact_no",
    "include",
    "relation_type",
    "subject_type",
    "target_code",
    "target_name",
    "polarity",
    "certainty",
    "note_text",
    "details_json",
    "evidence_text",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="sgml_noteの未解決候補をCSV出力")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--note-type", required=True)
    parser.add_argument(
        "--model",
        action="append",
        required=True,
        help="未解決判定に使うモデル。記載順にraw_responseを優先。複数回指定可",
    )
    parser.add_argument("--prompt-version")
    parser.add_argument("--package-insert-no")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def raw_facts(raw_response: str | None) -> List[dict]:
    if not raw_response:
        return []
    try:
        parsed = json_from_model_text(raw_response)
    except Exception:
        return []
    facts = parsed.get("facts")
    return [fact for fact in facts if isinstance(fact, dict)] if isinstance(facts, list) else []


def write_csv(path: Path, fields: List[str], rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limitは1以上にしてください")
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    prompt_version = args.prompt_version or config.get("note_prompt_version", "sgml-note-v4")
    models = list(dict.fromkeys(args.model))
    coverage_models = list(dict.fromkeys([HUMAN_MODEL, *models]))
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
    output_dir = Path(args.output_dir)
    cases_path = output_dir / "sgml_note_review_cases.csv"
    facts_path = output_dir / "sgml_note_review_facts.csv"
    if not args.overwrite and (cases_path.exists() or facts_path.exists()):
        raise FileExistsError("出力CSVが既にあります。上書きする場合は--overwriteを指定してください")

    where = ["c.is_current", "b.is_current", "c.note_type=%s"]
    params: List[object] = [args.note_type]
    if args.package_insert_no:
        where.append("c.package_insert_no=%s")
        params.append(args.package_insert_no)
    conn = psycopg2.connect(**config["db"])
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT DISTINCT ON (c.content_hash, c.note_type, c.definition_version)
                            c.content_hash, c.note_type, c.definition_version,
                            c.package_insert_no, b.generic_name_ja,
                            b.section_code, b.section_type, b.heading_path, b.block_text
                       FROM {candidate_table} c
                       JOIN {block_table} b ON b.block_id=c.block_id
                      WHERE {' AND '.join(where)}
                      ORDER BY c.content_hash, c.note_type, c.definition_version, c.candidate_id""",
                params,
            )
            candidates = [dict(row) for row in cur.fetchall()]
        for candidate in candidates:
            candidate["analysis_hash"] = sha256_text(
                f"{candidate['content_hash']}\n{candidate['note_type']}\n"
                f"{candidate['definition_version']}\n{prompt_version}"
            )
        hashes = sorted({candidate["analysis_hash"] for candidate in candidates})
        runs_by_hash: Dict[str, Dict[str, dict]] = {}
        if hashes:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT run_id, analysis_hash, model_name, status,
                                raw_response, validation_errors, error_message
                           FROM {run_table}
                          WHERE analysis_hash=ANY(%s)
                            AND model_name=ANY(%s)
                            AND prompt_version=%s""",
                    (hashes, coverage_models, prompt_version),
                )
                for row in cur.fetchall():
                    runs_by_hash.setdefault(row["analysis_hash"], {})[row["model_name"]] = dict(row)

        case_rows: List[dict] = []
        fact_rows: List[dict] = []
        for candidate in candidates:
            model_runs = runs_by_hash.get(candidate["analysis_hash"], {})
            if any(run["status"] == "success" for run in model_runs.values()):
                continue
            source = next(
                (
                    model_runs[model]
                    for model in models
                    if model in model_runs and model_runs[model]["status"] == "review"
                ),
                None,
            )
            if source is None:
                source = next(
                    (
                        model_runs[model]
                        for model in models
                        if model in model_runs and model_runs[model]["status"] == "error"
                    ),
                    None,
                )
            if source is None:
                continue
            case_rows.append(
                {
                    "analysis_hash": candidate["analysis_hash"],
                    "note_type": candidate["note_type"],
                    "definition_version": candidate["definition_version"],
                    "prompt_version": prompt_version,
                    "content_hash": candidate["content_hash"],
                    "source_run_id": source["run_id"],
                    "source_model": source["model_name"],
                    "source_status": source["status"],
                    "package_insert_no": candidate["package_insert_no"],
                    "generic_name_ja": candidate["generic_name_ja"] or "",
                    "section_code": candidate["section_code"] or "",
                    "section_type": candidate["section_type"] or "",
                    "heading_path": candidate["heading_path"] or "",
                    "validation_errors": json.dumps(
                        source["validation_errors"] or [], ensure_ascii=False
                    ),
                    "error_message": source["error_message"] or "",
                    "block_text": candidate["block_text"],
                    "decision": "PENDING",
                    "reviewer": "",
                    "review_comment": "",
                }
            )
            proposed = raw_facts(source["raw_response"])
            if not proposed:
                proposed = [{}]
            for fact_no, fact in enumerate(proposed, start=1):
                details = fact.get("details", {})
                if isinstance(details, (dict, list)):
                    details_text = json.dumps(details, ensure_ascii=False, separators=(",", ":"))
                else:
                    details_text = str(details) if details is not None else "{}"
                fact_rows.append(
                    {
                        "analysis_hash": candidate["analysis_hash"],
                        "fact_no": fact_no,
                        "include": "FALSE",
                        "relation_type": fact.get("relation_type", "") or "",
                        "subject_type": fact.get("subject_type", "DRUG") or "DRUG",
                        "target_code": fact.get("target_code", "") or "",
                        "target_name": fact.get("target_name", "") or "",
                        "polarity": fact.get("polarity", "POSITIVE") or "POSITIVE",
                        "certainty": fact.get("certainty", "EXPLICIT") or "EXPLICIT",
                        "note_text": fact.get("note_text", "") or "",
                        "details_json": details_text,
                        "evidence_text": fact.get("evidence_text", "") or "",
                    }
                )
            if args.limit is not None and len(case_rows) >= args.limit:
                break

        output_dir.mkdir(parents=True, exist_ok=True)
        write_csv(cases_path, CASE_FIELDS, case_rows)
        write_csv(facts_path, FACT_FIELDS, fact_rows)
        log.info(
            "出力完了 cases=%s facts=%s cases_file=%s facts_file=%s",
            len(case_rows),
            len(fact_rows),
            cases_path,
            facts_path,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()

