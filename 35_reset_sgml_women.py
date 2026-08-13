#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""新しい妊婦・授乳パイプラインのテーブルを明示指定でリセットする。"""

from __future__ import annotations

import argparse
import logging
import os

import psycopg2

from _sgml_note_common import checked_table_name, load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="妊婦・授乳パイプラインをTRUNCATE")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--include-published", action="store_true")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    keys = [
        ("temp_sgml_women_fact_table", "public.temp_sgml_women_fact"),
        ("temp_sgml_women_run_table", "public.temp_sgml_women_run"),
        ("temp_sgml_women_candidate_table", "public.temp_sgml_women_candidate"),
        ("sgml_women_block_table", "public.sgml_women_block"),
        ("sgml_women_state_table", "public.sgml_women_document_state"),
    ]
    if args.include_published:
        keys = [
            ("sgml_women_statement_table", "public.sgml_women_statement"),
            ("sgml_women_summary_table", "public.sgml_women_summary"),
        ] + keys
    tables = [checked_table_name(config.get(key, default), key) for key, default in keys]
    print("対象テーブル:")
    for table in tables:
        print(f"  {table}")
    if not args.execute:
        print("dry-runです。実行する場合は --execute を付けてください。")
        return
    conn = psycopg2.connect(**config["db"])
    try:
        with conn.cursor() as cur:
            existing = []
            for table in tables:
                cur.execute("SELECT to_regclass(%s)", (table,))
                if cur.fetchone()[0] is not None:
                    existing.append(table)
            if existing:
                cur.execute("TRUNCATE TABLE " + ", ".join(existing) + " RESTART IDENTITY")
        conn.commit()
        logging.info("リセット完了 tables=%s", len(existing))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

