#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""41～43のsgml_note中間テーブルを安全に初期化する。"""

from __future__ import annotations

import argparse
import logging
import os
from typing import List

import psycopg2

from _sgml_note_common import checked_table_name, load_config


log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="sgml_noteパイプラインの中間結果をTRUNCATE")
    parser.add_argument("--config", default="config.json")
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="44が公開したsgml_noteも初期化する",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="実際にTRUNCATEを実行する。省略時は対象と件数の確認だけ行う",
    )
    return parser.parse_args()


def target_tables(config: dict, include_published: bool) -> List[str]:
    tables = [
        checked_table_name(
            config.get("temp_sgml_note_fact_table", "public.temp_sgml_note_fact"),
            "temp_sgml_note_fact_table",
        ),
        checked_table_name(
            config.get("temp_sgml_note_run_table", "public.temp_sgml_note_run"),
            "temp_sgml_note_run_table",
        ),
        checked_table_name(
            config.get("temp_sgml_note_candidate_table", "public.temp_sgml_note_candidate"),
            "temp_sgml_note_candidate_table",
        ),
        checked_table_name(
            config.get("sgml_note_block_table", "public.sgml_note_block"),
            "sgml_note_block_table",
        ),
        checked_table_name(
            config.get("sgml_note_document_state_table", "public.sgml_note_document_state"),
            "sgml_note_document_state_table",
        ),
    ]
    if include_published:
        tables.insert(
            0,
            checked_table_name(
                config.get("sgml_note_table", "public.sgml_note"),
                "sgml_note_table",
            ),
        )
    return list(dict.fromkeys(tables))


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]
    tables = target_tables(config, args.include_published)

    conn = psycopg2.connect(**config["db"])
    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT count(*) FROM {table}")
                log.info("対象 table=%s rows=%s", table, cur.fetchone()[0])

        if not args.execute:
            log.warning("確認のみです。初期化する場合は --execute を指定してください。")
            if not args.include_published:
                log.warning(
                    "public.sgml_noteは対象外です。公開結果も消す場合は"
                    " --include-published を追加してください。"
                )
            return

        sql = "TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY"
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log.info("初期化完了 tables=%s", len(tables))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
