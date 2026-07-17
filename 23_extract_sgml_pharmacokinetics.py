#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sgml_rawdata.doc_xml から16章「薬物動態」を節・チャンク単位で抽出する。

出力はLLM処理用の中間テーブル temp_sgml_pk_blocks のみ。
配布対象の sgml_* テーブルや OQSDrug の ai_* テーブルには触れない。
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras


SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_DIR = "./logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_BASENAME}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

PI_NS = "http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0"
NS = {"pi": PI_NS}

SECTION_CODES: Dict[str, str] = {
    "BloodLevel": "16.1",
    "Absorption": "16.2",
    "Distribution": "16.3",
    "Metabolism": "16.4",
    "Excretion": "16.5",
    "SpecificPopulation": "16.6",
    "DrugAndDrugInteractions": "16.7",
    "PharmacokineticsEtc": "16.8",
}

QUALIFIED_NAME_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PMDA XMLの16章をLLM処理用tempテーブルへ抽出")
    parser.add_argument("--config", default="config.json", help="設定JSON（既定: config.json）")
    parser.add_argument("--package-insert-no", help="指定した添付文書番号だけ抽出")
    parser.add_argument("--limit", type=int, help="抽出する添付文書数の上限（試験用）")
    parser.add_argument("--chunk-length", type=int, help="チャンク最大文字数")
    parser.add_argument("--chunk-overlap", type=int, help="隣接チャンクの重複文字数")
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def checked_table_name(value: str, setting_name: str) -> str:
    if not QUALIFIED_NAME_RX.fullmatch(value):
        raise ValueError(f"{setting_name} は schema.table 形式で指定してください: {value!r}")
    return value


def local_name(tag: str) -> str:
    return tag.split("}")[-1] if tag else ""


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u3000", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def element_text(element: ET.Element) -> str:
    return normalize_text("".join(element.itertext()))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def choose_break(text: str, start: int, desired_end: int) -> int:
    """文末を優先しつつ、極端に短くならない位置でチャンクを切る。"""
    if desired_end >= len(text):
        return len(text)
    lower = start + max(1, (desired_end - start) * 2 // 3)
    candidates = []
    for marker in ("。", "\n", "；", ";", "、", " "):
        pos = text.rfind(marker, lower, desired_end)
        if pos >= lower:
            candidates.append(pos + len(marker))
    return max(candidates) if candidates else desired_end


def chunk_text(text: str, max_length: int, overlap: int) -> Iterable[Tuple[int, int, str]]:
    if max_length <= 0:
        raise ValueError("chunk_length は1以上である必要があります")
    if overlap < 0 or overlap >= max_length:
        raise ValueError("chunk_overlap は0以上かつ chunk_length 未満である必要があります")

    start = 0
    while start < len(text):
        end = choose_break(text, start, min(start + max_length, len(text)))
        piece = text[start:end].strip()
        if piece:
            yield start, end, piece
        if end >= len(text):
            break
        next_start = max(start + 1, end - overlap)
        while next_start < end and text[next_start].isspace():
            next_start += 1
        start = next_start


def extract_rows(
    package_insert_no: str,
    prepared_ym: Optional[str],
    generic_name_ja: Optional[str],
    xml_text: str,
    chunk_length: int,
    chunk_overlap: int,
) -> List[Tuple]:
    root = ET.fromstring(xml_text)
    pk = root.find("pi:Pharmacokinetics", NS)
    if pk is None:
        return []

    rows: List[Tuple] = []
    section_order = 0
    for section in list(pk):
        section_type = local_name(section.tag)
        section_code = SECTION_CODES.get(section_type)
        if section_code is None:
            log.warning("未知の16章要素: package=%s tag=%s", package_insert_no, section_type)
            section_code = "16.?"
        section_order += 1

        section_text = element_text(section)
        if not section_text:
            continue
        section_hash = sha256_text(section_text)
        section_xml = ET.tostring(section, encoding="unicode")

        for chunk_no, (start, end, text) in enumerate(
            chunk_text(section_text, chunk_length, chunk_overlap), start=1
        ):
            rows.append(
                (
                    package_insert_no,
                    prepared_ym,
                    generic_name_ja,
                    section_code,
                    section_type,
                    section_order,
                    chunk_no,
                    start,
                    end,
                    section_hash,
                    sha256_text(text),
                    text,
                    section_xml if chunk_no == 1 else None,
                )
            )
    return rows


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if os.environ.get("PGPASSWORD"):
        config["db"]["password"] = os.environ["PGPASSWORD"]

    src_table = checked_table_name(config.get("sgml_table", "public.sgml_rawdata"), "sgml_table")
    block_table = checked_table_name(
        config.get("temp_sgml_pk_blocks_table", "public.temp_sgml_pk_blocks"),
        "temp_sgml_pk_blocks_table",
    )
    chunk_length = args.chunk_length or int(config.get("pk_chunk_length", config.get("chunk_length", 4000)))
    chunk_overlap = (
        args.chunk_overlap
        if args.chunk_overlap is not None
        else int(config.get("pk_chunk_overlap", config.get("chunk_overlap", 200)))
    )
    batch_size = int(config.get("batch_size", 500))
    progress_every = int(config.get("progress_every", 2000))

    db_conf = config["db"]
    read_conn = psycopg2.connect(**db_conf)
    write_conn = psycopg2.connect(**db_conf)
    read_conn.autocommit = False
    write_conn.autocommit = False

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {block_table} (
      block_id           bigserial PRIMARY KEY,
      package_insert_no  text NOT NULL,
      prepared_ym        text,
      generic_name_ja    text,
      section_code       text NOT NULL,
      section_type       text NOT NULL,
      section_order      integer NOT NULL,
      chunk_no           integer NOT NULL,
      char_start         integer NOT NULL,
      char_end           integer NOT NULL,
      section_hash       text NOT NULL,
      content_hash       text NOT NULL,
      block_text         text NOT NULL,
      section_xml        xml,
      created_at         timestamptz NOT NULL DEFAULT now(),
      UNIQUE (package_insert_no, section_type, chunk_no)
    );
    CREATE INDEX IF NOT EXISTS idx_{block_table.split('.')[-1]}_content_hash
      ON {block_table} (content_hash);
    CREATE INDEX IF NOT EXISTS idx_{block_table.split('.')[-1]}_package
      ON {block_table} (package_insert_no);
    """

    where = ["doc_xml IS NOT NULL"]
    params: List[object] = []
    if args.package_insert_no:
        where.append("package_insert_no = %s")
        params.append(args.package_insert_no)
    limit_sql = ""
    if args.limit is not None:
        if args.limit <= 0:
            raise ValueError("--limit は1以上で指定してください")
        limit_sql = " LIMIT %s"
        params.append(args.limit)

    select_sql = f"""
      SELECT DISTINCT ON (package_insert_no)
             package_insert_no, prepared_ym, generic_name_ja, doc_xml::text
        FROM {src_table}
       WHERE {' AND '.join(where)}
       ORDER BY package_insert_no, yj_code
       {limit_sql}
    """

    insert_sql = f"""
      INSERT INTO {block_table}
      (package_insert_no, prepared_ym, generic_name_ja,
       section_code, section_type, section_order, chunk_no,
       char_start, char_end, section_hash, content_hash, block_text, section_xml)
      VALUES %s
      ON CONFLICT (package_insert_no, section_type, chunk_no) DO UPDATE SET
        prepared_ym = EXCLUDED.prepared_ym,
        generic_name_ja = EXCLUDED.generic_name_ja,
        section_code = EXCLUDED.section_code,
        section_order = EXCLUDED.section_order,
        char_start = EXCLUDED.char_start,
        char_end = EXCLUDED.char_end,
        section_hash = EXCLUDED.section_hash,
        content_hash = EXCLUDED.content_hash,
        block_text = EXCLUDED.block_text,
        section_xml = EXCLUDED.section_xml,
        created_at = now()
    """

    started = time.time()
    documents = 0
    documents_with_pk = 0
    inserted = 0
    failed = 0
    batch: List[Tuple] = []

    try:
        with write_conn.cursor() as cur:
            cur.execute(ddl)
            if args.package_insert_no:
                cur.execute(
                    f"DELETE FROM {block_table} WHERE package_insert_no = %s",
                    (args.package_insert_no,),
                )
            elif args.limit is None:
                cur.execute(f"TRUNCATE TABLE {block_table} RESTART IDENTITY")
            else:
                log.warning(
                    "--limit 指定のため既存ブロックは削除しません。"
                    "全件再構築時は --limit を外してください。"
                )
            write_conn.commit()

        with read_conn.cursor(name="cur_pk_xml") as scan:
            scan.itersize = max(10, min(batch_size, 500))
            scan.execute(select_sql, params)
            with write_conn.cursor() as out:
                for package_insert_no, prepared_ym, generic_name_ja, xml_text in scan:
                    documents += 1
                    try:
                        new_rows = extract_rows(
                            package_insert_no,
                            prepared_ym,
                            generic_name_ja,
                            xml_text,
                            chunk_length,
                            chunk_overlap,
                        )
                    except Exception:
                        failed += 1
                        log.exception("XML抽出失敗: package_insert_no=%s", package_insert_no)
                        continue

                    if new_rows:
                        documents_with_pk += 1
                        batch.extend(new_rows)

                    if len(batch) >= batch_size:
                        psycopg2.extras.execute_values(out, insert_sql, batch, page_size=batch_size)
                        write_conn.commit()
                        inserted += len(batch)
                        batch.clear()

                    if documents % progress_every == 0:
                        log.info(
                            "進捗 documents=%s with_pk=%s blocks=%s failed=%s",
                            f"{documents:,}",
                            f"{documents_with_pk:,}",
                            f"{inserted:,}",
                            f"{failed:,}",
                        )

                if batch:
                    psycopg2.extras.execute_values(out, insert_sql, batch, page_size=batch_size)
                    write_conn.commit()
                    inserted += len(batch)

        elapsed = time.time() - started
        log.info(
            "完了 documents=%s with_pk=%s blocks=%s failed=%s elapsed=%.1fs",
            f"{documents:,}",
            f"{documents_with_pk:,}",
            f"{inserted:,}",
            f"{failed:,}",
            elapsed,
        )
    finally:
        read_conn.close()
        write_conn.close()


if __name__ == "__main__":
    main()
