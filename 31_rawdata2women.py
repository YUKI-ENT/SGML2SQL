#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
31_rawdata2women.py
- sgml_rawdata.doc_xml から妊婦・授乳の本文を抽出し、sgml_women に保存
- 設定はカレントの config.json から
- 既存の sgml_women は DROP→CREATE（上書き）
- 進捗・ログを出力（./logs/51_build_sgml_women.log）
"""

import os
import json
import logging
import time
import re
from typing import Optional, Tuple, List, Dict

import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET

# ===================== ログ設定 =====================
SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_DIR = "./logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_BASENAME}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ===================== config.json（カレント） =====================
CONFIG_PATH = "config.json"
if not os.path.isfile(CONFIG_PATH):
    log.error("config.json がカレントに見つかりません。")
    raise SystemExit(1)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

db_conf = {
    "host":     config["db"]["host"],
    "port":     config["db"]["port"],
    "dbname":   config["db"]["dbname"],
    "user":     config["db"]["user"],
    "password": config["db"]["password"],
}

SRC_TABLE   = config.get("sgml_table", "public.sgml_rawdata")           # 入力（既存）
DEST_TABLE  = config.get("sgml_women_table", "public.sgml_women")       # 出力（新規作成）
BATCH_SIZE  = int(config.get("batch_size", 500))
PROG_EVERY  = int(config.get("progress_every", 2000))  # 進捗表示の間隔

# （参考）PMDAパッケージ挿入の名前空間—固定せずlocal-nameで拾うため使わない
XMLNS = "http://www.w3.org/XML/1998/namespace"

# ====== タグ候補（local-name 判定） ======
PREGNANT_TAGS = {
    "UseInPregnant", "UseInPregnantWomen", "Pregnant"
}
NURSING_TAGS = {
    "UseInNursing", "UseInNursingMothers", "Nursing", "BreastFeeding"
}
LANG_LOCAL = "Lang"

# ===================== ヘルパ =====================
def local_name(tag: str) -> str:
    """{namespace}Tag から Tag を取り出す（名前空間非依存）"""
    if tag is None:
        return ""
    return tag.split('}')[-1]

def textnorm(s: Optional[str]) -> Optional[str]:
    """軽い正規化：前後空白除去・連続空白/改行の整理（表現のバリエーションは基本保持）"""
    if s is None:
        return None
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t\u3000]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def extract_section_texts(xml_str: Optional[str], target_names: set) -> Tuple[Optional[str], Optional[str]]:
    """
    xml文字列から、local-name が target_names に一致する要素を探し、
    その配下の Lang 要素本文を抽出。
    返り値: (連結テキスト, 最初のヒット要素の id属性 or None)
    """
    if not xml_str:
        return None, None

    try:
        root = ET.fromstring(xml_str)  # recover相当はないが PMDA配布XMLなら通常OK
    except Exception as e:
        log.debug(f"XML parse error: {e}")
        return None, None

    # 該当要素を収集
    hits: List[ET.Element] = []
    for elem in root.iter():
        if local_name(elem.tag) in target_names:
            hits.append(elem)

    if not hits:
        return None, None

    first_id = hits[0].get("id")
    # Lang要素を収集（ja優先）
    ja_texts: List[str] = []
    other_texts: List[str] = []

    # hits それぞれの配下から Lang を探す
    for h in hits:
        # 直接/深い階層の Lang をすべて拾う
        for n in h.iter():
            if local_name(n.tag) == LANG_LOCAL:
                t = "".join(n.itertext())
                t = textnorm(t)
                if not t:
                    continue
                # lang属性判定（xml:lang または lang）
                lang = n.attrib.get(f"{{{XMLNS}}}lang") or n.attrib.get("lang")
                if lang and lang.lower().startswith("ja"):
                    ja_texts.append(t)
                else:
                    other_texts.append(t)

    if not ja_texts and not other_texts:
        # Langが無い場合はヒット要素テキスト全体を代替で抽出
        raw = "".join(hits[0].itertext())
        return textnorm(raw), first_id

    # 重複排除 + 結合
    def uniq(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for s in seq:
            if s not in seen:
                out.append(s)
                seen.add(s)
        return out

    parts: List[str] = []
    if ja_texts:
        parts.append("\n\n".join(uniq(ja_texts)))
    if other_texts:
        parts.append("\n\n".join(uniq(other_texts)))

    return (("\n\n".join(parts)).strip() if parts else None), first_id

# ===================== DDL =====================
DDL_DROP_CREATE = f"""
DROP TABLE IF EXISTS {DEST_TABLE};

CREATE TABLE {DEST_TABLE} (
    package_insert_no   text NOT NULL,
    yj_code             text NOT NULL,
    brand_name_ja       text,
    pregnant_text       text,
    nursing_text        text,
    has_pregnant        boolean,
    has_nursing         boolean,
    src_ids             jsonb,
    updated_at          timestamptz DEFAULT now(),
    PRIMARY KEY (package_insert_no, yj_code)
);

-- 日本語の部分一致検索に強い trigram インデックス
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX idx_{DEST_TABLE.split('.')[-1]}_pregnant_trgm
  ON {DEST_TABLE} USING gin (pregnant_text gin_trgm_ops);

CREATE INDEX idx_{DEST_TABLE.split('.')[-1]}_nursing_trgm
  ON {DEST_TABLE} USING gin (nursing_text gin_trgm_ops);
"""


UPSERT_SQL = f"""
INSERT INTO {DEST_TABLE}
(package_insert_no, yj_code, brand_name_ja, pregnant_text, nursing_text,
 has_pregnant, has_nursing, src_ids)
VALUES %s
ON CONFLICT (package_insert_no, yj_code) DO UPDATE
  SET brand_name_ja = EXCLUDED.brand_name_ja,
      pregnant_text = EXCLUDED.pregnant_text,
      nursing_text  = EXCLUDED.nursing_text,
      has_pregnant  = EXCLUDED.has_pregnant,
      has_nursing   = EXCLUDED.has_nursing,
      src_ids       = EXCLUDED.src_ids,
      updated_at    = now();
"""

# ===================== メイン処理 =====================
def main():
    dsn = (
        f"host={db_conf['host']} port={db_conf['port']} dbname={db_conf['dbname']} "
        f"user={db_conf['user']} password={db_conf['password']}"
    )
    t0 = time.time()
    log.info("=== sgml_women build start ===")
    log.info(f"source={SRC_TABLE} dest={DEST_TABLE} batch_size={BATCH_SIZE} progress_every={PROG_EVERY}")

    # ★ 読み取りと書き込みで接続を分ける
    read_conn  = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)
    read_conn.autocommit  = False     # named cursor用：トランザクション継続
    write_conn.autocommit = False     # バッチごとにcommit

    try:
        # ---- DDL は write_conn で実行 ----
        with write_conn.cursor() as cur:
            log.info("Recreate destination table (DROP → CREATE)...")
            cur.execute(DDL_DROP_CREATE)
            write_conn.commit()
            log.info("Table recreated.")

        total = 0
        upserted = 0

        # ---- 読み取り：サーバサイドカーソルは read_conn 側 ----
        with read_conn.cursor(name="cur_sgml_scan", cursor_factory=psycopg2.extras.DictCursor) as scan:
            scan.itersize = BATCH_SIZE
            scan.execute(f"""
                SELECT package_insert_no, yj_code, brand_name_ja, doc_xml
                  FROM {SRC_TABLE}
                 ORDER BY package_insert_no, yj_code;
            """)

            batch = []
            with write_conn.cursor() as wcur:
                for row in scan:
                    total += 1
                    pin  = row["package_insert_no"]
                    yjc  = row["yj_code"]
                    brand = row["brand_name_ja"]
                    xml   = row["doc_xml"]

                    preg_text, preg_id = extract_section_texts(xml, PREGNANT_TAGS)
                    nurs_text, nurs_id = extract_section_texts(xml, NURSING_TAGS)

                    has_preg = bool(preg_text)
                    has_nurs = bool(nurs_text)

                    src_ids = {}
                    if preg_id: src_ids["pregnant"] = preg_id
                    if nurs_id: src_ids["nursing"]  = nurs_id

                    batch.append((
                        pin, yjc, brand, preg_text, nurs_text, has_preg, has_nurs,
                        psycopg2.extras.Json(src_ids or None)
                    ))

                    if len(batch) >= BATCH_SIZE:
                        psycopg2.extras.execute_values(wcur, UPSERT_SQL, batch, page_size=BATCH_SIZE)
                        write_conn.commit()      # ★ commitは write_conn のみ
                        upserted += len(batch)
                        batch.clear()

                    if total % PROG_EVERY == 0:
                        elapsed = time.time() - t0
                        speed = total / elapsed if elapsed > 0 else 0.0
                        log.info(f"[progress] scanned={total:,} upserted≈{upserted:,} speed={speed:.1f} rows/s")

                # 端数コミット
                if batch:
                    psycopg2.extras.execute_values(wcur, UPSERT_SQL, batch, page_size=BATCH_SIZE)
                    write_conn.commit()
                    upserted += len(batch)
                    batch.clear()

        elapsed = time.time() - t0
        log.info(f"=== done: scanned={total:,} upserted≈{upserted:,} elapsed={elapsed:.1f}s ({(total/elapsed):.1f} rows/s) ===")

    finally:
        # read_conn はここで最後に閉じる（途中でcommitしない）
        try:
            read_conn.close()
        except Exception:
            pass
        try:
            write_conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
