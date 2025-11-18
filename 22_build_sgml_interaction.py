#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
22_build_sgml_interaction.py

- sgml_rawdata（config.json の sgml_table）から interactions_flat を読み取り
- sgml_interaction（config.json の sgml_interaction_table）に平坦化して保存
- partner_group_ja カラムを追加（現時点では None。将来的に interactions_flat 内 "group" を参照予定）
- 既存テーブルは DROP して作り直し（毎回再生成前提）
- ログは ./logs/22_build_sgml_interaction.log
"""

import os
import json
import logging
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

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

# ===================== config.json 読み込み =====================
CONFIG_PATH = "config.json"
if not os.path.isfile(CONFIG_PATH):
    logging.error("config.json が見つかりません。")
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

SRC_TABLE = config.get("sgml_table", "public.sgml_rawdata")
DST_TABLE = config.get("sgml_interaction_table", "public.sgml_interaction")

BATCH_SIZE = int(config.get("batch_size", 500))
PROGRESS_EVERY = int(config.get("progress_every", 2000))

# テーブル名だけからベース名を取り出して index 名に使う
DST_BASENAME = DST_TABLE.split(".")[-1]

# ===================== CREATE TABLE =====================
CREATE_TABLE_SQL = f"""
DROP TABLE IF EXISTS {DST_TABLE} CASCADE;

CREATE TABLE {DST_TABLE} (
  id                    bigserial PRIMARY KEY,
  package_insert_no     text NOT NULL,
  yj_code               text NOT NULL,

  -- 区分（併用禁忌 / 併用注意 など）
  section_type          text,

  -- クラス名（強い又は中程度のCYP3A阻害剤、抗コリン剤、CYP3A誘導剤 など）
  partner_group_ja      text,

  -- 個別名（イトラコナゾール、クラリスロマイシン、ジルチアゼム など）
  partner_name_ja       text,

  -- 症状・対応
  symptoms_measures_ja  text,

  -- 機序・リスク要因
  mechanism_ja          text,

  created_at            timestamptz DEFAULT now()
);

CREATE INDEX idx_{DST_BASENAME}_yj
  ON {DST_TABLE} (yj_code);

CREATE INDEX idx_{DST_BASENAME}_pkg
  ON {DST_TABLE} (package_insert_no);

CREATE INDEX idx_{DST_BASENAME}_partner
  ON {DST_TABLE} (partner_name_ja);
"""

# ===================== INSERT 用 SQL =====================
INSERT_SQL = f"""
INSERT INTO {DST_TABLE}
(package_insert_no, yj_code, section_type,
 partner_group_ja, partner_name_ja,
 symptoms_measures_ja, mechanism_ja)
VALUES
(%(package_insert_no)s, %(yj_code)s, %(section_type)s,
 %(partner_group_ja)s, %(partner_name_ja)s,
 %(symptoms_measures_ja)s, %(mechanism_ja)s);
"""

# ===================== ヘルパ =====================
def fmt_eta(seconds: float) -> str:
    if seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:d}h{m:02d}m{s:02d}s"
    return f"{m:02d}m{s:02d}s"


def build_interaction_rows(pkg: str, yj: str, inter_flat: Any) -> List[Dict[str, Optional[str]]]:
    """
    sgml_rawdata.interactions_flat から sgml_interaction 1レコード単位に変換する。

    inter_flat は以下の形式を想定（21_sgml2sql.py 側の仕様）:
      [
        {
          "partner": "...",
          "symptoms": "...",
          "mechanism": "...",
          "category": "併用禁忌" or "併用注意",
          // 将来的に "group": "強い又は中程度のCYP3A阻害剤" などを追加予定
        },
        ...
      ]
    """
    rows: List[Dict[str, Optional[str]]] = []

    if not inter_flat:
        return rows

    if isinstance(inter_flat, str):
        # 念のため文字列で来た場合のフォールバック
        try:
            inter_flat = json.loads(inter_flat)
        except Exception:
            return rows

    if not isinstance(inter_flat, list):
        return rows

    for item in inter_flat:
        if not isinstance(item, dict):
            continue

        section_type = item.get("category")  # 併用禁忌 / 併用注意 等
        partner_name = item.get("partner")
        symptoms     = item.get("symptoms")
        mechanism    = item.get("mechanism")

        # 将来的に 21_sgml2sql.py が "group" を入れるようにした場合、
        # ここで自動的に partner_group_ja に入る。
        partner_group = item.get("group")  # 現状 None のままになる想定

        # 完全に空の行はスキップ
        if not (partner_name or partner_group or symptoms or mechanism):
            continue

        rows.append({
            "package_insert_no":    pkg,
            "yj_code":              yj,
            "section_type":         section_type,
            "partner_group_ja":     partner_group,
            "partner_name_ja":      partner_name,
            "symptoms_measures_ja": symptoms,
            "mechanism_ja":         mechanism,
        })

    return rows

# ===================== メイン処理 =====================
def main():
    logging.info(f"Start build sgml_interaction: src={SRC_TABLE}, dst={DST_TABLE}")

    conn = psycopg2.connect(**db_conf)
    try:
        with conn, conn.cursor() as cur:
            logging.info(f"DROP -> CREATE {DST_TABLE}")
            cur.execute(CREATE_TABLE_SQL)

        with conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # interactions_flat が NULL でない行だけ対象
            cur.execute(f"""
                SELECT package_insert_no, yj_code, interactions_flat
                FROM {SRC_TABLE}
                WHERE interactions_flat IS NOT NULL;
            """)
            rows = cur.fetchall()

        total = len(rows)
        logging.info(f"source rows with interactions_flat: {total}")

        inserted_total = 0
        t0 = time.time()
        batch: List[Dict[str, Optional[str]]] = []

        with conn, conn.cursor() as cur:
            for idx, r in enumerate(rows, start=1):
                pkg = r["package_insert_no"]
                yj  = r["yj_code"]
                inter_flat = r["interactions_flat"]

                new_rows = build_interaction_rows(pkg, yj, inter_flat)
                batch.extend(new_rows)

                if len(batch) >= BATCH_SIZE:
                    psycopg2.extras.execute_batch(cur, INSERT_SQL, batch, page_size=BATCH_SIZE)
                    inserted_total += len(batch)
                    batch.clear()

                if idx % PROGRESS_EVERY == 0:
                    elapsed = time.time() - t0
                    rate = idx / total if total else 1.0
                    eta = (elapsed / rate) - elapsed if rate > 0 else -1
                    logging.info(
                        f"[{idx}/{total} {rate*100:5.1f}% ETA:{fmt_eta(eta)}] "
                        f"inserted={inserted_total}"
                    )

            # 残りのバッチ
            if batch:
                psycopg2.extras.execute_batch(cur, INSERT_SQL, batch, page_size=BATCH_SIZE)
                inserted_total += len(batch)
                batch.clear()

        total_time = time.time() - t0
        logging.info(
            "===== SUMMARY =====\n"
            f"src table: {SRC_TABLE}\n"
            f"dst table: {DST_TABLE}\n"
            f"source rows: {total}\n"
            f"inserted rows: {inserted_total}\n"
            f"total time: {total_time:.2f}s"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
