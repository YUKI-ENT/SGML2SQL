#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
32_label_women_risk.py
- sgml_women（原文保持）に、厚労省Q&Aの記載強度に沿ったスコアとルールタグを付与
- エビデンス・フラグ（ヒト/動物催奇形性 等）を抽出し、sgml_women に JSONB で保持
- sgml_women_risk_labels（定義テーブル。ご指定名）を DROP→CREATE の上でUPSERT投入
- scheme='toranomon'（虎ノ門相当の簡易ラベル）を自動割当
- 設定はカレントの config.json から
- 大規模データ向けに read/write 接続を分離し、進捗ログを表示
"""

import os
import re
import json
import time
import logging
from typing import Dict, List, Tuple

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
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"),
              logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ===================== config.json =====================
CONFIG_PATH = "config.json"
if not os.path.isfile(CONFIG_PATH):
    log.error("config.json が見つかりません。")
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
SRC_TABLE  = config.get("sgml_women_table", "public.sgml_women")
DEST_TABLE = config.get("sgml_women_risk_table", "public.sgml_women_risk_labels")
BATCH_SIZE = int(config.get("batch_size", 1000))
PROG_EVERY = int(config.get("progress_every", 5000))

# ===================== ルール定義（厚労省Q&A準拠の強弱） =====================
# スコア: 3=強(投与しないこと), 2=中(投与しないことが望ましい), 1=弱(有益性>危険性でのみ投与), 0=不明
# ※ 妊婦側は「望ましい」を2に、「有益性＞危険性」を1に修正済み
# スコア: 3(強) > 2 > 1 > 0
PREG_RULES_RAW = [
    # 3: 投与/使用 禁止
    (3, r"(禁忌|投与してはならない|使用してはならない|"
         r"投与しないこと(?!が望ましい)|使用しないこと(?!が望ましい))", "contraindicated"),

    # 2: 投与/使用 しないことが望ましい・原則しない・推奨されない・回避推奨
    (2, r"(投与しないことが望ましい|使用しないことが望ましい|"
         r"原則.*(投与|使用)しない|"
         r"(投与|使用)は推奨されない|"
         r"(投与|使用)を避けることが望ましい|"
         r"(使用|投与)を回避することが望ましい|"
         r"可能な限り(投与|使用)を避ける)", "not_recommended"),

    # 1: 有益性>危険性/必要時のみ/やむを得ない場合のみ/必要最小限/慎重に投与
    (1, r"((治療上の)?(有益性|ベネフィット|利益|利点).*(危険性|リスク).*(上回|凌駕)[るれ]|"
         r"必要(な|時)?場合に(限り|のみ).*(投与|使用)|"
         r"やむを得ない場合(に限り|のみ).*(投与|使用)|"
         r"必要最小限(の用量|の範囲)?(での)?(投与|使用)|"
         r"慎重に投与)", "benefit_over_risk_or_caution"),
]
# 事前コンパイル
PREG_RULES = [(s, re.compile(p, re.DOTALL), tag) for (s,p,tag) in PREG_RULES_RAW]

NURS_RULES_RAW: List[Tuple[int, str, str]] = [
    (3, r"(授乳.*中止|断乳|母乳栄養.*中止)", "stop_lactation"),
    (2, r"(有益性.*考慮.*授乳.*(継続|中止).*検討)", "consider_benefit"),
    (1, r"(乳汁|母乳).*(移行|検出|認められた)", "info_only"),
]

# コンパイル済みにする（速度・安定）
NURS_RULES: List[Tuple[int, re.Pattern, str]] = [
    (s, re.compile(p, re.MULTILINE | re.DOTALL), tag) for (s, p, tag) in NURS_RULES_RAW
]

# ---- エビデンス・フラグ（妊婦） ----
PREG_EVIDENCE_PATS = {
    "has_human_terato":      r"(ヒト|人).*(奇形|催奇形|胎児障害|胎児毒性|胎児への影響|出生児.*(障害|影響))",
    "has_animal_terato":     r"(動物|非臨床|ラット|マウス|ウサギ|サル).*(催奇形|奇形性|胎児毒性|胚.*致死|胎仔.*死亡|胎児.*影響)",
    "has_nonmalform_harm":   r"(新生児|出生児).*(呼吸抑制|低血糖|離脱|禁断|鎮静|筋(緊張|弛緩)|神経|発達遅延|出血|動脈管.*(収縮|閉鎖)|骨.*(発育|成長|石灰化))",
    "mentions_trimester":    r"妊娠(初期|中期|後期|末期)|第[一二三1-3]三?半期|[1-3]期|妊娠[0-9０-９]{1,2}\s*ヶ?月",
    "mentions_uncertain":    r"(影響が不明|データが(ない|不足)|情報が不足|未知)",
    "pharm_concern":         r"(薬理作用|プロスタグランジン|RAAS|アンジオテンシン|NSAID|抗てんかん|葉酸拮抗|レチノイド|スタチン)"
}
# ---- エビデンス・フラグ（授乳） ----
NURS_EVIDENCE_PATS = {
    "milk_transfer_detected":     r"(乳汁|母乳).*(移行|検出|認められた|検出された)",
    "milk_transfer_not_detected": r"(乳汁|母乳).*(移行しない|検出されなかった|認められない)",
    "adverse_infant_effects":     r"(乳児|新生児|出生児).*(傾眠|鎮静|下痢|発疹|嘔吐|体重増加不良|呼吸抑制|肝障害|黄疸)",
    "recommend_stop_lactation":   r"(授乳.*中止|断乳|母乳栄養.*中止)",
    "recommend_consideration":    r"(有益性.*考慮.*(継続|中止).*検討)",
    "pumping_discard":            r"(搾乳.*破棄|ミルク.*置換|代替栄養|人工乳)"
}
# 事前コンパイル
PREG_EVIDENCE_RX = {k: re.compile(v, re.MULTILINE | re.DOTALL) for k, v in PREG_EVIDENCE_PATS.items()}
NURS_EVIDENCE_RX = {k: re.compile(v, re.MULTILINE | re.DOTALL) for k, v in NURS_EVIDENCE_PATS.items()}

def normalize_for_match(s: str) -> str:
    """
    マッチ判定専用の軽い正規化（原文は変更しない）
    - 全角空白→半角
    - 改行→空白
    - 「上まわる/上廻る」→「上回る」に寄せる
    """
    if not isinstance(s, str):
        return ""
    s = s.replace("\u3000", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    # 表記ゆれ
    s = s.replace("上まわる", "上回る").replace("上廻る", "上回る")
    return s.strip()

def classify(text: str, rules: List[Tuple[int, re.Pattern, str]]) -> Tuple[int, str]:
    if not isinstance(text, str) or not text.strip():
        return 0, "none"
    t = normalize_for_match(text)
    for score, rx, tag in rules:
        if rx.search(t):
            return score, tag
    return 0, "unclear"

def extract_flags(text: str, rx_dict: Dict[str, re.Pattern]) -> Dict[str, bool]:
    """パターン辞書に対して True/False のフラグを返す。"""
    if not isinstance(text, str) or not text.strip():
        return {}
    return {k: bool(rx.search(text)) for k, rx in rx_dict.items()}

# 虎ノ門ラベル（簡易）：score→ラベル
def tora_label_preg(score: int) -> str:
    return {3: "D/X", 2: "C", 1: "B", 0: "不明"}.get(score, "不明")

def tora_label_nurs(score: int) -> str:
    return {3: "授乳中止", 2: "有益性考慮", 1: "情報提供", 0: "不明"}.get(score, "不明")

# ===================== DDL =====================
DDL_DEST_DROP_CREATE = f"""
DROP TABLE IF EXISTS {DEST_TABLE};

CREATE TABLE {DEST_TABLE} (
  package_insert_no text NOT NULL,
  yj_code           text NOT NULL,
  scheme            text NOT NULL,    -- 'toranomon' など
  pregnant_label    text,
  nursing_label     text,
  pregnant_score    int,
  nursing_score     int,
  evidence_json     jsonb,
  updated_at        timestamptz DEFAULT now(),
  PRIMARY KEY (package_insert_no, yj_code, scheme)
);
"""

# sgml_women にスコア列 & エビデンス JSON を追加（なければ）
DDL_ALTER_SRC = f"""
ALTER TABLE {SRC_TABLE}
  ADD COLUMN IF NOT EXISTS pregnant_score int,
  ADD COLUMN IF NOT EXISTS pregnant_rule  text,
  ADD COLUMN IF NOT EXISTS nursing_score  int,
  ADD COLUMN IF NOT EXISTS nursing_rule   text,
  ADD COLUMN IF NOT EXISTS overall_score  int,
  ADD COLUMN IF NOT EXISTS pregnant_evidence jsonb,
  ADD COLUMN IF NOT EXISTS nursing_evidence  jsonb;
"""

UPSERT_DEST = f"""
INSERT INTO {DEST_TABLE}
(package_insert_no, yj_code, scheme, pregnant_label, nursing_label,
 pregnant_score, nursing_score, evidence_json)
VALUES %s
ON CONFLICT (package_insert_no, yj_code, scheme) DO UPDATE
  SET pregnant_label = EXCLUDED.pregnant_label,
      nursing_label  = EXCLUDED.nursing_label,
      pregnant_score = EXCLUDED.pregnant_score,
      nursing_score  = EXCLUDED.nursing_score,
      evidence_json  = EXCLUDED.evidence_json,
      updated_at     = now();
"""

def build_update_src_sql(src_table: str, rows: List[Tuple]) -> Tuple[str, List]:
    """
    rows: (package_insert_no, yj_code, p_score, p_rule, n_score, n_rule, overall, preg_evi_json, nurs_evi_json)
    """
    placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(rows))
    sql = f"""
    WITH vals(package_insert_no, yj_code, ps, pr, ns, nr, os, pe, ne) AS (
      VALUES {placeholders}
    )
    UPDATE {src_table} s
       SET pregnant_score   = v.ps,
           pregnant_rule    = v.pr,
           nursing_score    = v.ns,
           nursing_rule     = v.nr,
           overall_score    = v.os,
           pregnant_evidence= v.pe::jsonb,
           nursing_evidence = v.ne::jsonb
      FROM vals v
     WHERE s.package_insert_no = v.package_insert_no
       AND s.yj_code = v.yj_code;
    """
    flat: List = []
    for r in rows:
        flat.extend(r)
    return sql, flat

# ===================== メイン =====================
def main():
    dsn = f"host={db_conf['host']} port={db_conf['port']} dbname={db_conf['dbname']} user={db_conf['user']} password={db_conf['password']}"
    t0 = time.time()
    log.info("=== women risk labeling start ===")
    log.info(f"src={SRC_TABLE} dest={DEST_TABLE} batch_size={BATCH_SIZE} progress_every={PROG_EVERY}")

    # read / write 接続を分離（named cursor を commit で殺さないため）
    read_conn  = psycopg2.connect(dsn)
    write_conn = psycopg2.connect(dsn)
    read_conn.autocommit  = False
    write_conn.autocommit = False

    try:
        # DDL（write_conn）
        with write_conn.cursor() as cur:
            log.info("Create/alter destination & source tables...")
            cur.execute(DDL_DEST_DROP_CREATE)
            cur.execute(DDL_ALTER_SRC)
            write_conn.commit()
            log.info("Tables ready.")

        total = 0
        updated_src = 0
        upserted_dest = 0

        # 読み取りカーソル（read_conn）
        with read_conn.cursor(name="cur_women_scan", cursor_factory=psycopg2.extras.DictCursor) as scan:
            scan.itersize = BATCH_SIZE
            scan.execute(f"""
                SELECT package_insert_no, yj_code, brand_name_ja,
                       pregnant_text, nursing_text
                  FROM {SRC_TABLE}
                 ORDER BY package_insert_no, yj_code;
            """)

            batch_update_src: List[Tuple] = []
            batch_upsert_dest: List[Tuple] = []

            with write_conn.cursor() as wcur:
                for row in scan:
                    total += 1
                    pin = row["package_insert_no"]
                    yjc = row["yj_code"]
                    ptx = row["pregnant_text"] or ""
                    ntx = row["nursing_text"] or ""

                    # スコア／ルール
                    p_score, p_rule = classify(ptx, PREG_RULES)
                    n_score, n_rule = classify(ntx, NURS_RULES)
                    overall = max(p_score, n_score)

                    # エビデンス・フラグ
                    preg_flags = extract_flags(ptx, PREG_EVIDENCE_RX)
                    nurs_flags = extract_flags(ntx, NURS_EVIDENCE_RX)

                    # 簡易コンフィデンス（0–3）：スコア強度 + 一部フラグで加点（上限3）
                    preg_conf = min(3, (3 if p_score==3 else 2 if p_score==2 else 1 if p_score==1 else 0)
                                       + (1 if preg_flags.get("has_human_terato") else 0)
                                       + (1 if preg_flags.get("has_animal_terato") else 0))
                    nurs_conf = min(3, (3 if n_score==3 else 2 if n_score==2 else 1 if n_score==1 else 0)
                                       + (1 if nurs_flags.get("milk_transfer_detected") else 0)
                                       + (1 if nurs_flags.get("adverse_infant_effects") else 0))

                    # sgml_women 更新用
                    batch_update_src.append((
                        pin, yjc, p_score, p_rule, n_score, n_rule, overall,
                        psycopg2.extras.Json(preg_flags),
                        psycopg2.extras.Json(nurs_flags),
                    ))

                    # labels 用 evidence_json
                    evidence = {
                        "pregnant_rule": p_rule,
                        "nursing_rule": n_rule,
                        "preg": {**preg_flags, "confidence": preg_conf},
                        "nurs": {**nurs_flags, "confidence": nurs_conf},
                    }
                    batch_upsert_dest.append((
                        pin, yjc, "toranomon",
                        tora_label_preg(p_score),
                        tora_label_nurs(n_score),
                        p_score, n_score,
                        psycopg2.extras.Json(evidence)
                    ))

                    # バッチ書き込み
                    if len(batch_update_src) >= BATCH_SIZE:
                        # sgml_women UPDATE
                        sql, params = build_update_src_sql(SRC_TABLE, batch_update_src)
                        wcur.execute(sql, params)
                        write_conn.commit()
                        updated_src += len(batch_update_src)
                        batch_update_src.clear()

                        # risk_labels UPSERT
                        psycopg2.extras.execute_values(wcur, UPSERT_DEST, batch_upsert_dest, page_size=BATCH_SIZE)
                        write_conn.commit()
                        upserted_dest += len(batch_upsert_dest)
                        batch_upsert_dest.clear()

                    if p_score == 0 and ptx.strip():
                        # 低頻度で良い（例：最初の1000件のうち5件だけ）
                        if total <= 1000 and (total % 200 == 0):
                            log.warning(f"[pregnant_score=0 sample] {pin}/{yjc} text={ptx[:120]}...")

                    if total % PROG_EVERY == 0:
                        elapsed = time.time() - t0
                        speed = total / elapsed if elapsed > 0 else 0.0
                        log.info(f"[progress] scanned={total:,} upd_src≈{updated_src:,} upsert_dest≈{upserted_dest:,} speed={speed:.1f} rows/s")

                # 端数コミット
                if batch_update_src:
                    sql, params = build_update_src_sql(SRC_TABLE, batch_update_src)
                    wcur.execute(sql, params)
                    write_conn.commit()
                    updated_src += len(batch_update_src)
                    batch_update_src.clear()

                if batch_upsert_dest:
                    psycopg2.extras.execute_values(wcur, UPSERT_DEST, batch_upsert_dest, page_size=BATCH_SIZE)
                    write_conn.commit()
                    upserted_dest += len(batch_upsert_dest)
                    batch_upsert_dest.clear()

        elapsed = time.time() - t0
        log.info(f"=== done: scanned={total:,} updated_src≈{updated_src:,} upserted_dest≈{upserted_dest:,} "
                 f"elapsed={elapsed:.1f}s ({(total/elapsed):.1f} rows/s) ===")

    finally:
        try: read_conn.close()
        except Exception: pass
        try: write_conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
