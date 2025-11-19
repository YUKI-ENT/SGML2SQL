#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
21_sgml2sql.py
- PMDA マイ医薬品集の SGML/XML を走査し、sgml_rawdata に投入
- 第1層（キー・ブランドなど）は列、主要セクションは JSONB（＋全文 doc_xml）
- 相互作用は禁忌/注意を平坦化して interactions_flat にも保存
- 設定はカレントの config.json から
- ログは ./logs/21_sgml2sql.log（スクリプト名に連動）
"""

import os
import glob
import json
import logging
import time
import re
import zipfile
from typing import Dict, List, Optional, Tuple, Any

import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET

# ===================== ログ設定 =====================
SCRIPT_BASENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_DIR = "./logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_BASENAME}.log")
FAILED_CSV = os.path.join(LOG_DIR, "failed_files.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ===================== config.json（カレント） =====================
CONFIG_PATH = "config.json"
if not os.path.isfile(CONFIG_PATH):
    logging.error("config.json がカレントに見つかりません。")
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
XML_ROOT = config.get("DI_folder") or "./drug_information"
TABLE_NAME = config.get("sgml_table", "public.sgml_rawdata")
TABLE_BASENAME = TABLE_NAME.split(".")[-1]  # index名に使う

# ===================== ZIP解凍 =====================================
def extract_all_zips(root_dir):
    """
    PMDA 配布 SGML/XML データ内の ZIP をすべて再帰抽出する。
    ZIP と同じフォルダに展開する。
    """
    for folder, _, files in os.walk(root_dir):
        for f in files:
            if f.lower().endswith(".zip"):
                zip_path = os.path.join(folder, f)
                print(f"[ZIP] Extracting: {zip_path}")

                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        z.extractall(folder)
                    print(f"[ZIP] OK → {folder}")
                except Exception as e:
                    print(f"[ZIP] ERROR: {e}")

# ===================== XML 名前空間 & ヘルパ =====================
PI_NS  = "http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0"
XMLNS  = "http://www.w3.org/XML/1998/namespace"
NS     = {"pi": PI_NS, "xml": XMLNS}


def get_text(node: Optional[ET.Element], xpath: str) -> Optional[str]:
    if node is None:
        return None
    el = node.find(xpath, NS)
    if el is not None and el.text:
        s = el.text.strip()
        return s or None
    return None


def get_text_direct(el: Optional[ET.Element]) -> Optional[str]:
    if el is not None and el.text:
        s = el.text.strip()
        return s or None
    return None


def text_ja(detail_elem: Optional[ET.Element]) -> Optional[str]:
    """<Detail><Lang xml:lang='ja'> または直テキストを取得"""
    if detail_elem is None:
        return None
    for lang in detail_elem.findall("pi:Lang", NS):
        if lang.get(f"{{{XMLNS}}}lang") == "ja" and lang.text:
            s = lang.text.strip()
            if s:
                return s
    if detail_elem.text and detail_elem.text.strip():
        return detail_elem.text.strip()
    return None


def lang_text_all(detail_elem: Optional[ET.Element]) -> Optional[str]:
    """<Lang xml:lang='ja'> の子を含む全文（itertext）"""
    if detail_elem is None:
        return None
    for lang in detail_elem.findall("pi:Lang", NS):
        if lang.get(f"{{{XMLNS}}}lang") == "ja":
            txt = "".join(lang.itertext()).strip()
            if txt:
                return txt
    return None


# ---------- partner 名の正規化（「等」「など」を落とす） ----------
def normalize_partner_label(s: Optional[str]) -> Optional[str]:
    """
    相互作用相手ラベルのノイズ除去:
      - 前後の空白・記号を除去
      - 単独の「等」「など」だけのエントリは捨てる
      - 末尾の「等」「など」を削る（○○等 → ○○）
      - 先頭の「等」「など」を削る（等セイヨウオトギリソウ… → セイヨウオトギリソウ…）
    """
    if not s:
        return None
    # 前後の空白・読点など軽く掃除
    s = s.strip()
    s = re.sub(r'^[、，,\s]+', '', s)
    s = re.sub(r'[、，,\s]+$', '', s)

    if not s:
        return None

    # 単独の「等」「など」はノイズなので捨てる
    if s in ("等", "など"):
        return None

    # 末尾の「等」「など」を削る
    s = re.sub(r'(等|など)$', '', s).strip()
    # 先頭の「等」「など」を削る
    s = re.sub(r'^(等|など)', '', s).strip()

    if not s:
        return None

    return s


def detail_text_full(detail_el: Optional[ET.Element]) -> Optional[str]:
    """
    Detail 要素から Lang.itertext() を優先して全文取得し、
    正規化（等/などの除去など）まで行う。
    """
    if detail_el is None:
        return None
    s = lang_text_all(detail_el)
    if not s:
        s = text_ja(detail_el) or get_text_direct(detail_el)
    if not s:
        return None
    s = s.strip()
    return normalize_partner_label(s)


# ---------- DrugName から「クラス」と「個別成分」を抽出 ----------
def extract_partner_group_and_items(drug_elem: ET.Element) -> Tuple[Optional[str], List[str]]:
    """
    <Drug> 要素から (class_label, items) を抽出する。

    class_label:
      - <DrugName><Detail> に書かれているクラス/総称
        例: 「強い又は中程度のCYP3A阻害剤」「抗コリン剤」
    items:
      - <DrugName><SimpleList><Item><Detail> に書かれている個別成分名
        例: ["イトラコナゾール", "クラリスロマイシン", ...]
    """
    dn = drug_elem.find("pi:DrugName", NS)
    if dn is None:
        return None, []

    class_label: Optional[str] = None
    items: List[str] = []

    # クラス名候補: DrugName直下の Detail
    for det in dn.findall("pi:Detail", NS):
        s = detail_text_full(det)
        if s:
            class_label = s
            break  # 最初のものを代表とみなす

    # 個別成分: SimpleList 配下
    for it in dn.findall("pi:SimpleList/pi:Item/pi:Detail", NS):
        s = detail_text_full(it)
        if s:
            items.append(s)

    # 重複除去
    seen = set()
    uniq_items: List[str] = []
    for n in items:
        if n not in seen:
            seen.add(n)
            uniq_items.append(n)

    return class_label, uniq_items


# -------- XML要素→JSON（ロス少なめ汎用変換） --------
def elem_to_json(el: Optional[ET.Element]):
    """属性・テキスト・子要素・tail をできるだけ保持する簡易シリアライザ"""
    if el is None:
        return None
    obj: Dict[str, object] = {
        "tag": el.tag,  # 例: {URI}LocalName
    }
    if el.attrib:
        obj["attr"] = dict(el.attrib)
    if el.text and el.text.strip():
        obj["text"] = el.text.strip()

    children: List[object] = []
    for ch in list(el):
        children.append(elem_to_json(ch))
        if ch.tail and ch.tail.strip():
            children.append({"tag": "__tail__", "text": ch.tail.strip()})
    if children:
        obj["children"] = children
    return obj


# -------- 相互作用（禁忌/注意/要約）抽出 --------
def collect_interactions_flat(root: ET.Element) -> Dict[str, object]:
    """
    returns:
      {
        "summary": [..text..],  # SummaryOfCombination の日本語テキスト配列
        "flat": [
            {
              "partner": "...",         # 個別成分名 or クラス名
              "group":   "...",         # クラス名（ある場合）
              "symptoms": "...",
              "mechanism": "...",
              "category": "併用禁忌|併用注意"
            },
            ...
        ]
      }
    """
    summary_parts: List[str] = []
    for det in root.findall("pi:Interactions/pi:SummaryOfCombination//pi:Detail", NS):
        s = lang_text_all(det) or text_ja(det) or get_text_direct(det)
        if s:
            summary_parts.append(s)

    flat: List[Dict[str, Optional[str]]] = []

    # 共通: symptoms/mechanism 抽出ヘルパ
    def get_symptoms_and_mechanism(drug: ET.Element) -> Tuple[Optional[str], Optional[str]]:
        symptoms  = lang_text_all(drug.find("pi:ClinSymptomsAndMeasures/pi:Detail", NS)) \
                    or text_ja(drug.find("pi:ClinSymptomsAndMeasures/pi:Detail", NS)) \
                    or get_text(drug, "pi:ClinSymptomsAndMeasures")
        mechanism = lang_text_all(drug.find("pi:MechanismAndRiskFactors/pi:Detail", NS)) \
                    or text_ja(drug.find("pi:MechanismAndRiskFactors/pi:Detail", NS)) \
                    or get_text(drug, "pi:MechanismAndRiskFactors")
        return symptoms, mechanism

    # 10.1 併用禁忌
    for drug in root.findall("pi:Interactions/pi:ContraIndicatedCombinations//pi:Drug", NS):
        group_label, items = extract_partner_group_and_items(drug)
        symptoms, mechanism = get_symptoms_and_mechanism(drug)

        if items:
            for p in items:
                flat.append({
                    "partner":   p,
                    "group":     group_label,
                    "symptoms":  symptoms,
                    "mechanism": mechanism,
                    "category":  "併用禁忌",
                })
        elif group_label:
            flat.append({
                "partner":   group_label,
                "group":     group_label,
                "symptoms":  symptoms,
                "mechanism": mechanism,
                "category":  "併用禁忌",
            })

    # 10.2 併用注意
    for drug in root.findall("pi:Interactions/pi:PrecautionsForCombinations//pi:Drug", NS):
        group_label, items = extract_partner_group_and_items(drug)
        symptoms, mechanism = get_symptoms_and_mechanism(drug)

        if items:
            for p in items:
                flat.append({
                    "partner":   p,
                    "group":     group_label,
                    "symptoms":  symptoms,
                    "mechanism": mechanism,
                    "category":  "併用注意",
                })
        elif group_label:
            flat.append({
                "partner":   group_label,
                "group":     group_label,
                "symptoms":  symptoms,
                "mechanism": mechanism,
                "category":  "併用注意",
            })

    return {"summary": summary_parts, "flat": flat}


# -------- 第1層：効能／用量（任意で列にも使う、今回はJSON併記が主） --------
def collect_indications_json(root: ET.Element):
    return elem_to_json(root.find("pi:IndicationsOrEfficacy", NS))


def collect_info_dose_admin_json(root: ET.Element):
    return elem_to_json(root.find("pi:InfoDoseAdmin", NS))


# ===================== XML → 行化 =====================
def parse_xml_to_rows(xml_path: str) -> List[Dict]:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # 第1層（列用）
    pkg      = get_text(root, "pi:PackageInsertNo") or ""
    company  = get_text(root, "pi:CompanyIdentifier")
    prepared = get_text(root, "pi:DateOfPreparationOrRevision/pi:PreparationOrRevision/pi:YearMonth")

    generic     = text_ja(root.find("pi:GenericName/pi:Detail", NS)) or get_text(root, "pi:GenericName")
    therapeutic = text_ja(root.find("pi:TherapeuticClassification/pi:Detail", NS)) or get_text(root, "pi:TherapeuticClassification")

    # JSONBセクション
    approval_etc_json      = elem_to_json(root.find("pi:ApprovalEtc", NS))
    indications_json       = collect_indications_json(root)
    info_dose_admin_json   = collect_info_dose_admin_json(root)
    interactions_json      = elem_to_json(root.find("pi:Interactions", NS))
    adverse_reactions_json = elem_to_json(root.find("pi:AdverseReactions", NS))  # あれば
    composition_json       = elem_to_json(root.find("pi:Composition", NS))
    property_json          = elem_to_json(root.find("pi:Properties", NS))

    # 全文XML（整形して保存）
    doc_xml = ET.tostring(root, encoding="unicode")

    # 平坦化相互作用
    inter_flat = collect_interactions_flat(root)

    rows: List[Dict] = []
    brands = root.findall("pi:ApprovalEtc/pi:DetailBrandName", NS)

    # ブランドが無い文書 → yj_code="" で1行
    if not brands:
        rows.append({
            "package_insert_no": pkg,
            "yj_code": "",
            "company_identifier": company,
            "prepared_ym": prepared,
            "brand_name_ja": None,
            "brand_name_hiragana": None,
            "trademark_en": None,
            "generic_name_ja": generic,
            "standard_name_ja": None,
            "therapeutic_class_ja": therapeutic,
            "approval_no": None,
            "start_marketing": None,

            "approval_etc_json": psycopg2.extras.Json(approval_etc_json),
            "indications_json": psycopg2.extras.Json(indications_json),
            "info_dose_admin_json": psycopg2.extras.Json(info_dose_admin_json),
            "interactions_json": psycopg2.extras.Json(interactions_json),
            "adverse_reactions_json": psycopg2.extras.Json(adverse_reactions_json),
            "composition_json": psycopg2.extras.Json(composition_json),
            "property_json": psycopg2.extras.Json(property_json),

            "interactions_flat": psycopg2.extras.Json(inter_flat["flat"]),
            "doc_xml": doc_xml,

            "raw_xml_path": xml_path
        })
        return rows

    # ブランドあり → ブランド毎に1行
    for br in brands:
        yj = get_text(br, "pi:BrandCode/pi:YJCode") or ""

        brand_name_ja   = text_ja(br.find("pi:ApprovalBrandName", NS)) or get_text(br, "pi:ApprovalBrandName")
        brand_hira      = get_text(br, "pi:BrandNameInHiragana/pi:NameInHiragana")
        trademark_en    = get_text(br, "pi:TrademarkInEnglish/pi:TrademarkName")
        approval_no     = get_text(br, "pi:ApprovalAndLicenseNo/pi:ApprovalNo")
        start_marketing = get_text(br, "pi:StartingDateOfMarketing")
        standard_name   = text_ja(br.find("pi:StandardName/pi:StandardNameCategory/pi:StandardNameDetail", NS)) or get_text(br, "pi:StandardName")
        storage_method  = text_ja(br.find("pi:Storage/pi:StorageMethod", NS)) or get_text(br, "pi:Storage/pi:StorageMethod")
        shelf_life      = text_ja(br.find("pi:Storage/pi:ShelfLife", NS)) or get_text(br, "pi:Storage/pi:ShelfLife")

        rows.append({
            "package_insert_no": pkg,
            "yj_code": yj,
            "company_identifier": company,
            "prepared_ym": prepared,
            "brand_name_ja": brand_name_ja,
            "brand_name_hiragana": brand_hira,
            "trademark_en": trademark_en,
            "generic_name_ja": generic,
            "standard_name_ja": standard_name,
            "therapeutic_class_ja": therapeutic,
            "approval_no": approval_no,
            "start_marketing": start_marketing,

            "approval_etc_json": psycopg2.extras.Json(approval_etc_json),
            "indications_json": psycopg2.extras.Json(indications_json),
            "info_dose_admin_json": psycopg2.extras.Json(info_dose_admin_json),
            "interactions_json": psycopg2.extras.Json(interactions_json),
            "adverse_reactions_json": psycopg2.extras.Json(adverse_reactions_json),
            "composition_json": psycopg2.extras.Json(composition_json),
            "property_json": psycopg2.extras.Json(property_json),

            "interactions_flat": psycopg2.extras.Json(inter_flat["flat"]),
            "doc_xml": doc_xml,

            "raw_xml_path": xml_path
        })
    return rows


# ===================== DB =====================
CREATE_TABLE_SQL = f"""
-- 既存を落として作り直し（再取り込み前提）
DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;

CREATE TABLE {TABLE_NAME} (
  -- 第1層（キー・代表値）
  package_insert_no     text NOT NULL,
  yj_code               text NOT NULL,
  company_identifier    text,
  prepared_ym           text,
  brand_name_ja         text,
  brand_name_hiragana   text,
  trademark_en          text,
  generic_name_ja       text,
  standard_name_ja      text,
  therapeutic_class_ja  text,
  approval_no           text,
  start_marketing       text,

  -- 主要セクション（第2層以降）
  approval_etc_json         jsonb,
  indications_json          jsonb,
  info_dose_admin_json      jsonb,
  interactions_json         jsonb,
  adverse_reactions_json    jsonb,
  composition_json          jsonb,
  property_json             jsonb,

  -- 相互作用（平坦化）
  interactions_flat         jsonb,

  -- 全文
  doc_xml                   xml,

  raw_xml_path          text,
  updated_at            timestamptz DEFAULT now(),

  CONSTRAINT {TABLE_BASENAME}_pkey PRIMARY KEY (package_insert_no, yj_code)
);

-- index名が重複しないよう、テーブルのベース名を含める
CREATE INDEX IF NOT EXISTS idx_{TABLE_BASENAME}_pkg_no
  ON {TABLE_NAME} (package_insert_no);

CREATE INDEX IF NOT EXISTS idx_{TABLE_BASENAME}_yj
  ON {TABLE_NAME} (yj_code);

CREATE INDEX IF NOT EXISTS idx_{TABLE_BASENAME}_inter_json_gin
  ON {TABLE_NAME} USING gin (interactions_json);

CREATE INDEX IF NOT EXISTS idx_{TABLE_BASENAME}_inter_flat_gin
  ON {TABLE_NAME} USING gin (interactions_flat);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME}
(package_insert_no, yj_code, company_identifier, prepared_ym,
 brand_name_ja, brand_name_hiragana, trademark_en,
 generic_name_ja, standard_name_ja, therapeutic_class_ja,
 approval_no, start_marketing,
 approval_etc_json, indications_json, info_dose_admin_json,
 interactions_json, adverse_reactions_json, composition_json, property_json,
 interactions_flat, doc_xml,
 raw_xml_path, updated_at)
VALUES
(%(package_insert_no)s, %(yj_code)s, %(company_identifier)s, %(prepared_ym)s,
 %(brand_name_ja)s, %(brand_name_hiragana)s, %(trademark_en)s,
 %(generic_name_ja)s, %(standard_name_ja)s, %(therapeutic_class_ja)s,
 %(approval_no)s, %(start_marketing)s,
 %(approval_etc_json)s, %(indications_json)s, %(info_dose_admin_json)s,
 %(interactions_json)s, %(adverse_reactions_json)s, %(composition_json)s, %(property_json)s,
 %(interactions_flat)s, %(doc_xml)s,
 %(raw_xml_path)s, now())
ON CONFLICT (package_insert_no, yj_code) DO UPDATE SET
 company_identifier     = EXCLUDED.company_identifier,
 prepared_ym            = EXCLUDED.prepared_ym,
 brand_name_ja          = EXCLUDED.brand_name_ja,
 brand_name_hiragana    = EXCLUDED.brand_name_hiragana,
 trademark_en           = EXCLUDED.trademark_en,
 generic_name_ja        = EXCLUDED.generic_name_ja,
 standard_name_ja       = EXCLUDED.standard_name_ja,
 therapeutic_class_ja   = EXCLUDED.therapeutic_class_ja,
 approval_no            = EXCLUDED.approval_no,
 start_marketing        = EXCLUDED.start_marketing,
 approval_etc_json      = EXCLUDED.approval_etc_json,
 indications_json       = EXCLUDED.indications_json,
 info_dose_admin_json   = EXCLUDED.info_dose_admin_json,
 interactions_json      = EXCLUDED.interactions_json,
 adverse_reactions_json = EXCLUDED.adverse_reactions_json,
 composition_json       = EXCLUDED.composition_json,
 property_json          = EXCLUDED.property_json,
 interactions_flat      = EXCLUDED.interactions_flat,
 doc_xml                = EXCLUDED.doc_xml,
 raw_xml_path           = EXCLUDED.raw_xml_path,
 updated_at             = now();
"""


def ensure_table(conn):
    logging.info(f"DROP -> CREATE {TABLE_NAME} を実行します（既存データは消えます）")
    with conn, conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)


def upsert_rows(conn, rows: List[Dict]) -> int:
    if not rows:
        return 0
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=300)
    return len(rows)


# ===================== 進捗支援 =====================
def iter_xml_files(root_dir: str) -> List[str]:
    pattern = os.path.join(root_dir, "**", "*.xml")
    return glob.glob(pattern, recursive=True)


def fmt_eta(seconds: float) -> str:
    if seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:d}h{m:02d}m{s:02d}s"
    return f"{m:02d}m{s:02d}s"


# ===================== メイン =====================
def main():
    if not os.path.isdir(XML_ROOT):
        logging.error(f"DI_folder が見つかりません: {XML_ROOT}")
        raise SystemExit(1)

    # ★★★ ここで ZIP を全部展開する ★★★
    logging.info(f"Extracting ZIP files under {XML_ROOT} ...")
    extract_all_zips(XML_ROOT)
    logging.info("ZIP extraction completed.")
    
    files = iter_xml_files(XML_ROOT)
    total = len(files)
    if total == 0:
        logging.warning(f"XMLが見つかりませんでした: {XML_ROOT}")
        return

    logging.info(f"Start import: root={XML_ROOT}, total_xml={total}")
    with open(FAILED_CSV, "w", encoding="utf-8") as wf:
        wf.write("file,error,exception,seconds\n")

    conn = psycopg2.connect(**db_conf)
    t0 = time.time()
    try:
        ensure_table(conn)

        done = 0
        ok_rows_total = 0
        err_count = 0
        max_sec: Tuple[float, str] = (0.0, "")
        sum_sec = 0.0

        for idx, xml_path in enumerate(files, start=1):
            start = time.time()
            try:
                rows = parse_xml_to_rows(xml_path)
                inserted = upsert_rows(conn, rows)
                elapsed = time.time() - start

                done += 1
                ok_rows_total += inserted
                sum_sec += elapsed
                if elapsed > max_sec[0]:
                    max_sec = (elapsed, xml_path)

                total_elapsed = time.time() - t0
                rate = done / total if total else 1.0
                eta = (total_elapsed / rate) - total_elapsed if rate > 0 else -1

                progress = (
                    f"[{done}/{total} {rate*100:5.1f}% ETA:{fmt_eta(eta)}] "
                    f"{os.path.basename(xml_path)} rows={inserted} time={elapsed:.2f}s"
                )
                # コンソールは1行上書き
                print("\r" + progress, end="", flush=True)
                # ログファイルには詳細を残したければ debug などで
                logging.debug(progress)

            except Exception as e:
                elapsed = time.time() - start
                err_count += 1
                logging.exception(f"[ERROR] {xml_path}: {e}")
                with open(FAILED_CSV, "a", encoding="utf-8") as wf:
                    esc_err = str(e).replace("\n", " ").replace(",", "，")
                    wf.write(f"{xml_path},{esc_err},{e.__class__.__name__},{elapsed:.2f}\n")

                total_elapsed = time.time() - t0
                rate = (idx) / total if total else 1.0
                eta = (total_elapsed / rate) - total_elapsed if rate > 0 else -1
                progress = (
                    f"[{idx}/{total} {rate*100:5.1f}% ETA:{fmt_eta(eta)}] "
                    f"{os.path.basename(xml_path)} ERROR time={elapsed:.2f}s"
                )
                print("\r" + progress, end="", flush=True)
                logging.debug(progress)

        total_time = time.time() - t0
        avg_sec = (sum_sec / done) if done else 0.0
        logging.info(
            "===== SUMMARY =====\n"
            f"root: {XML_ROOT}\n"
            f"total xml: {total}\n"
            f"success files: {done - err_count}\n"
            f"errors: {err_count}\n"
            f"total rows inserted/updated: {ok_rows_total}\n"
            f"total time: {total_time:.2f}s\n"
            f"avg per file: {avg_sec:.2f}s\n"
            f"slowest: {max_sec[0]:.2f}s -> {os.path.basename(max_sec[1]) if max_sec[1] else ''}\n"
            f"failed csv: {FAILED_CSV}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
