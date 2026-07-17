#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
temp_sgml_pk_blocks をOllamaで解析し、薬物動態ファクトをDB化する。

- LLMの呼び出し・応答・検証結果は temp_* テーブルへ保存する。
- 配布対象は sgml_pharmacokinetics（設定で変更可）のみ。
- OQSDrugアプリケーションが利用する ai_* テーブルには触れない。
- 実際にLLMへリクエストした後は、成功・失敗を問わず指定秒数待機する。
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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

PROMPT_VERSION_DEFAULT = "pk-feature-v2"
QUALIFIED_NAME_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")

FEATURE_TYPES = {"ENZYME", "TRANSPORTER", "METABOLITE", "EXCRETION", "ORGAN_IMPAIRMENT"}
RELATION_TYPES = {
    "METABOLIZED_BY",
    "SUBSTRATE_OF",
    "INHIBITS",
    "INDUCES",
    "NOT_METABOLIZED_BY",
    "NOT_SUBSTRATE_OF",
    "NOT_INHIBITS",
    "NOT_INDUCES",
    "HAS_METABOLITE",
    "EXCRETED_IN",
    "EXPOSURE_INCREASES_WITH_IMPAIRMENT",
    "EXPOSURE_DECREASES_WITH_IMPAIRMENT",
    "EXPOSURE_INCREASED_BY_INHIBITION",
    "EXPOSURE_DECREASED_BY_INDUCTION",
    "OTHER",
}
STRENGTHS = {"UNKNOWN", "WEAK", "MODERATE", "STRONG", "PRIMARY"}
POLARITIES = {"POSITIVE", "NEGATIVE", "UNCERTAIN"}
EVIDENCE_LEVELS = {"IN_VITRO", "CLINICAL", "LABEL_STATEMENT", "UNKNOWN"}
ASSERTION_TYPES = {"EXPLICIT", "INFERRED_FROM_CONTEXT"}
RELATIONS_BY_FEATURE = {
    "ENZYME": {
        "METABOLIZED_BY", "SUBSTRATE_OF", "INHIBITS", "INDUCES",
        "NOT_METABOLIZED_BY", "NOT_SUBSTRATE_OF", "NOT_INHIBITS", "NOT_INDUCES",
        "EXPOSURE_INCREASED_BY_INHIBITION", "EXPOSURE_DECREASED_BY_INDUCTION", "OTHER",
    },
    "TRANSPORTER": {
        "SUBSTRATE_OF", "INHIBITS", "INDUCES",
        "NOT_SUBSTRATE_OF", "NOT_INHIBITS", "NOT_INDUCES",
        "EXPOSURE_INCREASED_BY_INHIBITION", "EXPOSURE_DECREASED_BY_INDUCTION", "OTHER",
    },
    "METABOLITE": {"HAS_METABOLITE", "OTHER"},
    "EXCRETION": {"EXCRETED_IN", "OTHER"},
    "ORGAN_IMPAIRMENT": {
        "EXPOSURE_INCREASES_WITH_IMPAIRMENT", "EXPOSURE_DECREASES_WITH_IMPAIRMENT", "OTHER",
    },
}


SYSTEM_INSTRUCTIONS = """あなたは日本の医療用医薬品添付文書の薬物動態記載を構造化する抽出器です。
入力は16章の一部分です。医学的助言や一般知識の補完は行わず、入力文に記載または明確に含意された事実だけを抽出してください。

最優先で抽出する対象:
- CYP、UGT等の代謝酵素による代謝、基質、阻害、誘導とその否定
- P-gp、BCRP、OATP、OAT、OCT、MATE等のトランスポーターの基質、阻害、誘導とその否定
- 主代謝物、活性代謝物
- 尿、糞、胆汁等への排泄と、未変化体か代謝物を含むか
- 腎機能・肝機能低下による曝露量または半減期の変化

注意:
- Cmax、AUC等の一般的な数値表は、上記対象に直接必要な場合以外は抽出しない。
- 併用薬だけの性質を「本剤」の性質として抽出しない。
- 「本剤が酵素を阻害する」と「本剤の代謝が阻害される」を区別する。
- 「代謝が促進される」だけでは、特定酵素の誘導と断定しない。
- 強い・中程度・弱いが明記されていない場合、strengthはUNKNOWNとする。
- METABOLITEのrelation_typeはHAS_METABOLITE、EXCRETIONはEXCRETED_IN、ORGAN_IMPAIRMENTは
  EXPOSURE_INCREASES_WITH_IMPAIRMENTまたはEXPOSURE_DECREASES_WITH_IMPAIRMENTだけを用いる。
- 吸収率、組織分布、蛋白結合だけの記述は今回の抽出対象外とし、無理に5種類へ分類しない。
- evidence_textには入力から改変しない連続した原文を短く引用する。
- 該当事実がなければ facts を空配列にする。

JSONのみを返し、次の形式に厳密に従ってください。
{
  "facts": [
    {
      "feature_type": "ENZYME|TRANSPORTER|METABOLITE|EXCRETION|ORGAN_IMPAIRMENT",
      "target_name": "原文に基づく対象名",
      "target_code": "CYP3A4、P-gp等の正規化名。不明ならnull",
      "relation_type": "METABOLIZED_BY|SUBSTRATE_OF|INHIBITS|INDUCES|NOT_METABOLIZED_BY|NOT_SUBSTRATE_OF|NOT_INHIBITS|NOT_INDUCES|HAS_METABOLITE|EXCRETED_IN|EXPOSURE_INCREASES_WITH_IMPAIRMENT|EXPOSURE_DECREASES_WITH_IMPAIRMENT|EXPOSURE_INCREASED_BY_INHIBITION|EXPOSURE_DECREASED_BY_INDUCTION|OTHER",
      "strength": "UNKNOWN|WEAK|MODERATE|STRONG|PRIMARY",
      "polarity": "POSITIVE|NEGATIVE|UNCERTAIN",
      "evidence_level": "IN_VITRO|CLINICAL|LABEL_STATEMENT|UNKNOWN",
      "assertion_type": "EXPLICIT|INFERRED_FROM_CONTEXT",
      "evidence_text": "入力中の連続した原文",
      "qualifiers": {},
      "confidence": 0.0
    }
  ]
}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="16章チャンクをLLMで解析してsgmlテーブルを構築")
    parser.add_argument("--config", default="config.json", help="設定JSON（既定: config.json）")
    parser.add_argument("--limit", type=int, help="今回新規にLLMへ送信するチャンク数の上限")
    parser.add_argument("--package-insert-no", help="指定添付文書のチャンクだけを処理")
    parser.add_argument("--wait-seconds", type=float, help="各LLMリクエスト後のGPU冷却待機秒数")
    parser.add_argument("--max-retries", type=int, help="JSON不正・通信失敗時の再試行回数")
    parser.add_argument("--model", help="config.jsonのpk_ollama_modelを一時的に上書き")
    parser.add_argument("--prompt-version", help="プロンプト版を一時的に上書き")
    parser.add_argument("--force", action="store_true", help="成功済みキャッシュも再実行")
    parser.add_argument("--no-publish", action="store_true", help="最終sgmlテーブルを更新しない")
    parser.add_argument(
        "--publish-partial",
        action="store_true",
        help="未処理・エラーが残っていても最終sgmlテーブルを更新する",
    )
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def checked_table_name(value: str, setting_name: str) -> str:
    if not QUALIFIED_NAME_RX.fullmatch(value):
        raise ValueError(f"{setting_name} は schema.table 形式で指定してください: {value!r}")
    return value


def normalize_text(value: str) -> str:
    value = value.replace("\r\n", "\n").replace("\r", "\n").replace("\u3000", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\s*\n\s*", "\n", value)
    return value.strip()


def json_from_model_text(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        value = json.loads(text[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("LLM応答のルートがJSONオブジェクトではありません")
    return value


def normalize_target_code(target_name: str, proposed: Optional[str]) -> Optional[str]:
    source = f"{proposed or ''} {target_name}".upper().replace(" ", "")
    source = source.replace("ＣＹＰ", "CYP")
    match = re.search(r"CYP(?:1A2|2A6|2B6|2C8|2C9|2C19|2D6|2E1|3A4|3A5|3A7|3A)", source)
    if match:
        return match.group(0)
    match = re.search(r"UGT(?:1A1|1A3|1A4|1A6|1A9|1A10|2B4|2B7|2B10|2B15|2B17)", source)
    if match:
        return match.group(0)
    aliases = {
        "P-GLYCOPROTEIN": "P-gp",
        "PGLYCOPROTEIN": "P-gp",
        "P-糖蛋白": "P-gp",
        "P糖蛋白": "P-gp",
        "P-GP": "P-gp",
        "PGP": "P-gp",
        "BCRP": "BCRP",
        "OATP1B1": "OATP1B1",
        "OATP1B3": "OATP1B3",
        "OATP2B1": "OATP2B1",
        "OAT1": "OAT1",
        "OAT3": "OAT3",
        "OCT1": "OCT1",
        "OCT2": "OCT2",
        "MATE1": "MATE1",
        "MATE2-K": "MATE2-K",
        "MATE2K": "MATE2-K",
    }
    for alias, canonical in aliases.items():
        if alias in source:
            return canonical
    return proposed.strip() if isinstance(proposed, str) and proposed.strip() else None


def validate_response(parsed: dict, block_text: str) -> Tuple[List[dict], List[str]]:
    facts = parsed.get("facts")
    if not isinstance(facts, list):
        return [], ["factsが配列ではありません"]

    valid: List[dict] = []
    errors: List[str] = []
    normalized_block = normalize_text(block_text)
    for index, raw in enumerate(facts):
        prefix = f"facts[{index}]"
        if not isinstance(raw, dict):
            errors.append(f"{prefix}: オブジェクトではありません")
            continue

        feature_type = str(raw.get("feature_type", "")).upper()
        relation_type = str(raw.get("relation_type", "")).upper()
        strength = str(raw.get("strength", "UNKNOWN")).upper()
        polarity = str(raw.get("polarity", "UNCERTAIN")).upper()
        evidence_level = str(raw.get("evidence_level", "UNKNOWN")).upper()
        assertion_type = str(raw.get("assertion_type", "EXPLICIT")).upper()
        target_name = str(raw.get("target_name", "")).strip()
        evidence_text = normalize_text(str(raw.get("evidence_text", "")))

        field_errors = []
        if feature_type not in FEATURE_TYPES:
            field_errors.append(f"feature_type={feature_type!r}")
        if relation_type not in RELATION_TYPES:
            field_errors.append(f"relation_type={relation_type!r}")
        elif feature_type in RELATIONS_BY_FEATURE and relation_type not in RELATIONS_BY_FEATURE[feature_type]:
            field_errors.append(
                f"{feature_type}とrelation_type={relation_type!r}の組み合わせが不正です"
            )
        if strength not in STRENGTHS:
            field_errors.append(f"strength={strength!r}")
        if polarity not in POLARITIES:
            field_errors.append(f"polarity={polarity!r}")
        if evidence_level not in EVIDENCE_LEVELS:
            field_errors.append(f"evidence_level={evidence_level!r}")
        if assertion_type not in ASSERTION_TYPES:
            field_errors.append(f"assertion_type={assertion_type!r}")
        if not target_name:
            field_errors.append("target_nameが空です")
        if not evidence_text or evidence_text not in normalized_block:
            field_errors.append("evidence_textが入力原文の連続部分と一致しません")

        target_code = normalize_target_code(target_name, raw.get("target_code"))
        if feature_type in {"ENZYME", "TRANSPORTER"} and not target_code:
            field_errors.append("酵素・トランスポーターのtarget_codeを正規化できません")
        strength_terms = {
            "WEAK": ("弱い", "弱く", "weak"),
            "MODERATE": ("中程度", "moderate"),
            "STRONG": ("強い", "強く", "strong"),
            "PRIMARY": ("主として", "主に", "主要", "primary"),
        }
        if strength in strength_terms and not any(
            term.lower() in evidence_text.lower() for term in strength_terms[strength]
        ):
            field_errors.append(f"strength={strength!r}を裏付ける表現が根拠文にありません")

        qualifiers = raw.get("qualifiers")
        if not isinstance(qualifiers, dict):
            qualifiers = {"raw_qualifiers": qualifiers}
        try:
            confidence = float(raw.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
            field_errors.append("confidenceが数値ではありません")
        confidence = min(1.0, max(0.0, confidence))

        if field_errors:
            errors.append(f"{prefix}: " + "; ".join(field_errors))
            continue

        valid.append(
            {
                "feature_type": feature_type,
                "target_name": target_name,
                "target_code": target_code,
                "relation_type": relation_type,
                "strength": strength,
                "polarity": polarity,
                "evidence_level": evidence_level,
                "assertion_type": assertion_type,
                "evidence_text": evidence_text,
                "qualifiers": qualifiers,
                "confidence": confidence,
            }
        )
    return valid, errors


def build_prompt(section_code: str, section_type: str, block_text: str) -> str:
    return (
        SYSTEM_INSTRUCTIONS
        + "\n\n【入力セクション】\n"
        + f"section_code: {section_code}\nsection_type: {section_type}\n"
        + "【入力本文】\n"
        + block_text
    )


def call_ollama(url: str, model: str, prompt: str, timeout: int) -> Tuple[str, dict]:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0},
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        outer = json.loads(response.read().decode("utf-8"))
    generated = outer.get("response")
    if generated is None and isinstance(outer.get("message"), dict):
        generated = outer["message"].get("content")
    if not isinstance(generated, str):
        raise ValueError("Ollama応答に response または message.content がありません")
    return generated, outer


def fact_hash(fact: dict) -> str:
    key = {
        "feature_type": fact["feature_type"],
        "target_code": fact.get("target_code"),
        "target_name": fact["target_name"],
        "relation_type": fact["relation_type"],
        "strength": fact["strength"],
        "polarity": fact["polarity"],
        "evidence_level": fact["evidence_level"],
        "assertion_type": fact["assertion_type"],
        "evidence_text": fact["evidence_text"],
        "qualifiers": fact["qualifiers"],
    }
    encoded = json.dumps(key, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def create_tables(
    conn,
    run_table: str,
    fact_table: str,
) -> None:
    run_base = run_table.split(".")[-1]
    fact_base = fact_table.split(".")[-1]
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {run_table} (
      run_id              bigserial PRIMARY KEY,
      content_hash        text NOT NULL,
      prompt_version      text NOT NULL,
      model_name          text NOT NULL,
      server_url          text NOT NULL,
      status              text NOT NULL,
      request_at          timestamptz,
      response_at         timestamptz,
      attempts            integer NOT NULL DEFAULT 0,
      raw_response        text,
      response_json       jsonb,
      validation_errors   jsonb,
      error_message       text,
      updated_at          timestamptz NOT NULL DEFAULT now(),
      UNIQUE (content_hash, prompt_version, model_name)
    );
    CREATE INDEX IF NOT EXISTS idx_{run_base}_status ON {run_table} (status);

    CREATE TABLE IF NOT EXISTS {fact_table} (
      temp_fact_id        bigserial PRIMARY KEY,
      package_insert_no   text NOT NULL,
      block_id            bigint NOT NULL,
      content_hash        text NOT NULL,
      section_code        text NOT NULL,
      section_type        text NOT NULL,
      feature_type        text NOT NULL,
      target_code         text,
      target_name         text NOT NULL,
      relation_type       text NOT NULL,
      strength            text NOT NULL,
      polarity            text NOT NULL,
      evidence_level      text NOT NULL,
      assertion_type      text NOT NULL,
      evidence_text       text NOT NULL,
      qualifiers_json     jsonb NOT NULL DEFAULT '{{}}'::jsonb,
      confidence          double precision NOT NULL,
      fact_hash           text NOT NULL,
      prompt_version      text NOT NULL,
      model_name          text NOT NULL,
      created_at          timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_{fact_base}_package ON {fact_table} (package_insert_no);
    CREATE INDEX IF NOT EXISTS idx_{fact_base}_target ON {fact_table} (target_code, relation_type);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def process_llm_calls(
    conn,
    block_table: str,
    run_table: str,
    prompt_version: str,
    model: str,
    url: str,
    timeout: int,
    wait_seconds: float,
    max_retries: int,
    force: bool,
    limit: Optional[int],
    package_insert_no: Optional[str],
) -> Tuple[int, int]:
    where = []
    params: List[Any] = []
    if package_insert_no:
        where.append("b.package_insert_no = %s")
        params.append(package_insert_no)
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
      SELECT DISTINCT ON (b.content_hash)
             b.content_hash, b.section_code, b.section_type, b.block_text,
             r.status AS cached_status
        FROM {block_table} b
        LEFT JOIN {run_table} r
          ON r.content_hash = b.content_hash
         AND r.prompt_version = %s
         AND r.model_name = %s
        {where_sql}
       ORDER BY b.content_hash
    """
    query_params = [prompt_version, model] + params
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, query_params)
        rows = cur.fetchall()

    called = 0
    succeeded = 0
    for row in rows:
        if not force and row["cached_status"] == "success":
            continue
        if limit is not None and called >= limit:
            break

        content_hash = row["content_hash"]
        prompt = build_prompt(row["section_code"], row["section_type"], row["block_text"])
        final_status = "error"
        final_raw: Optional[str] = None
        final_json: Optional[dict] = None
        final_errors: List[str] = []
        final_error_message: Optional[str] = None
        request_at = datetime.now(timezone.utc)
        response_at: Optional[datetime] = None
        call_attempts = 0

        for attempt in range(1, max_retries + 2):
            call_attempts += 1
            log.info(
                "LLM送信 hash=%s section=%s attempt=%s/%s",
                content_hash[:12],
                row["section_code"],
                attempt,
                max_retries + 1,
            )
            try:
                generated, _outer = call_ollama(url, model, prompt, timeout)
                final_raw = generated
                parsed = json_from_model_text(generated)
                valid_facts, validation_errors = validate_response(parsed, row["block_text"])
                final_json = {"facts": valid_facts}
                final_errors = validation_errors
                response_at = datetime.now(timezone.utc)
                if validation_errors:
                    final_status = "review"
                    final_error_message = "LLM応答に検証不合格のファクトがあります"
                    break
                else:
                    final_status = "success"
                    final_error_message = None
                    break
            except Exception as exc:
                response_at = datetime.now(timezone.utc)
                final_status = "error"
                final_error_message = f"{type(exc).__name__}: {exc}"
                log.warning("LLM処理失敗 hash=%s: %s", content_hash[:12], final_error_message)
            finally:
                if wait_seconds > 0:
                    log.info("GPU冷却待機 %.1f 秒", wait_seconds)
                    time.sleep(wait_seconds)

        upsert = f"""
          INSERT INTO {run_table}
          (content_hash, prompt_version, model_name, server_url, status,
           request_at, response_at, attempts, raw_response, response_json,
           validation_errors, error_message)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT (content_hash, prompt_version, model_name) DO UPDATE SET
            server_url = EXCLUDED.server_url,
            status = EXCLUDED.status,
            request_at = EXCLUDED.request_at,
            response_at = EXCLUDED.response_at,
            attempts = {run_table.split('.')[-1]}.attempts + EXCLUDED.attempts,
            raw_response = EXCLUDED.raw_response,
            response_json = EXCLUDED.response_json,
            validation_errors = EXCLUDED.validation_errors,
            error_message = EXCLUDED.error_message,
            updated_at = now()
        """
        with conn.cursor() as cur:
            cur.execute(
                upsert,
                (
                    content_hash,
                    prompt_version,
                    model,
                    url,
                    final_status,
                    request_at,
                    response_at,
                    call_attempts,
                    final_raw,
                    psycopg2.extras.Json(final_json) if final_json is not None else None,
                    psycopg2.extras.Json(final_errors),
                    final_error_message,
                ),
            )
        conn.commit()
        called += 1
        if final_status == "success":
            succeeded += 1

    return called, succeeded


def rebuild_temp_facts(
    conn,
    block_table: str,
    run_table: str,
    fact_table: str,
    prompt_version: str,
    model: str,
    batch_size: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {fact_table} RESTART IDENTITY")
    conn.commit()

    sql = f"""
      SELECT b.block_id, b.package_insert_no, b.content_hash,
             b.section_code, b.section_type, r.response_json
        FROM {block_table} b
        JOIN {run_table} r
          ON r.content_hash = b.content_hash
         AND r.prompt_version = %s
         AND r.model_name = %s
         AND r.status = 'success'
       ORDER BY b.block_id
    """
    insert_sql = f"""
      INSERT INTO {fact_table}
      (package_insert_no, block_id, content_hash, section_code, section_type,
       feature_type, target_code, target_name, relation_type, strength,
       polarity, evidence_level, assertion_type, evidence_text,
       qualifiers_json, confidence, fact_hash, prompt_version, model_name)
      VALUES %s
    """

    total = 0
    batch: List[Tuple] = []
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as scan:
        scan.execute(sql, (prompt_version, model))
        rows = scan.fetchall()
        with conn.cursor() as out:
            for row in rows:
                response_json = row["response_json"] or {}
                for fact in response_json.get("facts", []):
                    batch.append(
                        (
                            row["package_insert_no"],
                            row["block_id"],
                            row["content_hash"],
                            row["section_code"],
                            row["section_type"],
                            fact["feature_type"],
                            fact.get("target_code"),
                            fact["target_name"],
                            fact["relation_type"],
                            fact["strength"],
                            fact["polarity"],
                            fact["evidence_level"],
                            fact["assertion_type"],
                            fact["evidence_text"],
                            psycopg2.extras.Json(fact.get("qualifiers") or {}),
                            fact["confidence"],
                            fact_hash(fact),
                            prompt_version,
                            model,
                        )
                    )
                if len(batch) >= batch_size:
                    psycopg2.extras.execute_values(out, insert_sql, batch, page_size=batch_size)
                    conn.commit()
                    total += len(batch)
                    batch.clear()
            if batch:
                psycopg2.extras.execute_values(out, insert_sql, batch, page_size=batch_size)
                conn.commit()
                total += len(batch)
    return total


def coverage_counts(
    conn,
    block_table: str,
    run_table: str,
    prompt_version: str,
    model: str,
    package_insert_no: Optional[str] = None,
) -> Tuple[int, int, int, int]:
    hash_where = "WHERE package_insert_no = %s" if package_insert_no else ""
    params: List[Any] = [prompt_version, model]
    if package_insert_no:
        params = [package_insert_no, prompt_version, model]
    sql = f"""
      WITH hashes AS (
        SELECT DISTINCT content_hash FROM {block_table} {hash_where}
      ),
      states AS (
        SELECT h.content_hash, r.status
          FROM hashes h
          LEFT JOIN {run_table} r
            ON r.content_hash = h.content_hash
           AND r.prompt_version = %s
           AND r.model_name = %s
      )
      SELECT count(*),
             count(*) FILTER (WHERE status = 'success'),
             count(*) FILTER (WHERE status = 'review'),
             count(*) FILTER (WHERE status IS NULL OR status = 'error')
        FROM states
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return tuple(cur.fetchone())


def publish_final(
    conn,
    src_table: str,
    fact_table: str,
    final_table: str,
) -> int:
    final_base = final_table.split(".")[-1]
    sql = f"""
      CREATE TABLE IF NOT EXISTS {final_table} (
        id                  bigserial PRIMARY KEY,
        package_insert_no   text NOT NULL,
        yj_code             text NOT NULL,
        prepared_ym         text,
        generic_name_ja     text,
        brand_name_ja       text,
        section_code        text NOT NULL,
        section_type        text NOT NULL,
        feature_type        text NOT NULL,
        target_code         text,
        target_name         text NOT NULL,
        relation_type       text NOT NULL,
        strength            text NOT NULL,
        polarity            text NOT NULL,
        evidence_level      text NOT NULL,
        assertion_type      text NOT NULL,
        evidence_text       text NOT NULL,
        qualifiers_json     jsonb NOT NULL DEFAULT '{{}}'::jsonb,
        confidence          double precision NOT NULL,
        fact_hash           text NOT NULL,
        prompt_version      text NOT NULL,
        model_name          text NOT NULL,
        created_at          timestamptz NOT NULL DEFAULT now(),
        UNIQUE (package_insert_no, yj_code, fact_hash)
      );
      CREATE INDEX IF NOT EXISTS idx_{final_base}_yj ON {final_table} (yj_code);
      CREATE INDEX IF NOT EXISTS idx_{final_base}_target ON {final_table} (target_code, relation_type);
      CREATE INDEX IF NOT EXISTS idx_{final_base}_feature ON {final_table} (feature_type);
      TRUNCATE TABLE {final_table} RESTART IDENTITY;
      WITH dedup AS (
        SELECT DISTINCT ON (package_insert_no, fact_hash)
               package_insert_no, section_code, section_type,
               feature_type, target_code, target_name, relation_type, strength,
               polarity, evidence_level, assertion_type, evidence_text,
               qualifiers_json, confidence, fact_hash, prompt_version, model_name
          FROM {fact_table}
         ORDER BY package_insert_no, fact_hash, confidence DESC
      )
      INSERT INTO {final_table}
      (package_insert_no, yj_code, prepared_ym, generic_name_ja, brand_name_ja,
       section_code, section_type, feature_type, target_code, target_name,
       relation_type, strength, polarity, evidence_level, assertion_type,
       evidence_text, qualifiers_json, confidence, fact_hash,
       prompt_version, model_name)
      SELECT r.package_insert_no, r.yj_code, r.prepared_ym,
             r.generic_name_ja, r.brand_name_ja,
             d.section_code, d.section_type, d.feature_type,
             d.target_code, d.target_name, d.relation_type, d.strength,
             d.polarity, d.evidence_level, d.assertion_type, d.evidence_text,
             d.qualifiers_json, d.confidence, d.fact_hash,
             d.prompt_version, d.model_name
        FROM dedup d
        JOIN {src_table} r ON r.package_insert_no = d.package_insert_no;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(f"SELECT count(*) FROM {final_table}")
        count = cur.fetchone()[0]
    conn.commit()
    return count


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
    run_table = checked_table_name(
        config.get("temp_sgml_pk_llm_runs_table", "public.temp_sgml_pk_llm_runs"),
        "temp_sgml_pk_llm_runs_table",
    )
    fact_table = checked_table_name(
        config.get("temp_sgml_pk_facts_table", "public.temp_sgml_pk_facts"),
        "temp_sgml_pk_facts_table",
    )
    final_table = checked_table_name(
        config.get("sgml_pharmacokinetics_table", "public.sgml_pharmacokinetics"),
        "sgml_pharmacokinetics_table",
    )

    model = args.model or config.get("pk_ollama_model", "gpt-oss:20b")
    url = config.get("pk_ollama_url", config.get("ollama_url", "http://localhost:11434/api/generate"))
    timeout = int(config.get("pk_ollama_timeout", 600))
    prompt_version = args.prompt_version or config.get("pk_prompt_version", PROMPT_VERSION_DEFAULT)
    wait_seconds = (
        args.wait_seconds
        if args.wait_seconds is not None
        else float(config.get("gpu_cooling_wait", 15))
    )
    max_retries = (
        args.max_retries
        if args.max_retries is not None
        else int(config.get("pk_llm_max_retries", 2))
    )
    batch_size = int(config.get("batch_size", 500))

    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit は1以上で指定してください")
    if wait_seconds < 0:
        raise ValueError("--wait-seconds は0以上で指定してください")
    if max_retries < 0:
        raise ValueError("--max-retries は0以上で指定してください")

    log.info(
        "開始 model=%s prompt=%s wait=%.1fs block=%s final=%s",
        model,
        prompt_version,
        wait_seconds,
        block_table,
        final_table,
    )

    conn = psycopg2.connect(**config["db"])
    conn.autocommit = False
    try:
        create_tables(conn, run_table, fact_table)
        called, succeeded = process_llm_calls(
            conn,
            block_table,
            run_table,
            prompt_version,
            model,
            url,
            timeout,
            wait_seconds,
            max_retries,
            args.force,
            args.limit,
            args.package_insert_no,
        )
        facts = rebuild_temp_facts(
            conn,
            block_table,
            run_table,
            fact_table,
            prompt_version,
            model,
            batch_size,
        )
        total, success, review, missing_or_error = coverage_counts(
            conn, block_table, run_table, prompt_version, model, args.package_insert_no
        )
        log.info(
            "LLM結果 calls=%s succeeded=%s unique_blocks=%s success=%s review=%s missing_or_error=%s facts=%s",
            called,
            succeeded,
            total,
            success,
            review,
            missing_or_error,
            facts,
        )

        complete = total > 0 and success == total
        if args.no_publish:
            log.info("--no-publish 指定のため最終テーブルは更新しません")
        elif (args.package_insert_no or args.limit is not None) and not args.publish_partial:
            log.info(
                "部分実行のため最終テーブルは更新しません。"
                "意図的に部分公開する場合だけ --publish-partial を指定してください。"
            )
        elif complete or args.publish_partial:
            published = publish_final(conn, src_table, fact_table, final_table)
            log.info("最終テーブル更新完了 table=%s rows=%s", final_table, f"{published:,}")
        else:
            log.warning(
                "未処理または要確認ブロックが残るため最終テーブルを更新しません。"
                "再実行するか、検証後に --publish-partial を明示してください。"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
