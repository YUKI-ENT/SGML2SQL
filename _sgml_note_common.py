#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""sgml_note パイプライン共通処理。単独実行はしない。"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


QUALIFIED_NAME_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_definitions(path: str) -> List[dict]:
    with open(path, "r", encoding="utf-8") as f:
        root = json.load(f)
    definitions = root.get("definitions")
    if not isinstance(definitions, list) or not definitions:
        raise ValueError("note definition の definitions は空でない配列にしてください")
    seen = set()
    for definition in definitions:
        note_type = definition.get("note_type")
        if not isinstance(note_type, str) or not note_type:
            raise ValueError("各 note definition に note_type が必要です")
        if note_type in seen:
            raise ValueError(f"note_type が重複しています: {note_type}")
        seen.add(note_type)
        if not isinstance(definition.get("definition_version"), str):
            raise ValueError(f"definition_version が必要です: {note_type}")
        if not isinstance(definition.get("allowed_relations"), list):
            raise ValueError(f"allowed_relations が必要です: {note_type}")
    return definitions


def definition_map(path: str) -> Dict[str, dict]:
    return {item["note_type"]: item for item in load_definitions(path)}


def checked_table_name(value: str, setting_name: str) -> str:
    if not QUALIFIED_NAME_RX.fullmatch(value):
        raise ValueError(f"{setting_name} は schema.table 形式で指定してください: {value!r}")
    return value


def table_base(value: str) -> str:
    return value.split(".")[-1]


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "")
    value = value.replace("\r\n", "\n").replace("\r", "\n").replace("\u3000", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r" *\n *", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_json_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_text(encoded)


def remove_json_trailing_commas(text: str) -> str:
    """JSON文字列の外側にある、配列・オブジェクト末尾のカンマだけを除去する。"""
    result: List[str] = []
    in_string = False
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if in_string:
            result.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            result.append(char)
            index += 1
            continue
        if char == ",":
            following = index + 1
            while following < len(text) and text[following].isspace():
                following += 1
            if following < len(text) and text[following] in "}]":
                index += 1
                continue
        result.append(char)
        index += 1
    return "".join(result)


def json_from_model_text(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    candidates = [text]
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start and (start != 0 or end != len(text) - 1):
        candidates.append(text[start : end + 1])
    parse_error: Optional[json.JSONDecodeError] = None
    value: Any = None
    parsed = False
    for candidate in candidates:
        for prepared in (candidate, remove_json_trailing_commas(candidate)):
            try:
                value = json.loads(prepared)
                parsed = True
                break
            except json.JSONDecodeError as exc:
                parse_error = exc
        if parsed:
            break
    if not parsed:
        if parse_error is not None:
            raise parse_error
        raise ValueError("LLM応答にJSONオブジェクトがありません")
    if not isinstance(value, dict):
        raise ValueError("LLM応答のルートがJSONオブジェクトではありません")
    return value


def call_ollama(url: str, model: str, prompt: str, timeout: int) -> Tuple[str, dict]:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "think": False,
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
    if not generated and isinstance(outer.get("message"), dict):
        generated = outer["message"].get("content")
    if not generated and isinstance(outer.get("thinking"), str):
        generated = outer["thinking"]
    if not isinstance(generated, str) or not generated.strip():
        raise ValueError("Ollama応答の response、message.content、thinking が空です")
    return generated, outer


def normalized_target_code(target_name: str, proposed: Any) -> Optional[str]:
    proposed_text = proposed.strip() if isinstance(proposed, str) else ""
    source = normalize_text(f"{proposed_text} {target_name}").upper().replace(" ", "")
    cyp = re.search(r"CYP(?:1A2|2A6|2B6|2C8|2C9|2C19|2D6|2E1|3A4|3A5|3A7|3A)", source)
    if cyp:
        return cyp.group(0)
    ugt = re.search(r"UGT(?:1A1|1A3|1A4|1A6|1A9|1A10|2B4|2B7|2B10|2B15|2B17)", source)
    if ugt:
        return ugt.group(0)
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
        "QTC": "QTc",
        "QT": "QT",
        "HERG": "hERG",
        "IKR": "IKr",
    }
    for alias, canonical in aliases.items():
        if alias in source:
            return canonical
    return proposed_text or None


def exact_or_whitespace_only_span(block_text: str, proposed: str) -> Optional[str]:
    """完全一致、又は空白差だけの引用を入力本文の正確な連続文字列へ戻す。"""
    if proposed in block_text:
        return proposed
    compact_block_chars: List[str] = []
    source_positions: List[int] = []
    for index, char in enumerate(block_text):
        if not char.isspace():
            compact_block_chars.append(char)
            source_positions.append(index)
    compact_block = "".join(compact_block_chars)
    compact_proposed = "".join(char for char in proposed if not char.isspace())
    if not compact_proposed:
        return None
    start = compact_block.find(compact_proposed)
    if start < 0:
        return None
    end = start + len(compact_proposed) - 1
    return block_text[source_positions[start] : source_positions[end] + 1]


def enclosing_sentence(block_text: str, span: str) -> str:
    start = block_text.find(span)
    if start < 0:
        return span
    end = start + len(span)
    previous = max(block_text.rfind("。", 0, start), block_text.rfind("\n", 0, start))
    following_period = block_text.find("。", end)
    following_newline = block_text.find("\n", end)
    following_candidates = [value for value in (following_period, following_newline) if value >= 0]
    following = min(following_candidates) if following_candidates else len(block_text) - 1
    if following < len(block_text) and block_text[following] == "。":
        following += 1
    return block_text[previous + 1 : following].strip()


def build_prompt(definition: dict, block: dict) -> str:
    allowed = "|".join(definition["allowed_relations"])
    return f"""あなたは日本の医療用医薬品添付文書から、根拠が明示された事実だけを抽出する抽出器です。
一般知識で補完せず、入力本文が直接支持する事実だけを抽出してください。
今回の対象薬は「{block.get('generic_name_ja') or '名称不明'}」です。
主語が併用薬、比較薬、代謝物又は一般論の場合、それを対象薬の事実として抽出しないでください。
表を平文化した入力では「本剤」が別の列に係る可能性があります。主語と対象薬の対応が曖昧なら抽出しないでください。
表の複数セルを並べ替えてevidence_textを作らず、本文中に連続して存在する記述だけを引用してください。
不明、推測が必要、主語不明の場合は facts を空配列にしてください。
note_text は根拠文の意味を狭めず、簡潔な日本語にしてください。
evidence_text は入力本文から改変しない連続した原文を引用してください。
1件に複数の主張を混ぜず、原子的な事実に分けてください。
ただし、「A及びBとしてX%」「尿中及び糞中に合計X%」のように、1つの数値が複数の成分・経路をまとめた合計値である場合は分割しないでください。
合計値を各成分又は各経路の個別値として複製してはいけません。合計対象をtarget_name又はnote_textにまとめ、1件の事実として抽出してください。
「それぞれ」と明記されていない限り、共有された1つの割合を列挙対象の各々に割り当てないでください。

【抽出テーマ】
{definition['note_type']}: {definition.get('description', '')}

【テーマ固有指示】
{definition.get('instructions', '')}

【許可される relation_type】
{allowed}

【入力位置】
section_code: {block.get('section_code') or ''}
section_type: {block.get('section_type') or ''}
heading_path: {block.get('heading_path') or ''}

【出力形式】
JSONのみを返してください。
{{
  "facts": [
    {{
      "relation_type": "上記の許可値",
      "subject_type": "DRUG",
      "target_code": "正規化可能な対象コード。不明又は不要ならnull",
      "target_name": "対象名。不要なら空文字",
      "polarity": "POSITIVE|NEGATIVE",
      "certainty": "EXPLICIT",
      "note_text": "表示用の簡潔なノート",
      "details": {{}},
      "evidence_text": "入力本文中の連続した原文"
    }}
  ]
}}

【入力本文】
{block['block_text']}
"""


def validate_facts(parsed: dict, definition: dict, block_text: str) -> Tuple[List[dict], List[str]]:
    facts = parsed.get("facts")
    if not isinstance(facts, list):
        return [], ["factsが配列ではありません"]
    allowed = set(definition["allowed_relations"])
    valid: List[dict] = []
    errors: List[str] = []
    for index, raw in enumerate(facts):
        prefix = f"facts[{index}]"
        if not isinstance(raw, dict):
            errors.append(f"{prefix}: オブジェクトではありません")
            continue
        relation = str(raw.get("relation_type", "")).upper()
        subject = str(raw.get("subject_type", "")).upper()
        polarity = str(raw.get("polarity", "")).upper()
        certainty = str(raw.get("certainty", "")).upper()
        target_name = normalize_text(str(raw.get("target_name", "")))
        note_text = normalize_text(str(raw.get("note_text", "")))
        evidence = str(raw.get("evidence_text", "")).strip()
        details = raw.get("details")
        field_errors = []
        if relation not in allowed:
            field_errors.append(f"relation_type={relation!r} は許可されていません")
        if subject != "DRUG":
            field_errors.append("subject_typeがDRUGではありません")
        if polarity not in {"POSITIVE", "NEGATIVE"}:
            field_errors.append(f"polarity={polarity!r}")
        if certainty != "EXPLICIT":
            field_errors.append("certaintyがEXPLICITではありません")
        if not note_text:
            field_errors.append("note_textが空です")
        max_note_percentages = definition.get("max_note_percentages")
        note_percentages = re.findall(r"\d+(?:\.\d+)?\s*[％%]", note_text)
        if (
            isinstance(max_note_percentages, int)
            and max_note_percentages >= 0
            and len(note_percentages) > max_note_percentages
        ):
            field_errors.append(
                f"note_textの割合列挙が{len(note_percentages)}件あり、"
                f"上限{max_note_percentages}件を超えています"
            )
        exact_evidence = exact_or_whitespace_only_span(block_text, evidence) if evidence else None
        # 関係分類は、後段で文・ブロックへ拡張する前のLLM引用そのものと照合する。
        # 同じブロック内の別ファクトの語で誤分類が通ることを防ぐ。
        relation_evidence = exact_evidence or ""
        if not exact_evidence:
            field_errors.append("evidence_textが入力原文の連続部分と一致しません")
        if not isinstance(details, dict):
            field_errors.append("detailsがオブジェクトではありません")
            details = {}
        required_groups = definition.get("required_evidence_term_groups", [])
        if exact_evidence and required_groups:
            expanded = enclosing_sentence(block_text, exact_evidence)
            expanded_satisfies = all(
                any(normalize_text(str(term)).casefold() in normalize_text(expanded).casefold() for term in term_group)
                for term_group in required_groups
            )
            block_satisfies = all(
                any(normalize_text(str(term)).casefold() in normalize_text(block_text).casefold() for term in term_group)
                for term_group in required_groups
            )
            if expanded_satisfies:
                exact_evidence = expanded
            elif block_satisfies and len(block_text) <= 1000:
                # Item見出しに患者群、本文にPK変化が分かれている場合は両方を根拠に含める。
                exact_evidence = block_text
        for group_index, term_group in enumerate(required_groups):
            if not any(
                normalize_text(str(term)).casefold() in normalize_text(exact_evidence or "").casefold()
                for term in term_group
            ):
                field_errors.append(f"evidence_textが必須語群{group_index + 1}を満たしません")
        relation_required_groups = definition.get("relation_required_evidence_term_groups", {}).get(
            relation, []
        )
        for group_index, term_group in enumerate(relation_required_groups):
            if not any(
                normalize_text(str(term)).casefold() in normalize_text(relation_evidence).casefold()
                for term in term_group
            ):
                field_errors.append(
                    f"relation_type={relation}のevidence_textが関係別必須語群"
                    f"{group_index + 1}を満たしません"
                )
        if field_errors:
            errors.append(f"{prefix}: " + "; ".join(field_errors))
            continue
        target_code = normalized_target_code(target_name, raw.get("target_code"))
        fixed_target = definition.get("relation_targets", {}).get(relation)
        if isinstance(fixed_target, dict):
            target_code = fixed_target.get("target_code")
            target_name = normalize_text(str(fixed_target.get("target_name", "")))
        fact = {
            "relation_type": relation,
            "subject_type": subject,
            "target_code": target_code,
            "target_name": target_name,
            "polarity": polarity,
            "certainty": certainty,
            "note_text": note_text,
            "details": details,
            "evidence_text": exact_evidence,
        }
        fact["fact_hash"] = stable_json_hash(fact)
        valid.append(fact)

    # 同一LLM応答内に完全に同じファクトが複数回含まれても、保存単位は1件にする。
    # DBの UNIQUE (run_id, fact_hash) 違反を防ぎ、後段の件数検証にも重複を持ち込まない。
    unique_facts: Dict[str, dict] = {}
    for fact in valid:
        unique_facts.setdefault(fact["fact_hash"], fact)
    valid = list(unique_facts.values())

    # 1つの合計割合を列挙された成分・排泄経路へ複製する誤りを防ぐ。
    # 「それぞれ」が明記された文は個別値である可能性が高いため対象外とする。
    facts_by_evidence: Dict[str, List[dict]] = {}
    for fact in valid:
        facts_by_evidence.setdefault(fact["evidence_text"], []).append(fact)
    max_per_evidence = definition.get("max_facts_per_evidence")
    for evidence, evidence_facts in facts_by_evidence.items():
        if isinstance(max_per_evidence, int) and max_per_evidence > 0 and len(evidence_facts) > max_per_evidence:
            errors.append(
                f"同一evidence_textから{len(evidence_facts)}件が抽出されました。"
                f"上限{max_per_evidence}件を超えるため、列挙を要約してください"
            )
        if not definition.get("reject_duplicated_aggregate_percent") or "それぞれ" in evidence:
            continue
        percentage_to_facts: Dict[str, List[dict]] = {}
        for fact in evidence_facts:
            percentages = {
                match.group(1).replace("％", "%").replace(" ", "")
                for match in re.finditer(r"(\d+(?:\.\d+)?\s*[％%])", fact["note_text"])
            }
            for percentage in percentages:
                percentage_to_facts.setdefault(percentage, []).append(fact)
        normalized_evidence = normalize_text(evidence).replace("％", "%").replace(" ", "")
        for percentage, duplicated_facts in percentage_to_facts.items():
            if len(duplicated_facts) < 2:
                continue
            if normalized_evidence.count(percentage) == 1:
                errors.append(
                    f"同一evidence_text中に1回だけ現れる割合{percentage}が"
                    f"{len(duplicated_facts)}件へ複製されています。合計値として1件にまとめてください"
                )
    return valid, errors


def select_definitions(definitions: Iterable[dict], note_type: Optional[str]) -> List[dict]:
    selected = [item for item in definitions if not note_type or item["note_type"] == note_type]
    if note_type and not selected:
        raise ValueError(f"note_type が定義されていません: {note_type}")
    return selected

