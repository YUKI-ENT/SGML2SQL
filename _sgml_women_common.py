#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""妊婦・授乳構造化パイプラインの共通定義。"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from _sgml_note_common import exact_or_whitespace_only_span, normalize_text, stable_json_hash


BLOCK_EXTRACTOR_VERSION = "sgml-women-v1"
LLM_DEFINITION_VERSION = "sgml-women-v1"
RULE_DEFINITION_VERSION = "sgml-women-rules-v2"
# 既存コードとの互換用。新規コードでは用途別定数を使用する。
PIPELINE_VERSION = LLM_DEFINITION_VERSION
POPULATIONS = {"PREGNANCY", "LACTATION"}

CLASSIFICATION_META: Dict[str, Tuple[str, str, int]] = {
    "CONTRAINDICATED": ("RED", "投与禁忌", 100),
    "AVOID": ("RED", "投与又は授乳を避ける", 90),
    "STOP_BREASTFEEDING": ("RED", "授乳を中止する", 90),
    "PREFER_AVOID": ("YELLOW", "投与しないことが望ましい", 70),
    "BENEFIT_RISK": ("YELLOW", "有益性が危険性を上回る場合のみ", 60),
    "CONSIDER_CONTINUE_OR_STOP": ("YELLOW", "授乳の継続又は中止を検討", 50),
    "UNCLASSIFIABLE": ("YELLOW", "判定不能・要確認", 40),
    "ACCEPTABLE": ("BLUE", "明示的に使用可能", 20),
}


def _rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.DOTALL)


PREGNANCY_RULES = [
    ("CONTRAINDICATED", "DRUG", _rx(r"禁忌|投与してはならない|使用してはならない")),
    ("PREFER_AVOID", "DRUG", _rx(r"(?:投与|使用)(?:を)?しないことが望ましい|(?:投与|使用)を避けることが望ましい")),
    ("AVOID", "DRUG", _rx(r"(?:投与|使用)(?:を)?しないこと(?!が望ましい)|(?:投与|使用)せず|(?:投与|使用)を避けること(?!が望ましい)|原則として?(?:投与|使用)(?:を)?しない")),
    ("BENEFIT_RISK", "DRUG", _rx(r"(?:有益性|ベネフィット|利益).*(?:危険性|リスク).*(?:上回|上まわ|上廻)")),
    ("ACCEPTABLE", "DRUG", _rx(r"投与して差し支えない|使用して差し支えない|投与可能である|使用可能である")),
]

LACTATION_RULES = [
    ("CONTRAINDICATED", "DRUG", _rx(r"授乳婦.{0,30}(?:禁忌|投与してはならない|使用してはならない)")),
    ("CONSIDER_CONTINUE_OR_STOP", "BREASTFEEDING", _rx(r"授乳.{0,30}(?:継続|続行).{0,30}(?:中止|中断).{0,20}(?:検討|考慮)|(?:有益性|ベネフィット).{0,50}授乳.{0,30}(?:継続|中止)")),
    ("STOP_BREASTFEEDING", "BREASTFEEDING", _rx(r"授乳.{0,20}(?:中止|中断)|断乳|母乳栄養.{0,20}(?:中止|中断)")),
    ("PREFER_AVOID", "DRUG", _rx(r"(?:授乳婦|授乳中の(?:患者|女性|婦人)).{0,30}(?:(?:投与|使用)(?:を)?しないこと|(?:投与|使用)を避けること)が望ましい")),
    ("PREFER_AVOID", "BREASTFEEDING", _rx(r"(?:授乳(?:を)?しない|母乳を与えない)ことが望ましい|授乳を避け(?:させ)?ることが望ましい")),
    ("AVOID", "DRUG", _rx(r"(?:授乳婦|授乳中の(?:患者|女性|婦人)).{0,30}(?:投与|使用)(?:を)?しないこと(?!が望ましい)")),
    ("AVOID", "BREASTFEEDING", _rx(r"授乳.{0,20}(?:避け|しないこと)|授乳をさせない|母乳を与えないこと")),
    ("BENEFIT_RISK", "DRUG", _rx(r"(?:有益性|ベネフィット|利益).*(?:危険性|リスク).*(?:上回|上まわ|上廻)")),
    ("ACCEPTABLE", "BREASTFEEDING", _rx(r"授乳を継続できる|授乳して差し支えない|母乳栄養を継続できる")),
]

RECOMMENDATION_CUE = _rx(
    r"禁忌|投与|使用|避け|望ましい|有益性|ベネフィット|危険性|リスク|"
    r"授乳.{0,30}(?:中止|中断|継続|検討|考慮|させない)|断乳"
)

EXPRESSION_RULES = [
    ("MILK_TRANSFER", _rx(r"乳汁|母乳")),
    ("INFANT_EFFECT", _rx(r"乳児|新生児|出生児")),
    ("ANIMAL_FINDING", _rx(r"動物|ラット|マウス|ウサギ|サル|胎仔|胎児毒性|催奇形")),
    ("PLACENTAL_TRANSFER", _rx(r"胎盤.{0,20}(?:通過|移行)")),
    ("HUMAN_FINDING", _rx(r"妊婦|妊娠女性|ヒト")),
]

WOMEN_CONTEXT_EXCLUSIONS = [
    _rx(r"HMG-CoA還元酵素阻害剤.{0,100}禁忌.{0,100}本剤との併用投与"),
]


def split_statements(text: str) -> List[str]:
    """原文順を保ち、句点・改行を境界として文を落とさず分割する。"""
    normalized = normalize_text(text)
    if not normalized:
        return []
    pieces = re.split(r"(?<=。)|\n+", normalized)
    return [piece.strip() for piece in pieces if piece.strip()]


def classify_statement(population_type: str, text: str) -> dict:
    normalized = normalize_text(text)
    if any(pattern.search(normalized) for pattern in WOMEN_CONTEXT_EXCLUSIONS):
        return {
            "expression_type": "OTHER_INFORMATION",
            "classification_code": None,
            "recommendation_target": None,
            "requires_llm": False,
        }
    rules = PREGNANCY_RULES if population_type == "PREGNANCY" else LACTATION_RULES
    for code, target, pattern in rules:
        if pattern.search(normalized):
            return {
                "expression_type": "RECOMMENDATION",
                "classification_code": code,
                "recommendation_target": target,
                "requires_llm": False,
            }
    if RECOMMENDATION_CUE.search(normalized):
        return {
            "expression_type": "RECOMMENDATION",
            "classification_code": "UNCLASSIFIABLE",
            "recommendation_target": None,
            "requires_llm": True,
        }
    expression_type = "OTHER_INFORMATION"
    for candidate_type, pattern in EXPRESSION_RULES:
        if pattern.search(normalized):
            expression_type = candidate_type
            break
    return {
        "expression_type": expression_type,
        "classification_code": None,
        "recommendation_target": None,
        "requires_llm": False,
    }


def classification_is_supported(population_type: str, code: str, evidence: str) -> bool:
    if code == "UNCLASSIFIABLE":
        return True
    support_patterns = {
        "CONTRAINDICATED": _rx(r"禁忌|禁止|してはならない"),
        "AVOID": _rx(r"避け|回避|控え|しないこと|せず"),
        "STOP_BREASTFEEDING": _rx(r"授乳.{0,30}(?:中止|中断)|断乳|母乳.{0,30}(?:中止|中断)"),
        "PREFER_AVOID": _rx(r"望ましい|極力|可能な限り"),
        "BENEFIT_RISK": _rx(r"有益性|ベネフィット|利益|必要性"),
        "CONSIDER_CONTINUE_OR_STOP": _rx(r"授乳.{0,40}(?:継続|中止|中断).{0,30}(?:検討|考慮)|(?:継続|中止).{0,30}(?:検討|考慮)"),
        "ACCEPTABLE": _rx(r"差し支えない|可能|継続できる|影響しない"),
    }
    pattern = support_patterns.get(code)
    if pattern is None or not pattern.search(evidence):
        return False
    if code in {"STOP_BREASTFEEDING", "CONSIDER_CONTINUE_OR_STOP"} and population_type != "LACTATION":
        return False
    return True


def normalize_recommendation_target(value: Any, classification_code: str) -> Optional[str]:
    """LLMがJSON nullを文字列で返した場合も安全にNULLへ正規化する。"""
    if classification_code == "UNCLASSIFIABLE":
        return None
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if normalized in {"", "NULL", "NONE"}:
        return None
    return normalized


def exact_women_evidence(source: str, evidence: str) -> Optional[str]:
    """原文全文との一致を確認し、末尾句読点だけの差なら原文へ復元する。"""
    exact = exact_or_whitespace_only_span(source, evidence)
    if exact == source:
        return exact
    source_stem = re.sub(r"[,，。．.!！?？]+\s*$", "", source).rstrip()
    evidence_stem = re.sub(r"[,，。．.!！?？]+\s*$", "", evidence).rstrip()
    compact_source_stem = "".join(char for char in source_stem if not char.isspace())
    compact_evidence_stem = "".join(char for char in evidence_stem if not char.isspace())
    if compact_source_stem and compact_source_stem == compact_evidence_stem:
        return source
    without_spurious_commas = re.sub(
        r"[,，]+(?=\s*(?:[。．.!！?？]|$))",
        "",
        evidence,
    )
    if without_spurious_commas == evidence:
        return None
    exact = exact_or_whitespace_only_span(source, without_spurious_commas)
    return exact if exact == source else None


def validate_llm_response(parsed: dict, candidate: dict) -> tuple[Optional[dict], List[str]]:
    errors: List[str] = []
    code = str(parsed.get("classification_code", "")).upper()
    target = normalize_recommendation_target(parsed.get("recommendation_target"), code)
    evidence = str(parsed.get("evidence_text", "")).strip()
    assessment = str(parsed.get("assessment_text", "")).strip()
    exact = exact_women_evidence(candidate["evidence_text"], evidence) if evidence else None
    if code not in CLASSIFICATION_META:
        errors.append(f"classification_code={code!r}は許可されていません")
    if target not in {None, "DRUG", "BREASTFEEDING"}:
        errors.append(f"recommendation_target={target!r}は許可されていません")
    if not exact or exact != candidate["evidence_text"]:
        errors.append("evidence_textが入力文全体と一致しません")
    if (
        code in CLASSIFICATION_META
        and exact
        and not classification_is_supported(candidate["population_type"], code, exact)
    ):
        # 原文は一致しているが強い分類の明示根拠がない場合、推測で赤判定にしない。
        # 「分類可能なら分類、不可能なら明記」という方針に従い黄判定へ落とす。
        code = "UNCLASSIFIABLE"
        target = None
        assessment = "既存分類へ確実に分類できない記載"
    if not assessment:
        errors.append("assessment_textが空です")
    if errors:
        return None, errors
    return {
        "classification_code": code,
        "recommendation_target": target,
        "assessment_text": assessment,
        "evidence_text": exact,
    }, []


def assessment_for_codes(codes: Iterable[Optional[str]], has_section: bool) -> Tuple[str, str, str]:
    present = [code for code in codes if code in CLASSIFICATION_META]
    if present:
        selected = max(present, key=lambda code: CLASSIFICATION_META[code][2])
        display, text, _priority = CLASSIFICATION_META[selected]
        return selected, display, text
    if has_section:
        return "NO_EXPLICIT_RECOMMENDATION", "GRAY", "明確な推奨記載なし"
    return "SECTION_ABSENT", "GRAY", "関連記載なし"


def statement_hash(population_type: str, evidence_text: str, occurrence: int) -> str:
    return stable_json_hash(
        {"population_type": population_type, "evidence_text": evidence_text, "occurrence": occurrence}
    )
