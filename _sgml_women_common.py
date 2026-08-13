#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""妊婦・授乳構造化パイプラインの共通定義。"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple

from _sgml_note_common import normalize_text, stable_json_hash


PIPELINE_VERSION = "sgml-women-v1"
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
    ("PREFER_AVOID", "DRUG", _rx(r"(?:投与|使用)しないことが望ましい|(?:投与|使用)を避けることが望ましい")),
    ("AVOID", "DRUG", _rx(r"(?:投与|使用)しないこと(?!が望ましい)|(?:投与|使用)を避けること(?!が望ましい)|原則として?(?:投与|使用)しない")),
    ("BENEFIT_RISK", "DRUG", _rx(r"(?:有益性|ベネフィット|利益).*(?:危険性|リスク).*(?:上回|上まわ|上廻)")),
    ("ACCEPTABLE", "DRUG", _rx(r"投与して差し支えない|使用して差し支えない|投与可能である|使用可能である")),
]

LACTATION_RULES = [
    ("CONTRAINDICATED", "DRUG", _rx(r"授乳婦.{0,30}(?:禁忌|投与してはならない|使用してはならない)")),
    ("CONSIDER_CONTINUE_OR_STOP", "BREASTFEEDING", _rx(r"授乳.{0,30}(?:継続|続行).{0,30}(?:中止|中断).{0,20}(?:検討|考慮)|(?:有益性|ベネフィット).{0,50}授乳.{0,30}(?:継続|中止)")),
    ("STOP_BREASTFEEDING", "BREASTFEEDING", _rx(r"授乳.{0,20}(?:中止|中断)|断乳|母乳栄養.{0,20}(?:中止|中断)")),
    ("AVOID", "BREASTFEEDING", _rx(r"授乳.{0,20}(?:避け|しないこと)|授乳をさせない")),
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


def split_statements(text: str) -> List[str]:
    """原文順を保ち、句点・改行を境界として文を落とさず分割する。"""
    normalized = normalize_text(text)
    if not normalized:
        return []
    pieces = re.split(r"(?<=。)|\n+", normalized)
    return [piece.strip() for piece in pieces if piece.strip()]


def classify_statement(population_type: str, text: str) -> dict:
    normalized = normalize_text(text)
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
        "AVOID": _rx(r"避け|回避|控え|しないこと"),
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
