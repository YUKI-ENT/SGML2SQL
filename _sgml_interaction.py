"""Offline interaction extraction/resolution. Never treats missing matches as safe."""

import hashlib
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

VERSION = "interaction-1.0"
NS = "http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0"
JA = "{http://www.w3.org/XML/1998/namespace}lang"
SECTIONS = {"ContraIndicatedCombinations": "併用禁忌",
            "PrecautionsForCombinations": "併用注意"}
CONDITION = re.compile(r"除[くき外い]|以外|含まな|ではない|でない|ただし|但し|限[るり定]|場合|投与|経口|静注|注射|外用|吸入|点眼|経皮|坐剤|用量|以上|以下|未満|超える|併用時")
OPEN_END = re.compile(r"(?:等|など)\s*$")
YJ = re.compile(r"[0-9]{7}[A-Z][0-9]{4}\Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def normalize(value):
    # Do not remove dosage forms, salts, punctuation, or composition separators.
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value or "")).strip().casefold()


def local(node):
    return node.tag.split("}")[-1] if isinstance(node.tag, str) else ""


def visible(node):
    """PI line breaks are boundaries; formatting whitespace is not."""
    parts = [node.text or ""]
    for child in node:
        if child.tag is ET.ProcessingInstruction:
            if (child.text or "").strip() == "enter":
                parts.append("\n")
        elif child.tag is not ET.Comment:
            if local(child) in ("HeaderRef", "ApprovalBrandNameRef"):
                parts.append("[ref:" + child.get("ref", "") + "]")
            else:
                parts.append(visible(child))
        parts.append(child.tail or "")
    return "".join(parts)


def detail_text(node):
    langs = [x for x in node if local(x) == "Lang" and x.get(JA) == "ja"]
    if langs:
        return "\n".join(visible(x).strip() for x in langs)
    if any(local(x) == "Lang" for x in node):
        return ""
    return visible(node).strip()


def paths(root):
    result = {}

    def visit(node, path):
        result[id(node)] = path
        counts = Counter()
        for child in node:
            tag = local(child)
            if tag:
                counts[tag] += 1
                visit(child, f"{path}/{tag}[{counts[tag]}]")
    visit(root, "/" + local(root) + "[1]")
    return result


def extract_document(package, xml, own_codes):
    """Preserve every DrugName, including unsupported structures as unresolved targets."""
    doc = {"package_insert_no": package, "source_hash": digest(xml),
           "own_yj_codes": sorted(set(own_codes)), "extraction_status": "ok",
           "reason": "", "interactions": []}
    try:
        if not xml:
            raise ValueError("missing_xml")
        parser = ET.XMLParser(target=ET.TreeBuilder(insert_pis=True, insert_comments=True))
        root = ET.fromstring(xml, parser=parser)
    except (ET.ParseError, ValueError):
        doc.update(extraction_status="error", reason="missing_or_invalid_xml")
        return doc
    if root.tag != "{" + NS + "}PackIns":
        doc.update(extraction_status="error", reason="unsupported_xml_root")
        return doc
    locators = paths(root)
    interaction_sections = root.findall("{" + NS + "}Interactions")
    if not interaction_sections:
        doc["extraction_status"] = "section_absent"
        return doc

    def add_interaction(node, category, fallback_reason=""):
        location = locators[id(node)]
        raw = ET.tostring(node, encoding="unicode")
        key = digest([package, doc["source_hash"], location, raw])
        interaction = {"interaction_key": key, "package_insert_no": package,
                       "source_locator": location, "section_type": category,
                       "source_xml": raw, "partner_text": "",
                       "symptoms_measures_ja": "", "mechanism_ja": "", "targets": []}
        for tag, field in (("ClinSymptomsAndMeasures", "symptoms_measures_ja"),
                           ("MechanismAndRiskFactors", "mechanism_ja")):
            e = node.find("{" + NS + "}" + tag)
            if e is not None:
                interaction[field] = visible(e).strip()
        dn = node.find("{" + NS + "}DrugName") if not fallback_reason else node
        if dn is None:
            dn = node
            fallback_reason = "missing_drug_name"
        whole_text = visible(dn).strip()
        interaction["partner_text"] = whole_text
        # Until scope/negation parsing is supported, block the entire DrugName.
        condition = whole_text if CONDITION.search(whole_text) else ""

        def add(name, element, index=0, parent=None, kind="unknown", role="target", reason=""):
            locator = locators[id(element)] + f"/part[{index + 1}]"
            name = name.strip()
            target = {"target_key": digest([key, locator, name, role]),
                      "interaction_key": key, "parent_target_key": parent,
                      "target_name": name, "normalized_name": normalize(OPEN_END.sub("", name)),
                      "target_kind": kind, "mention_role": role,
                      "source_locator": locator, "condition_text": condition,
                      "condition_status": "unresolved" if condition else "none",
                      "extraction_reason": reason,
                      "is_open_ended": bool(OPEN_END.search(name)),
                      "resolution_status": "unresolved", "unresolved_reason": "",
                      "codes": []}
            if condition:
                target["extraction_reason"] = "condition_or_exclusion_unresolved"
            interaction["targets"].append(target)
            return target

        def details(element, parent, kind, role):
            text = detail_text(element)
            # Unknown inline structures must not disappear into a matching name.
            unsupported = any(local(x) not in ("Detail", "Lang", "Sub", "Sup")
                              for x in element.iter() if isinstance(x.tag, str))
            unsupported = unsupported or any(not x.tag.startswith("{" + NS + "}")
                                              for x in element.iter() if isinstance(x.tag, str))
            unsupported = unsupported or any(x.tag is ET.ProcessingInstruction and
                                              (x.text or "").strip() != "enter" for x in element.iter())
            parts = text.split("\n")
            if unsupported or not text:
                return [add(text or ET.tostring(element, encoding="unicode"), element,
                            parent=parent, reason="unsupported_inline_or_empty_text")]
            result = []
            for i, part in enumerate(parts):
                if not part.strip():
                    continue
                # Standalone markers attach to the preceding mention, never vanish.
                if normalize(part) in ("等", "など") and result:
                    result[-1]["is_open_ended"] = True
                    result[-1]["target_name"] += "\n" + part.strip()
                    continue
                result.append(add(part, element, i, parent, kind, role))
            return result

        def walk(container, parent=None, inherited_reason=""):
            start = len(interaction["targets"])
            children = [c for c in container if isinstance(c.tag, str)]
            heading = None
            ambiguous_heading = False
            for i, child in enumerate(children):
                tag = local(child)
                if tag == "Detail":
                    next_list = i + 1 < len(children) and local(children[i + 1]) == "SimpleList"
                    ts = details(child, parent, "group" if next_list else "unknown",
                                 "example" if parent else "target")
                    # Multiple lines preceding a list have ambiguous scope.
                    heading = ts[0] if next_list and len(ts) == 1 else None
                    ambiguous_heading = next_list and heading is None
                    if next_list and heading is None:
                        for t in ts:
                            t["extraction_reason"] = "ambiguous_heading_scope"
                elif tag == "SimpleList":
                    list_parent = heading["target_key"] if heading else parent
                    for item in child:
                        if local(item) == "Item":
                            walk(item, list_parent, "ambiguous_heading_scope" if ambiguous_heading else inherited_reason)
                        elif isinstance(item.tag, str):
                            add(visible(item), item, parent=list_parent,
                                reason="unsupported_list_structure")
                    heading = None
                    ambiguous_heading = False
                else:
                    add(visible(child), child, parent=parent, reason="unsupported_structure")
                    heading = None
            if (container.text or "").strip() or any((c.tail or "").strip() for c in container):
                add(visible(container), container, parent=parent, reason="unstructured_text")
            if inherited_reason:
                for t in interaction["targets"][start:]:
                    t["extraction_reason"] = t["extraction_reason"] or inherited_reason

        if fallback_reason:
            add(whole_text or raw, dn, reason=fallback_reason)
        else:
            walk(dn)
        if not interaction["targets"]:
            add(whole_text or raw, dn, reason="empty_drug_name")
        doc["interactions"].append(interaction)

    for section in interaction_sections:
        for child in section:
            if not isinstance(child.tag, str):
                continue
            if local(child) in SECTIONS:
                drugs = child.findall(".//{" + NS + "}Drug")
                for drug in drugs:
                    add_interaction(drug, SECTIONS[local(child)])
                if not drugs:
                    add_interaction(child, SECTIONS[local(child)], "section_without_drug_rows")
            else:
                # Includes SummaryOfCombination: never silently mark summary-only docs complete.
                add_interaction(child, "相互作用その他", "unsupported_interaction_section")
    if not doc["interactions"]:
        doc.update(extraction_status="empty_section", reason="no_interaction_content")
    return doc


class Resolver:
    def __init__(self, master, aliases=None):
        self.master = sorted(master, key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False))
        self.dictionary_version = digest(self.master)
        self.names = defaultdict(list)
        self.by_code = defaultdict(list)
        self.invalid_codes = 0
        self.invalid_names = set()
        for row in self.master:
            code = (row.get("yj_code") or "").strip()
            if not YJ.fullmatch(code):
                self.invalid_codes += 1
                self.invalid_names.update(normalize(row.get(f)) for f in
                                          ("generic_name_ja", "standard_name_ja", "brand_name_ja") if row.get(f))
                continue
            self.by_code[code].append(row)
            for field in ("generic_name_ja", "standard_name_ja", "brand_name_ja"):
                name = normalize(row.get(field))
                if name:
                    self.names[name].append((code, field, row.get("package_insert_no", "")))
        self.aliases = {}
        aliases = aliases or {"version": "none", "entries": []}
        if not isinstance(aliases.get("version"), str) or not aliases["version"]:
            raise ValueError("alias dictionary requires a version")
        for entry in aliases["entries"]:
            if not all(entry.get(k) for k in ("name", "yj_codes", "source", "reviewer", "reviewed_at")):
                raise ValueError("alias entry requires name, yj_codes, source, reviewer, reviewed_at")
            if entry.get("scope_complete") is not True or not isinstance(entry["yj_codes"], list):
                raise ValueError("alias entry must specify a reviewed, complete scope")
            if any(code not in self.by_code for code in entry["yj_codes"]):
                raise ValueError("alias code missing from master")
            name = normalize(entry["name"])
            if name in self.aliases:
                raise ValueError("duplicate normalized alias")
            self.aliases[name] = entry
        self.alias_version = aliases["version"] + ":" + digest(aliases)

    def resolve_target(self, target):
        t = dict(target, codes=[], resolution_status="unresolved", unresolved_reason="")
        name = t["normalized_name"]
        reason = t["extraction_reason"]
        if reason:
            t["unresolved_reason"] = reason
            return t
        if t["target_kind"] == "group":
            t["unresolved_reason"] = "group_scope_unresolved"
            return t
        matches = self.names.get(name, [])
        method, review = "name_exact", "accepted"
        evidence = []
        ambiguity = ""
        if matches:
            meanings = {normalize(row.get("generic_name_ja")) for code, _, _ in matches
                        for row in self.by_code[code] if row.get("generic_name_ja")}
            if len(meanings) > 1:
                review, ambiguity = "needs_review", "ambiguous_name"
            if name in self.invalid_names:
                review, ambiguity = "needs_review", "invalid_master_code_for_name"
        if name in self.aliases:
            entry = self.aliases[name]
            matches = [(code, "reviewed_alias", "") for code in entry["yj_codes"]]
            method = "alias_dictionary"
            review, ambiguity = "accepted", ""
            evidence.append(entry)
        if not matches and len(name) >= 4:
            method, review = "name_partial", "needs_review"
            for other, values in self.names.items():
                if len(other) >= 4 and (name in other or other in name):
                    matches.extend(values)
        if not matches:
            t["unresolved_reason"] = "name_not_found"
            return t
        # Keep all evidence while deduplicating repeated master rows.
        by_yj7 = defaultdict(list)
        for code, field, package in sorted(set(matches)):
            by_yj7[code[:7]].append({"yj_code": code, "field": field, "package_insert_no": package})
        for prefix, items in sorted(by_yj7.items()):
            t["codes"].append({"partner_yj7": prefix, "allowed_yj_codes": sorted({x["yj_code"] for x in items}),
                               "match_method": method, "review_status": review,
                               "applicability": "requires_full_yj",
                               "evidence": {"matched_name": name, "master_matches": items,
                                            "alias_review": evidence},
                               "dictionary_version": self.dictionary_version,
                               "alias_version": self.alias_version, "resolver_version": VERSION})
        if review == "accepted":
            t["target_kind"] = "drug"
            open_scope = t["is_open_ended"] and not t["parent_target_key"]
            t["resolution_status"] = "partial" if open_scope else "resolved"
            t["unresolved_reason"] = "open_ended_scope" if open_scope else ""
        else:
            t["unresolved_reason"] = ambiguity or "candidate_only"
        return t

    def resolve_document(self, document):
        result = dict(document, interactions=[])
        for interaction in document["interactions"]:
            targets = [self.resolve_target(t) for t in interaction["targets"]]
            by_parent = defaultdict(list)
            for t in targets:
                by_parent[t["parent_target_key"]].append(t)
            for t in reversed(targets):
                if t["target_kind"] == "group" and any(c["resolution_status"] in ("resolved", "partial")
                                                         for c in by_parent[t["target_key"]]):
                    t["resolution_status"] = "partial"
            statuses = {t["resolution_status"] for t in targets}
            status = "resolved" if statuses == {"resolved"} else "partial" if statuses & {"resolved", "partial"} else "unresolved"
            result["interactions"].append(dict(interaction, targets=targets, resolution_status=status))
        return result


def report_counts(documents):
    interactions = [i for d in documents for i in d["interactions"]]
    targets = [t for i in interactions for t in i["targets"]]
    codes = [c for t in targets for c in t["codes"]]
    return {"documents": len(documents),
            "document_status": dict(Counter(d["extraction_status"] for d in documents)),
            "interactions": len(interactions), "targets": len(targets),
            "interaction_status": dict(Counter(i["resolution_status"] for i in interactions)),
            "target_status": dict(Counter(t["resolution_status"] for t in targets)),
            "unresolved_reasons": dict(Counter(t["unresolved_reason"] for t in targets if t["unresolved_reason"])),
            "code_mappings": len(codes),
            "review_status": dict(Counter(c["review_status"] for c in codes)),
            "match_methods": dict(Counter(c["match_method"] for c in codes)),
            "condition_unresolved": sum(t["condition_status"] == "unresolved" for t in targets)}
