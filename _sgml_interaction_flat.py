"""Legacy interactions_flat format, with XML line breaks preserved.

Keep names in the same row: a newline is not necessarily a drug-list separator
(it may precede an exclusion, a route restriction, or a brand-name example).
"""

import re
import xml.etree.ElementTree as ET

NS = {"pi": "http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0"}
JA = "{http://www.w3.org/XML/1998/namespace}lang"


class InteractionTreeBuilder(ET.TreeBuilder):
    def pi(self, target, text):
        if target == "enter":
            self.data("\n")


def lang_text_all(detail):
    if detail is not None:
        for lang in detail.findall("pi:Lang", NS):
            if lang.get(JA) == "ja":
                value = "".join(lang.itertext()).strip()
                if value:
                    return value
    return None


def detail_text_full(detail):
    if detail is None:
        return None
    value = lang_text_all(detail) or (detail.text or "").strip()
    if not value:
        return None
    # Retain the legacy label normalization. Do not split compound names.
    value = re.sub(r"^[、，,\s]+|[、，,\s]+$", "", value)
    value = re.sub(r"(等|など)$", "", value).strip()
    value = re.sub(r"^(等|など)", "", value).strip()
    return value or None


def extract_partner_group_and_items(drug):
    dn = drug.find("pi:DrugName", NS)
    if dn is None:
        return None, []
    group = next((text for d in dn.findall("pi:Detail", NS)
                  if (text := detail_text_full(d))), None)
    items = [text for d in dn.findall("pi:SimpleList/pi:Item/pi:Detail", NS)
             if (text := detail_text_full(d))]
    return group, list(dict.fromkeys(items))


def collect_interactions_flat(root):
    summary = []
    for detail in root.findall("pi:Interactions/pi:SummaryOfCombination//pi:Detail", NS):
        text = lang_text_all(detail) or (detail.text or "").strip()
        if text:
            summary.append(text)
    flat = []
    for section, category in (("ContraIndicatedCombinations", "併用禁忌"),
                              ("PrecautionsForCombinations", "併用注意")):
        for drug in root.findall(f"pi:Interactions/pi:{section}//pi:Drug", NS):
            group, items = extract_partner_group_and_items(drug)

            def content(tag):
                detail = drug.find(f"pi:{tag}/pi:Detail", NS)
                text = lang_text_all(detail)
                if not text and detail is not None:
                    text = (detail.text or "").strip()
                if not text:
                    parent = drug.find(f"pi:{tag}", NS)
                    text = (parent.text or "").strip() if parent is not None else None
                return text or None

            symptoms, mechanism = content("ClinSymptomsAndMeasures"), content("MechanismAndRiskFactors")
            for partner in items or ([group] if group else []):
                flat.append({"partner": partner, "group": group, "symptoms": symptoms,
                             "mechanism": mechanism, "category": category})
    return {"summary": summary, "flat": flat}


def collect_interactions_from_xml(xml):
    parser = ET.XMLParser(target=InteractionTreeBuilder())
    return collect_interactions_flat(ET.fromstring(xml, parser=parser))
