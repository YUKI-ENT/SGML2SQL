from pathlib import Path
import unittest
import xml.etree.ElementTree as ET

from _sgml_interaction_flat import collect_interactions_from_xml, NS


def wrap(partner, category="PrecautionsForCombinations"):
    return f'''<PackIns xmlns="{NS['pi']}"><Interactions><{category}><Drug>
      <DrugName><Detail><Lang xml:lang="ja">{partner}</Lang></Detail></DrugName>
      <ClinSymptomsAndMeasures><Detail><Lang xml:lang="ja">症状<?enter?>対応</Lang></Detail></ClinSymptomsAndMeasures>
      <MechanismAndRiskFactors><Detail><Lang xml:lang="ja">機序</Lang></Detail></MechanismAndRiskFactors>
      </Drug></{category}></Interactions></PackIns>'''


class LegacyFlatTests(unittest.TestCase):
    def test_real_xml_separates_names_but_keeps_combination(self):
        source = Path(__file__).parent / 'fixtures/interaction_maou.xml'
        rows = collect_interactions_from_xml(source.read_text(encoding="utf-8"))["flat"]
        self.assertIn("葛根湯\n小青竜湯\n麻黄湯", [r["partner"] for r in rows])
        self.assertIn("エフェドリン塩酸塩\ndl-メチルエフェドリン塩酸塩\nフェキソフェナジン塩酸塩・塩酸プソイドエフェドリン",
                      [r["partner"] for r in rows])

    def test_preserve_conditions_in_same_row(self):
        for text in ("薬剤A<?enter?>（薬剤Bを除く）", "薬剤A<?enter?>以外", "薬剤A<?enter?>経口投与時"):
            rows = collect_interactions_from_xml(wrap(text))["flat"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["partner"], text.replace("<?enter?>", "\n"))

    def test_schema_categories_and_other_fields(self):
        for section, expected in (("PrecautionsForCombinations", "併用注意"), ("ContraIndicatedCombinations", "併用禁忌")):
            row = collect_interactions_from_xml(wrap("薬剤A<?enter?>薬剤B", section))["flat"][0]
            self.assertEqual(set(row), {"partner", "group", "symptoms", "mechanism", "category"})
            self.assertEqual(row["category"], expected)
            self.assertEqual(row["symptoms"], "症状\n対応")
            self.assertEqual(row["mechanism"], "機序")

    def test_inline_elements_comments_and_non_enter_pi(self):
        row = collect_interactions_from_xml(wrap("H<Sub>2</Sub>受容体<!--comment--><?other test?>拮抗剤"))["flat"][0]
        self.assertEqual(row["partner"], "H2受容体拮抗剤")

    def test_invalid_xml_fails_instead_of_using_old_flat(self):
        with self.assertRaises(ET.ParseError):
            collect_interactions_from_xml("<broken>")


if __name__ == "__main__":
    unittest.main()
