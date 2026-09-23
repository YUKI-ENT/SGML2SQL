import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from contextlib import closing, redirect_stdout, redirect_stderr
from io import StringIO

from _sgml_interaction import NS, Resolver, digest, extract_document, report_counts

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("interaction_cli", ROOT / "25_resolve_sgml_interactions.py")
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


def detail(text):
    return f'<Detail><Lang xml:lang="ja">{text}</Lang></Detail>'


def xml(names):
    return f'<PackIns xmlns="{NS}"><Interactions><PrecautionsForCombinations><Drug><DrugName>{names}</DrugName></Drug></PrecautionsForCombinations></Interactions></PackIns>'


def master(name="シメチジン", code="1111111A1111", **extra):
    return dict(package_insert_no="master1", yj_code=code, generic_name_ja=name,
                standard_name_ja="", brand_name_ja="", **extra)


def extract(names):
    return extract_document("source1", xml(names), ["9999999A1111"])


def targets(document):
    return document["interactions"][0]["targets"]


class ExtractionTests(unittest.TestCase):
    def test_pi_list_boundary_and_combination(self):
        d = extract(detail("エフェドリン塩酸塩<?enter?>dl-メチルエフェドリン塩酸塩<?enter?>フェキソフェナジン塩酸塩・塩酸プソイドエフェドリン"))
        self.assertEqual(len(targets(d)), 3)
        self.assertEqual(targets(d)[2]["target_name"], "フェキソフェナジン塩酸塩・塩酸プソイドエフェドリン")

    def test_real_maou_structure(self):
        source = (ROOT / "tests/fixtures/interaction_maou.xml").read_text(encoding="utf-8")
        d = extract_document("5200138C1045_1_08", source, ["5200138C1045"])
        all_targets = [t for i in d["interactions"] for t in i["targets"]]
        by_key = {t["target_key"]: t for t in all_targets}
        for name, heading in [("葛根湯", "マオウ含有製剤"), ("セレギリン塩酸塩", "モノアミン酸化酵素（MAO）阻害剤"),
                              ("テオフィリン", "キサンチン系製剤")]:
            t = next(t for t in all_targets if t["target_name"] == name)
            self.assertEqual(by_key[t["parent_target_key"]]["target_name"], heading)
        self.assertTrue(any("フェキソフェナジン塩酸塩・塩酸プソイドエフェドリン" in t["target_name"] for t in all_targets))

    def test_nested_groups_keep_scope(self):
        names = detail("群A") + '<SimpleList><Item>' + detail("群B") + '<SimpleList><Item>' + detail("シメチジン") + '</Item></SimpleList></Item></SimpleList>'
        ts = targets(extract(names))
        self.assertEqual(ts[1]["parent_target_key"], ts[0]["target_key"])
        self.assertEqual(ts[2]["parent_target_key"], ts[1]["target_key"])

    def test_unknown_structures_and_reference_remain(self):
        for body in ['<Unknown>シメチジン</Unknown>', detail('<ApprovalBrandNameRef ref="x"/>'), detail('シメチジン<?unknown?>')]:
            t = targets(Resolver([master()]).resolve_document(extract(body)))[0]
            self.assertEqual(t["resolution_status"], "unresolved")
            self.assertFalse(t["codes"])

    def test_exclusions_and_route_block_entire_scope(self):
        for suffix in ("を除く", "以外", "を除いたもの", "を含まない", "経口投与"):
            d = extract(detail("群" + suffix) + '<SimpleList><Item>' + detail("シメチジン") + '</Item></SimpleList>')
            resolved = Resolver([master()]).resolve_document(d)
            self.assertTrue(all(t["condition_status"] == "unresolved" and not t["codes"] for t in targets(resolved)))

    def test_malformed_absent_empty_and_summary_differ(self):
        self.assertEqual(extract_document("x", "<", [])["extraction_status"], "error")
        self.assertEqual(extract_document("x", f'<PackIns xmlns="{NS}"/>', [])["extraction_status"], "section_absent")
        self.assertEqual(extract_document("x", f'<PackIns xmlns="{NS}"><Interactions/></PackIns>', [])["extraction_status"], "empty_section")
        d = extract_document("x", f'<PackIns xmlns="{NS}"><Interactions><SummaryOfCombination>{detail("記載")}</SummaryOfCombination></Interactions></PackIns>', [])
        self.assertEqual(len(d["interactions"]), 1)
        self.assertTrue(targets(d)[0]["extraction_reason"])

    def test_source_changes_change_keys(self):
        a, b = extract(detail("シメチジン")), extract(detail("シメチジン以外"))
        self.assertNotEqual(targets(a)[0]["target_key"], targets(b)[0]["target_key"])

    def test_ambiguous_heading_blocks_children(self):
        names = detail("群A<?enter?>群B") + '<SimpleList><Item>' + detail("シメチジン") + '</Item></SimpleList>'
        ts = targets(Resolver([master()]).resolve_document(extract(names)))
        self.assertTrue(all(t["unresolved_reason"] == "ambiguous_heading_scope" and not t["codes"] for t in ts))


class ResolutionTests(unittest.TestCase):
    def test_exact_multiple_yj_and_evidence(self):
        r = Resolver([master(), master(code="2222222A1111"), master()])
        d = r.resolve_document(extract(detail("シメチジン")))
        t = targets(d)[0]
        self.assertEqual(t["resolution_status"], "resolved")
        self.assertEqual(len(t["codes"]), 2)
        self.assertTrue(all(c["applicability"] == "requires_full_yj" for c in t["codes"]))
        self.assertEqual(len(t["codes"][0]["evidence"]["master_matches"]), 1)
        self.assertEqual(r.resolve_document(d), d)

    def test_group_examples_and_partial_row(self):
        body = detail("未登録群") + '<SimpleList><Item>' + detail("シメチジン等") + '</Item></SimpleList>'
        d = Resolver([master()]).resolve_document(extract(body))
        self.assertEqual([t["resolution_status"] for t in targets(d)], ["partial", "resolved"])
        self.assertEqual(d["interactions"][0]["resolution_status"], "partial")
        self.assertEqual(report_counts([d])["target_status"], {"partial": 1, "resolved": 1})

    def test_standalone_open_ended(self):
        d = Resolver([master()]).resolve_document(extract(detail("シメチジン<?enter?>等")))
        self.assertEqual(targets(d)[0]["resolution_status"], "partial")
        self.assertIn("等", targets(d)[0]["target_name"])

    def test_candidates_not_resolved(self):
        d = Resolver([master()]).resolve_document(extract(detail("シメチジン製剤")))
        t = targets(d)[0]
        self.assertEqual(t["resolution_status"], "unresolved")
        self.assertEqual(t["codes"][0]["review_status"], "needs_review")
        self.assertFalse(targets(Resolver([master()]).resolve_document(extract(detail("シメ"))))[0]["codes"])

    def test_ambiguous_brand_and_invalid_code(self):
        a, b = master("成分A"), master("成分B", code="2222222A1111")
        a["brand_name_ja"] = b["brand_name_ja"] = "共通名"
        t = targets(Resolver([a, b]).resolve_document(extract(detail("共通名"))))[0]
        self.assertEqual(t["unresolved_reason"], "ambiguous_name")
        t = targets(Resolver([master(), master(code="bad")]).resolve_document(extract(detail("シメチジン"))))[0]
        self.assertEqual(t["unresolved_reason"], "invalid_master_code_for_name")

    def test_reviewed_alias_validation(self):
        alias = {"version": "v1", "entries": [{"name": "確認済別名", "yj_codes": ["1111111A1111"],
                 "source": "fixture", "reviewer": "tester", "reviewed_at": "2026-09-23", "scope_complete": True}]}
        t = targets(Resolver([master()], alias).resolve_document(extract(detail("確認済別名"))))[0]
        self.assertEqual(t["resolution_status"], "resolved")
        self.assertEqual(t["codes"][0]["match_method"], "alias_dictionary")
        alias["entries"][0]["scope_complete"] = False
        with self.assertRaises(ValueError):
            Resolver([master()], alias)


class ArtifactTests(unittest.TestCase):
    def fixture(self):
        a = Resolver([master()]).resolve_document(extract(detail("シメチジン")))
        b = extract_document("master1", f'<PackIns xmlns="{NS}"/>', ["1111111A1111"])
        return [a, b]

    def test_database_full_code_direction_and_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "assessment.sqlite3"
            cli.save_database(path, self.fixture(), {"generation_id": "test"})
            result = cli.check_pair(path, "9999999A1111", "1111111A1111")
            self.assertEqual(len(result["matches"]), 1)
            self.assertEqual(result["matches"][0]["match_status"], "matched")
            self.assertEqual(cli.check_pair(path, "1111111A1111", "9999999A1111")["matches"][0]["direction"], "B_to_A")
            # Same YJ7 alone cannot broaden the product scope.
            result = cli.check_pair(path, "9999999A1111", "1111111B9999")
            self.assertFalse(result["matches"])
            self.assertTrue(result["has_unassessed_scope"])
            with closing(sqlite3.connect(path)) as conn:
                self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_failed_output_not_published_and_offline_rerun(self):
        from argparse import Namespace
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            source = {"package_insert_no": "source1", "own_yj_codes": ["9999999A1111"], "xml": xml(detail("シメチジン"))}
            args = Namespace(command="build", output_dir=str(folder / "one"), aliases=None, previous=None)
            with patch.object(cli, "fetch_snapshot", return_value=([master()], [source])), redirect_stdout(StringIO()):
                cli.build(args)
            args.command, args.input_dir, args.output_dir = "resolve", str(folder / "one"), str(folder / "two")
            with redirect_stdout(StringIO()):
                cli.build(args)
            one = json.loads((folder / "one/manifest.json").read_text(encoding="utf-8"))
            two = json.loads((folder / "two/manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(one["generation_id"], two["generation_id"])
            self.assertEqual((folder / "one/resolved.jsonl").read_bytes(), (folder / "two/resolved.jsonl").read_bytes())
            args.output_dir = str(folder / "failed")
            with patch.object(cli, "save_reports", side_effect=RuntimeError("test failure")), redirect_stderr(StringIO()):
                with self.assertRaises(RuntimeError):
                    cli.build(args)
            self.assertFalse((folder / "failed").exists())
            self.assertTrue((folder / "one/interactions.sqlite3").exists())

    def test_csv_formula_escaping(self):
        self.assertTrue(cli.safe_cell("  =SUM(A1)").startswith("'"))

    def test_cached_alias_is_reused_and_tampering_rejected(self):
        from argparse import Namespace
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(StringIO()):
            folder = Path(directory)
            alias = {"version": "v1", "entries": [{"name": "確認済別名", "yj_codes": ["1111111A1111"],
                     "source": "fixture", "reviewer": "tester", "reviewed_at": "2026-09-23", "scope_complete": True}]}
            cli.dump_json(folder / "alias.json", alias)
            source = {"package_insert_no": "source1", "own_yj_codes": ["9999999A1111"], "xml": xml(detail("確認済別名"))}
            args = Namespace(command="build", output_dir=str(folder / "one"), aliases=str(folder / "alias.json"), previous=None)
            with patch.object(cli, "fetch_snapshot", return_value=([master()], [source])):
                cli.build(args)
            args.command, args.input_dir, args.output_dir, args.aliases = "resolve", str(folder / "one"), str(folder / "two"), None
            cli.build(args)
            self.assertEqual((folder / "one/resolved.jsonl").read_bytes(), (folder / "two/resolved.jsonl").read_bytes())
            (folder / "one/extracted.jsonl").write_text('{}\n', encoding="utf-8")
            args.output_dir = str(folder / "rejected")
            with self.assertRaisesRegex(ValueError, "cached extraction changed"):
                cli.build(args)
            self.assertFalse((folder / "rejected").exists())


if __name__ == "__main__":
    unittest.main()
