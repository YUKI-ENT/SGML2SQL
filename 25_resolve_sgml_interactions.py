#!/usr/bin/env python3
"""Read-only DB assessment and reproducible offline interaction resolution."""

import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone

from _sgml_interaction import VERSION, Resolver, digest, extract_document, report_counts, YJ

SCHEMA_VERSION = 1
MASTER_FIELDS = ("package_insert_no", "yj_code", "generic_name_ja", "standard_name_ja", "brand_name_ja")
REVIEW_ACTIONS = {
    "name_not_found": ("名称が見つからない", "元XMLとマスターを確認し、出典付き別名辞書を追加"),
    "candidate_only": ("部分一致候補のみ", "候補の成分・製品を確認し、正しい全桁コードを別名辞書へ登録"),
    "group_scope_unresolved": ("群全体の範囲が未確定", "出典付き群辞書の整備が必要。例示薬だけで群全体を解決しない"),
    "ambiguous_name": ("同じ名称に異なる一般名が対応", "対象範囲を確認し、採用する全桁コードを明示"),
    "invalid_master_code_for_name": ("同名のマスターにYJ欠落・形式不適合あり", "対象範囲とマスターのコードを確認"),
    "condition_or_exclusion_unresolved": ("除外・適用条件を未評価", "原文の条件範囲を確認し、条件評価機能を追加"),
    "ambiguous_heading_scope": ("見出しの適用範囲が不明", "原文XMLの見出しとリストを確認し、抽出器を修正"),
    "unsupported_inline_or_empty_text": ("参照・未対応の内部要素、または空記載", "元XMLの参照先・内部要素を確認し、抽出器を拡張"),
    "unsupported_interaction_section": ("要約など未対応の相互作用節", "原文を確認し、対象・条件の抽出方法を追加"),
    "open_ended_scope": ("等・などにより残りの範囲が不明", "例示されていない対象範囲を確認"),
}


def dump_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def dump_lines(path, values):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for value in values:
            f.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")


def read_lines(path):
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def fetch_snapshot(args):
    # Imported only by the DB command; offline operations use the standard library.
    import psycopg2
    from psycopg2 import sql
    config = json.loads(Path(args.config).read_text(encoding="utf-8-sig"))
    table = sql.Identifier(*config.get("sgml_table", "public.sgml_rawdata").split("."))
    settings = {k: v for k, v in config["db"].items() if k in ("host", "port", "dbname", "user", "password")}
    connection = psycopg2.connect(**settings, connect_timeout=10,
                                 options="-c default_transaction_read_only=on -c statement_timeout=120000")
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with connection, connection.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            if cur.fetchone()[0] != "on":
                raise RuntimeError("read-only transaction required")
            cur.execute(sql.SQL("SELECT {} FROM {} ORDER BY package_insert_no, yj_code").format(
                sql.SQL(", ").join(map(sql.Identifier, MASTER_FIELDS)), table))
            master = [dict(zip(MASTER_FIELDS, row)) for row in cur.fetchall()]
            query = sql.SQL("SELECT DISTINCT package_insert_no FROM {} WHERE TRUE").format(table)
            params = []
            if args.package_insert_no:
                query += sql.SQL(" AND package_insert_no = ANY(%s)")
                params.append(args.package_insert_no)
            query += sql.SQL(" ORDER BY package_insert_no")
            if args.limit:
                query += sql.SQL(" LIMIT %s")
                params.append(args.limit)
            cur.execute(query, params)
            packages = [r[0] for r in cur.fetchall()]
            if args.package_insert_no and set(args.package_insert_no) - set(packages):
                raise ValueError("requested document not found (or excluded by --limit)")
            if not packages:
                raise ValueError("no source documents")
            sources = []
            for start in range(0, len(packages), 100):
                cur.execute(sql.SQL("SELECT package_insert_no, yj_code, doc_xml::text FROM {} "
                                    "WHERE package_insert_no = ANY(%s) ORDER BY package_insert_no, yj_code").format(table),
                            (packages[start:start + 100],))
                grouped = {}
                for package, code, xml in cur.fetchall():
                    value = grouped.setdefault(package, {"package_insert_no": package, "xml": xml, "own_yj_codes": []})
                    if xml != value["xml"]:
                        raise ValueError("inconsistent XML within one document")
                    value["own_yj_codes"].append(code)
                sources.extend(grouped.values())
                print(f"Read {min(start + 100, len(packages))}/{len(packages)} documents", flush=True)
            return master, sources
    finally:
        connection.close()


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE metadata (generation_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, manifest_json TEXT NOT NULL);
CREATE TABLE document (package_insert_no TEXT PRIMARY KEY, source_hash TEXT NOT NULL, extraction_status TEXT NOT NULL, reason TEXT NOT NULL);
CREATE TABLE document_drug (package_insert_no TEXT NOT NULL REFERENCES document, yj_code TEXT NOT NULL,
 PRIMARY KEY(package_insert_no, yj_code));
CREATE INDEX document_drug_yj ON document_drug(yj_code);
CREATE TABLE sgml_interaction (interaction_key TEXT PRIMARY KEY, package_insert_no TEXT NOT NULL REFERENCES document,
 section_type TEXT NOT NULL, source_locator TEXT NOT NULL, source_xml TEXT NOT NULL, partner_text TEXT NOT NULL,
 symptoms_measures_ja TEXT NOT NULL, mechanism_ja TEXT NOT NULL, resolution_status TEXT NOT NULL);
CREATE INDEX interaction_document ON sgml_interaction(package_insert_no);
CREATE TABLE sgml_interaction_target (target_key TEXT PRIMARY KEY, interaction_key TEXT NOT NULL REFERENCES sgml_interaction,
 parent_target_key TEXT REFERENCES sgml_interaction_target, target_name TEXT NOT NULL, normalized_name TEXT NOT NULL,
 target_kind TEXT NOT NULL, mention_role TEXT NOT NULL, resolution_status TEXT NOT NULL,
 condition_text TEXT NOT NULL, condition_status TEXT NOT NULL, unresolved_reason TEXT NOT NULL, source_locator TEXT NOT NULL);
CREATE INDEX target_interaction ON sgml_interaction_target(interaction_key);
CREATE TABLE sgml_interaction_target_code (target_key TEXT NOT NULL REFERENCES sgml_interaction_target,
 partner_yj7 TEXT NOT NULL, match_method TEXT NOT NULL, review_status TEXT NOT NULL, applicability TEXT NOT NULL,
 evidence_json TEXT NOT NULL, dictionary_version TEXT NOT NULL, alias_version TEXT NOT NULL, resolver_version TEXT NOT NULL,
 PRIMARY KEY(target_key, partner_yj7, match_method));
CREATE INDEX target_code_yj7 ON sgml_interaction_target_code(partner_yj7);
CREATE TABLE sgml_interaction_target_full_code (target_key TEXT NOT NULL, partner_yj7 TEXT NOT NULL, match_method TEXT NOT NULL,
 yj_code TEXT NOT NULL, PRIMARY KEY(target_key, partner_yj7, match_method, yj_code),
 FOREIGN KEY(target_key, partner_yj7, match_method) REFERENCES sgml_interaction_target_code);
CREATE INDEX target_full_code_yj ON sgml_interaction_target_full_code(yj_code);
"""


def save_database(path, documents, manifest):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA)
        with conn:
            conn.execute("INSERT INTO metadata VALUES (?, ?, ?)",
                         (manifest["generation_id"], SCHEMA_VERSION, json.dumps(manifest, ensure_ascii=False)))
            for d in documents:
                conn.execute("INSERT INTO document VALUES (?, ?, ?, ?)", tuple(d[k] for k in
                             ("package_insert_no", "source_hash", "extraction_status", "reason")))
                conn.executemany("INSERT INTO document_drug VALUES (?, ?)",
                                 [(d["package_insert_no"], code) for code in d["own_yj_codes"]])
                for i in d["interactions"]:
                    conn.execute("INSERT INTO sgml_interaction VALUES (?,?,?,?,?,?,?,?,?)", tuple(i[k] for k in
                                 ("interaction_key", "package_insert_no", "section_type", "source_locator", "source_xml",
                                  "partner_text", "symptoms_measures_ja", "mechanism_ja", "resolution_status")))
                    for t in i["targets"]:
                        conn.execute("INSERT INTO sgml_interaction_target VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", tuple(t[k] for k in
                                     ("target_key", "interaction_key", "parent_target_key", "target_name", "normalized_name",
                                      "target_kind", "mention_role", "resolution_status", "condition_text", "condition_status",
                                      "unresolved_reason", "source_locator")))
                        for c in t["codes"]:
                            conn.execute("INSERT INTO sgml_interaction_target_code VALUES (?,?,?,?,?,?,?,?,?)",
                                         (t["target_key"], c["partner_yj7"], c["match_method"], c["review_status"],
                                          c["applicability"], json.dumps(c["evidence"], ensure_ascii=False),
                                          c["dictionary_version"], c["alias_version"], c["resolver_version"]))
                            conn.executemany("INSERT INTO sgml_interaction_target_full_code VALUES (?,?,?,?)",
                                             [(t["target_key"], c["partner_yj7"], c["match_method"], code)
                                              for code in c["allowed_yj_codes"]])
            if conn.execute("PRAGMA foreign_key_check").fetchall():
                raise ValueError("foreign key validation failed")
            if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("SQLite integrity validation failed")
    finally:
        conn.close()


def safe_cell(value):
    # CSV is a review artifact, not executable spreadsheet content.
    text = str(value if value is not None else "")
    return "'" + text if text.lstrip().startswith(("=", "+", "-", "@")) else text


def save_csv(path, fields, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fields, lineterminator="\r\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: safe_cell(row.get(k, "")) for k in fields})


def save_reports(folder, documents, summary):
    rows, mappings = [], []
    for d in documents:
        for i in d["interactions"]:
            for t in i["targets"]:
                row = {k: v for k, v in t.items() if k != "codes"}
                row.update(package_insert_no=d["package_insert_no"], own_yj_codes=";".join(d["own_yj_codes"]),
                           section_type=i["section_type"], partner_text=i["partner_text"],
                           symptoms_measures_ja=i["symptoms_measures_ja"], mechanism_ja=i["mechanism_ja"])
                row["accepted_yj7"] = ";".join(c["partner_yj7"] for c in t["codes"] if c["review_status"] == "accepted")
                row["candidate_yj7"] = ";".join(c["partner_yj7"] for c in t["codes"] if c["review_status"] == "needs_review")
                row["unresolved_reason_ja"], row["next_action"] = REVIEW_ACTIONS.get(t["unresolved_reason"],
                    ("その他の未対応構造", "元XMLを確認し抽出器を修正") if t["unresolved_reason"] else ("", ""))
                rows.append(row)
                for c in t["codes"]:
                    mappings.append(dict(target_key=t["target_key"], **{k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                                                                      for k, v in c.items()}))
    fields = ["package_insert_no", "own_yj_codes", "section_type", "interaction_key", "target_key", "parent_target_key",
              "target_name", "normalized_name", "target_kind", "mention_role", "resolution_status", "unresolved_reason",
              "unresolved_reason_ja", "next_action",
              "condition_status", "condition_text", "accepted_yj7", "candidate_yj7", "source_locator", "partner_text",
              "symptoms_measures_ja", "mechanism_ja"]
    save_csv(folder / "targets.csv", fields, rows)
    save_csv(folder / "unresolved.csv", fields, [r for r in rows if r["resolution_status"] != "resolved" or r["condition_status"] == "unresolved"])
    save_csv(folder / "codes.csv", ["target_key", "partner_yj7", "allowed_yj_codes", "match_method", "review_status",
                                    "applicability", "evidence", "dictionary_version", "alias_version", "resolver_version"], mappings)
    save_csv(folder / "documents.csv", ["package_insert_no", "source_hash", "extraction_status", "reason"], documents)
    dump_json(folder / "summary.json", summary)
    report = ["# 相互作用コード化の評価結果", "", "自動処理の結果です。誤対応・見落としの人手評価は未実施です。",
              "解決状態は入力された薬剤マスター内での対応を表し、臨床的な安全性や全製品の網羅性を保証しません。",
              "", f"文書数: {summary['documents']} / 相互作用行数: {summary['interactions']} / 相手記載数: {summary['targets']}",
              "", "## 相手記載の解決状態", ""]
    for key in ("resolved", "partial", "unresolved"):
        report.append(f"- {key}: {summary['target_status'].get(key, 0)}")
    report.extend(["", "## 未解決理由", ""])
    report.extend(f"- {k}: {v}" for k, v in sorted(summary["unresolved_reasons"].items()))
    report.extend(["", "原文・確認対象は targets.csv / unresolved.csv、対応根拠と全桁コードは codes.csv を参照してください。",
                   "抽出エラー・章なしは documents.csv で確認してください。章なしは安全判定ではありません。", ""])
    (folder / "report.md").write_bytes("\r\n".join(report).encode("utf-8"))


def build(args):
    output = Path(args.output_dir).resolve()
    if output.exists():
        raise ValueError("output directory already exists; use a new directory for each generation")
    if args.command == "build":
        master, sources = fetch_snapshot(args)
        extracted = [extract_document(s["package_insert_no"], s["xml"], s["own_yj_codes"]) for s in sources]
    else:
        input_dir = Path(args.input_dir)
        master = read_lines(input_dir / "master.jsonl")
        extracted = read_lines(input_dir / "extracted.jsonl")
        prior_manifest = json.loads((input_dir / "manifest.json").read_text(encoding="utf-8"))
        if prior_manifest["extractor_version"] != VERSION:
            raise ValueError("extractor version changed; rebuild from XML")
        if digest(extracted) != prior_manifest["source_version"]:
            raise ValueError("cached extraction changed; rebuild from XML")
        if digest(sorted(master, key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False))) != prior_manifest["master_version"]:
            raise ValueError("cached master changed; rebuild the snapshot")
        sources = None
    alias_path = Path(args.aliases) if args.aliases else (Path(args.input_dir) / "aliases.json" if args.command == "resolve" else None)
    aliases = json.loads(alias_path.read_text(encoding="utf-8-sig")) if alias_path else None
    resolver = Resolver(master, aliases)
    documents = [resolver.resolve_document(d) for d in extracted]
    manifest = {"schema_version": SCHEMA_VERSION, "extractor_version": VERSION, "resolver_version": VERSION,
                "master_version": resolver.dictionary_version, "alias_version": resolver.alias_version,
                "source_version": digest(extracted), "created_at": datetime.now(timezone.utc).isoformat(),
                "scope": "assessment_only", "master_rows_with_invalid_yj": resolver.invalid_codes}
    manifest["generation_id"] = digest({k: v for k, v in manifest.items() if k != "created_at"})
    summary = report_counts(documents)
    summary.update(generation_id=manifest["generation_id"], master_rows=len(master),
                   master_rows_with_invalid_yj=resolver.invalid_codes)
    if args.previous:
        previous = json.loads((Path(args.previous) / "summary.json").read_text(encoding="utf-8"))
        summary["previous_generation"] = previous["generation_id"]
        summary["count_difference"] = {k: summary[k] - previous[k] for k in ("documents", "interactions", "targets", "code_mappings")}
        summary["target_status_difference"] = {k: summary["target_status"].get(k, 0) - previous["target_status"].get(k, 0)
                                                for k in ("resolved", "partial", "unresolved")}
        summary["difference_note"] = "Counts only; compare source scope and dictionary versions before interpreting."
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".interaction-staging-", dir=output.parent))
    try:
        dump_lines(staging / "master.jsonl", resolver.master)
        dump_lines(staging / "extracted.jsonl", extracted)
        if sources is not None:
            dump_lines(staging / "sources.jsonl", sources)
        else:
            source_file = Path(args.input_dir) / "sources.jsonl"
            if source_file.exists():
                shutil.copyfile(source_file, staging / "sources.jsonl")
        dump_json(staging / "aliases.json", aliases or {"version": "none", "entries": []})
        dump_json(staging / "manifest.json", manifest)
        dump_lines(staging / "resolved.jsonl", documents)
        save_database(staging / "interactions.sqlite3", documents, manifest)
        save_reports(staging, documents, summary)
        # An incomplete generation is never presented as the final output folder.
        os.rename(staging, output)
    except BaseException:
        # Preserve diagnostic files; never delete a caller-supplied directory.
        print(f"Incomplete output retained at: {staging}", file=sys.stderr)
        raise
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Output: {output}")


def check_pair(database, a, b):
    if not YJ.fullmatch(a) or not YJ.fullmatch(b):
        raise ValueError("pair lookup requires two full 12-character YJ codes")
    uri = Path(database).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        result = {"generation_id": connection.execute("SELECT generation_id FROM metadata").fetchone()[0],
                  "matches": [], "unresolved": [], "coverage": []}
        for direction, own, partner in (("A_to_B", a, b), ("B_to_A", b, a)):
            coverage = [dict(r) for r in connection.execute(
                "SELECT d.* FROM document d JOIN document_drug dd USING(package_insert_no) WHERE dd.yj_code=?", (own,))]
            result["coverage"].append({"direction": direction, "own_yj": own, "documents": coverage,
                                       "status": "available" if coverage else "source_missing"})
            query = """SELECT i.*, t.target_key, t.target_name, t.resolution_status AS target_status,
                        t.condition_status, t.unresolved_reason, c.partner_yj7, c.review_status, c.match_method, c.evidence_json
                        FROM document_drug dd JOIN sgml_interaction i USING(package_insert_no)
                        JOIN sgml_interaction_target t USING(interaction_key)
                        JOIN sgml_interaction_target_code c USING(target_key)
                        JOIN sgml_interaction_target_full_code f USING(target_key, partner_yj7, match_method)
                        WHERE dd.yj_code=? AND f.yj_code=? AND c.review_status != 'rejected'
                        AND t.mention_role != 'exclusion' ORDER BY i.interaction_key,t.target_key"""
            for row in connection.execute(query, (own, partner)):
                value = dict(row, direction=direction)
                value["match_status"] = "matched" if row["review_status"] == "accepted" and row["condition_status"] == "none" else "needs_review"
                result["matches"].append(value)
            query = """SELECT i.package_insert_no, i.section_type, i.interaction_key, t.*
                        FROM document_drug dd JOIN sgml_interaction i USING(package_insert_no)
                        JOIN sgml_interaction_target t USING(interaction_key)
                        WHERE dd.yj_code=? AND (t.resolution_status != 'resolved' OR t.condition_status='unresolved')
                        ORDER BY t.target_key"""
            result["unresolved"].extend(dict(r, direction=direction) for r in connection.execute(query, (own,)))
        # No 'safe' result: absence of matches is distinct from missing coverage.
        result["has_unassessed_scope"] = bool(result["unresolved"]) or any(
            not c["documents"] or any(d["extraction_status"] != "ok" for d in c["documents"]) for c in result["coverage"])
        return result
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "resolve"):
        p = sub.add_parser(command)
        p.add_argument("--output-dir", required=True)
        p.add_argument("--aliases")
        p.add_argument("--previous", help="Previous output directory for count differences")
        if command == "build":
            p.add_argument("--config", default="config.json")
            p.add_argument("--limit", type=int, default=100, help="Document count; 0 means all")
            p.add_argument("--package-insert-no", action="append")
        else:
            p.add_argument("--input-dir", required=True)
    p = sub.add_parser("check")
    p.add_argument("--database", required=True)
    p.add_argument("--a-yj", required=True)
    p.add_argument("--b-yj", required=True)
    args = parser.parse_args()
    if getattr(args, "limit", 0) < 0:
        parser.error("--limit must be nonnegative")
    try:
        if args.command == "check":
            print(json.dumps(check_pair(args.database, args.a_yj, args.b_yj), ensure_ascii=False, indent=2))
        else:
            build(args)
    except Exception as exc:
        # Database exceptions can contain connection strings. Do not print secrets.
        if exc.__class__.__module__.startswith("psycopg2"):
            print(f"Database operation failed ({type(exc).__name__}, SQLSTATE={getattr(exc, 'pgcode', None)}).", file=sys.stderr)
        else:
            print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
