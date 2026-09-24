"""Run only against an explicitly supplied, disposable PostgreSQL database.

SGML_DATE_TEST_DSN must point at a disposable database, never the application DB.
All fixtures are isolated in a randomly named schema and removed afterwards.
"""
import importlib.util
import csv
from datetime import date
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

DSN = os.environ.get('SGML_DATE_TEST_DSN')


@unittest.skipUnless(DSN, 'disposable PostgreSQL DSN not supplied')
class PipelineTests(unittest.TestCase):
    def test_import_preserves_application_timestamp_and_csv_dates(self):
        import psycopg2
        from psycopg2.extensions import parse_dsn

        root = Path(__file__).resolve().parents[1]
        schema = 'date_import_' + uuid.uuid4().hex[:12]
        conn = psycopg2.connect(DSN)
        previous_cwd = Path.cwd()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA {schema}')
            conn.commit()
            with tempfile.TemporaryDirectory() as tmp:
                os.chdir(tmp)
                folder = Path(tmp) / 'pmda_all_sgml_xml_20260923' / 'SGML_XML'
                (folder / 'drug').mkdir(parents=True)
                (folder / 'drug' / 'p.xml').write_text(
                    '<PackIns xmlns="http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0">'
                    '<PackageInsertNo>p</PackageInsertNo></PackIns>', encoding='utf-8')
                with (folder.parent / 'ファイルリスト.csv').open('w', encoding='cp932', newline='') as stream:
                    writer = csv.writer(stream)
                    writer.writerow(['パス', '添付文書更新日'])
                    writer.writerow(['./SGML_XML/drug', '2026/08/22'])
                Path('config.json').write_text(json.dumps({'DI_folder': str(folder), 'sgml_table': f'{schema}.raw',
                                                         'db': {'password': ''} | parse_dsn(DSN)}), encoding='utf-8')
                spec = importlib.util.spec_from_file_location('date_import_21', root / '21_sgml2rawdata.py')
                mod = importlib.util.module_from_spec(spec)
                with patch('logging.FileHandler', return_value=logging.NullHandler()):
                    spec.loader.exec_module(mod)
                mod.main()
                with conn.cursor() as cur:
                    cur.execute(f'SELECT source_update_date, source_download_date, updated_at IS NOT NULL, prepared_ym FROM {schema}.raw')
                    self.assertEqual(cur.fetchone(), (date(2026, 8, 22), date(2026, 9, 23), True, None))
                os.chdir(previous_cwd)
        finally:
            os.chdir(previous_cwd)
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
            conn.commit()
            conn.close()

    def test_date_gates_women_and_notes(self):
        import psycopg2
        from psycopg2.extensions import parse_dsn
        from _sgml_source_dates import DocumentGate

        root = Path(__file__).resolve().parents[1]
        modules = {}
        for number, name in [(31, 'build_sgml_women_blocks'), (41, 'build_sgml_note_blocks')]:
            spec = importlib.util.spec_from_file_location(f'date_test_{number}', root / f'{number}_{name}.py')
            mod = importlib.util.module_from_spec(spec)
            with patch('logging.FileHandler', return_value=logging.NullHandler()):
                spec.loader.exec_module(mod)
            modules[number] = mod
        schema = 'date_test_' + uuid.uuid4().hex[:12]
        conn = psycopg2.connect(DSN)
        config = {'db': parse_dsn(DSN), 'sgml_table': f'{schema}.raw',
                  'sgml_women_state_table': f'{schema}.women_state',
                  'sgml_women_block_table': f'{schema}.women',
                  'sgml_note_document_state_table': f'{schema}.note_state',
                  'sgml_note_block_table': f'{schema}.note'}
        xml = '''<PackIns xmlns="http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0">
            <PackageInsertNo>p</PackageInsertNo><Pharmacokinetics><Absorption><Detail>original absorption</Detail></Absorption></Pharmacokinetics>
            <UseInSpecificPopulations><UseInPregnant><Detail>original pregnancy</Detail></UseInPregnant></UseInSpecificPopulations>
            </PackIns>'''
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA {schema}')
                # Legacy schema first: migration must work without ALTER on rawdata.
                cur.execute(f'''CREATE TABLE {schema}.raw (package_insert_no text, yj_code text,
                    prepared_ym text, generic_name_ja text, doc_xml xml, raw_xml_path text)''')
                cur.execute(f'INSERT INTO {schema}.raw VALUES (%s,%s,NULL,NULL,%s,%s)',
                            ('p', '', xml, './SGML/pmda_all_sgml_xml_20260923/SGML_XML/p.xml'))
            conn.commit()
            with tempfile.TemporaryDirectory() as tmp:
                config_path = Path(tmp) / 'config.json'

                def run(number, *extra):
                    config_path.write_text(json.dumps(config), encoding='utf-8')
                    with patch.object(sys, 'argv', ['test', '--config', str(config_path), *extra]), patch.dict(os.environ, {'PGPASSWORD': ''}):
                        modules[number].main()

                def texts(table):
                    with conn.cursor() as cur:
                        where = ' WHERE is_current' if table in ('women', 'note') else ''
                        cur.execute(f'SELECT block_text FROM {schema}.{table}{where}')
                        result = [r[0] for r in cur.fetchall()]
                    conn.commit()
                    return result

                for number in (31, 41):
                    run(number)
                with conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE {schema}.raw ADD source_update_date date, ADD source_download_date date')
                    cur.execute(f"UPDATE {schema}.raw SET source_update_date='2026-08-22', source_download_date='2026-10-01', doc_xml=%s", (xml.replace('original', 'changed'),))
                conn.commit()
                # Older than previous download minus one month: retain original blocks.
                for number, table in ((31, 'women'), (41, 'note')):
                    run(number)
                    self.assertTrue(all('original' in t for t in texts(table)))
                # CSV date is now saved. A different date triggers all extractors.
                with conn.cursor() as cur:
                    cur.execute(f"UPDATE {schema}.raw SET source_update_date='2026-08-23'")
                conn.commit()
                for number, table in ((31, 'women'), (41, 'note')):
                    run(number)
                    self.assertTrue(all('changed' in t for t in texts(table)))
                    with patch.object(modules[number], {31: 'extract_population_blocks', 41: 'extract_blocks'}[number], side_effect=AssertionError('must skip')):
                        run(number)
                    with conn.cursor() as cur:
                        cur.execute(f'SELECT count(*) FROM {schema}.{table}_source_state')
                        self.assertEqual(cur.fetchone()[0], 1)
                # Same dates, changed extraction settings: must re-extract.
                config['note_included_top_sections'] = ['Pharmacokinetics']
                run(41)
                self.assertEqual(len(texts('note')), 1)
                # Reset/missing blocks must recover even with identical dates.
                with conn.cursor() as cur:
                    cur.execute(f'DELETE FROM {schema}.note')
                conn.commit()
                run(41)
                self.assertTrue(texts('note'))
                # A rollback also rolls back the date baseline.
                gate = DocumentGate(conn, config, f'{schema}.raw', f'{schema}.note', 'rollback-test', current_only=True)
                gate.success('never_committed', 0)
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM {schema}.note_source_state WHERE package_insert_no='never_committed'")
                    self.assertEqual(cur.fetchone()[0], 0)
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
            conn.commit()
            conn.close()


if __name__ == '__main__':
    unittest.main()
