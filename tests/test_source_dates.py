import csv
from datetime import date
from pathlib import Path
import tempfile
import unittest

from _sgml_source_dates import FileList, DocumentGate, date_unchanged, download_date, months_before


class DateTests(unittest.TestCase):
    def test_calendar_month_and_inclusive_boundary(self):
        self.assertEqual(months_before(date(2024, 3, 31), 1), date(2024, 2, 29))
        self.assertTrue(date_unchanged(date(2026, 8, 22), None, date(2026, 9, 23)))
        self.assertFalse(date_unchanged(date(2026, 8, 23), None, date(2026, 9, 23)))
        self.assertFalse(date_unchanged(date(2026, 9, 23), None, date(2026, 9, 23)))

    def test_saved_csv_dates_and_unknowns(self):
        day = date(2026, 8, 1)
        self.assertTrue(date_unchanged(day, day, None))
        self.assertFalse(date_unchanged(date(2026, 7, 1), day, None))
        self.assertFalse(date_unchanged(None, day, day))
        self.assertFalse(date_unchanged(day, None, None))

    def test_download_date_is_not_import_time(self):
        self.assertEqual(download_date(r'.\SGML\pmda_all_sgml_xml_20260923\SGML_XML\x.xml'), date(2026, 9, 23))
        self.assertIsNone(download_date('./other/20260923/x.xml'))
        self.assertIsNone(download_date('./pmda_all_sgml_xml_20260230/x.xml'))

    def test_csv_encodings_multiline_and_unknown(self):
        for encoding in ('cp932', 'utf-8-sig'):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / 'pmda_all_sgml_xml_20260923'
                folder = root / 'SGML_XML'
                folder.mkdir(parents=True)
                with (root / 'ファイルリスト.csv').open('w', encoding=encoding, newline='') as stream:
                    writer = csv.writer(stream)
                    writer.writerow(['パス', '製造販売業者名等', '添付文書更新日'])
                    writer.writerow(['./SGML_XML/薬A', '会社A\n会社B', '2026/08/01'])
                    writer.writerow(['./SGML_XML/薬B', '会社C', 'bad'])
                listing = FileList({'DI_folder': str(folder)})
                self.assertEqual(listing.updated(folder / '薬A' / 'a.xml'), date(2026, 8, 1))
                self.assertIsNone(listing.updated(folder / '薬B' / 'b.xml'))
                self.assertIsNone(listing.updated(folder / 'unknown.xml'))
                self.assertEqual(listing.downloaded, date(2026, 9, 23))

    def test_gate_force_settings_missing_blocks_and_rollback(self):
        gate = DocumentGate.__new__(DocumentGate)
        gate.enabled, gate.months, gate.version, gate.skipped = True, 1, 'v1', 0
        gate.sources = {'p': (date(2026, 8, 1), date(2026, 9, 23))}
        gate.previous = {'p': (date(2026, 8, 1), date(2026, 9, 1), 'v1', 2)}
        gate.counts = {'p': 2}
        saved = []
        gate.success = lambda *args: saved.append(args)
        self.assertTrue(gate.skip('p'))
        self.assertEqual(saved, [('p', 2)])
        self.assertFalse(gate.skip('p', force=True))
        gate.version = 'v2'
        self.assertFalse(gate.skip('p'))
        gate.version, gate.counts['p'] = 'v1', 1
        self.assertFalse(gate.skip('p'))
        gate.counts['p'] = 2
        gate.sources['p'] = (date(2026, 8, 1), date(2026, 8, 31))
        self.assertFalse(gate.skip('p'))
        self.assertFalse(gate.skip('new'))


if __name__ == '__main__':
    unittest.main()
