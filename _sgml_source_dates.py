# -*- coding: utf-8 -*-
"""PMDA配布日・CSV更新日による、LLM前段の文書単位差分判定。"""
from __future__ import annotations

import calendar
import csv
import hashlib
import io
import json
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path

log = logging.getLogger(__name__)


def absolute_path(path):
    # UNC上の全CSV行にresolve()すると、行ごとにネットワークI/Oが発生する。
    # CSVは参照先を開かないため、ここでは字句的な絶対パスで対応付ける。
    return Path(os.path.abspath(path))


def as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value).replace('/', '-'))
    except ValueError:
        return None


def download_date(path):
    match = re.search(r'(?:^|[/\\])pmda_all_sgml_xml_(\d{8})(?:[/\\]|$)', str(path))
    if not match:
        return None
    try:
        return datetime.strptime(match[1], '%Y%m%d').date()
    except ValueError:
        return None


def months_before(value, months):
    year, month = divmod(value.year * 12 + value.month - 1 - months, 12)
    month += 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def date_unchanged(current_update, previous_update, previous_download, months=1):
    """不明は省略しない。同日の更新も移行時の更新候補に含める。"""
    if current_update is None:
        return False
    if previous_update is not None:
        return current_update == previous_update
    return previous_download is not None and current_update < months_before(previous_download, months)


class FileList:
    """CSVは実行コードとして扱わず、パスと更新日のみを取り込む。"""
    def __init__(self, config):
        folder = Path(config.get('DI_folder') or './drug_information')
        self.downloaded = download_date(folder)
        explicit = config.get('sgml_file_list')
        candidates = [Path(explicit)] if explicit else [folder / 'ファイルリスト.csv', folder.parent / 'ファイルリスト.csv']
        self.dates = {}
        source = next((p for p in candidates if p.is_file()), None)
        if source is None:
            log.warning('ファイルリスト.csvが見つかりません。更新日不明としてハッシュ判定を使用します')
            return
        data = source.read_bytes()
        try:
            text = data.decode('utf-8-sig')
        except UnicodeDecodeError:
            text = data.decode('cp932')
        reader = csv.DictReader(io.StringIO(text, newline=''))
        if not {'パス', '添付文書更新日'}.issubset(reader.fieldnames or []):
            raise ValueError('ファイルリスト.csvにパス・添付文書更新日がありません')
        for row in reader:
            relative = row.get('パス', '')
            if not relative:
                continue
            target = absolute_path(source.parent / relative)
            if not target.is_relative_to(absolute_path(source.parent)):
                raise ValueError('ファイルリスト.csvのパスが配布フォルダ外です')
            key = os.path.normcase(str(target))
            value = as_date(row.get('添付文書更新日'))
            if key in self.dates and self.dates[key] != value:
                value = None
            self.dates[key] = value
        log.info('ファイルリスト読込 paths=%s download_date=%s', len(self.dates), self.downloaded)

    def updated(self, xml_path):
        path = absolute_path(xml_path)
        for target in (path, path.parent):
            key = os.path.normcase(str(target))
            if key in self.dates:
                return self.dates[key]
        return None


class DocumentGate:
    """ブロック変更と同じトランザクションで成功時だけ基準を保存する。

    CSVを直接読み直さず、21がXMLと一緒に保存したメタデータを使う。
    旧rawdataは列の有無を確認し、列追加なしに読める。取込日時は配布日の代用にしない。
    """
    def __init__(self, conn, config, source_table, block_table, version, current_only=False):
        for name in (source_table, block_table):
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*', name):
                raise ValueError('schema.table形式が必要です')
        self.conn = conn
        self.table = block_table + '_source_state'
        if len(self.table.split('.')[-1]) > 63:
            raise ValueError('更新状態テーブル名が長すぎます')
        self.version = hashlib.sha256(json.dumps([source_table, version], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        self.enabled = config.get('sgml_use_file_list_dates', True)
        self.months = config.get('sgml_update_lookback_months', 1)
        if type(self.months) is not int or not 0 <= self.months <= 120:
            raise ValueError('sgml_update_lookback_months は0～120の整数で指定してください')
        with conn.cursor() as cur:
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {self.table} (
                package_insert_no text PRIMARY KEY,
                source_update_date date,
                source_download_date date,
                extractor_signature text NOT NULL,
                block_count integer NOT NULL,
                last_success_at timestamptz NOT NULL DEFAULT now()
            )''')
            schema, table = source_table.split('.')
            cur.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s', (schema, table))
            columns = {r[0] for r in cur.fetchall()}
            updated_column = 'source_update_date' if 'source_update_date' in columns else 'NULL::date'
            downloaded_column = 'source_download_date' if 'source_download_date' in columns else 'NULL::date'
            cur.execute(f'''SELECT package_insert_no,
                {updated_column}, {downloaded_column}, raw_xml_path FROM {source_table}''')
            self.sources = {}
            for package, updated, downloaded, path in cur.fetchall():
                value = (as_date(updated), as_date(downloaded) or download_date(path))
                if package in self.sources and self.sources[package] != value:
                    value = (None, None)
                self.sources[package] = value
            cur.execute(f'SELECT package_insert_no, source_update_date, source_download_date, extractor_signature, block_count FROM {self.table}')
            self.previous = {r[0]: r[1:] for r in cur.fetchall()}
            where = ' WHERE is_current' if current_only else ''
            cur.execute(f'SELECT package_insert_no, count(*) FROM {block_table}{where} GROUP BY package_insert_no')
            self.counts = dict(cur.fetchall())
        conn.commit()
        self.skipped = 0

    def compatible(self, package):
        old = self.previous.get(package)
        return bool(old and old[2] == self.version and self.counts.get(package, 0) == old[3])

    def skip(self, package, force=False):
        old = self.previous.get(package)
        if force or not self.enabled or not self.compatible(package):
            return False
        updated, downloaded = self.sources.get(package, (None, None))
        if downloaded and old[1] and downloaded < old[1]:
            return False
        if not date_unchanged(updated, old[0], old[1], self.months):
            return False
        self.skipped += 1
        self.success(package, old[3])
        return True

    def success(self, package, count):
        updated, downloaded = self.sources.get(package, (None, None))
        with self.conn.cursor() as cur:
            cur.execute(f'''INSERT INTO {self.table}
                (package_insert_no, source_update_date, source_download_date, extractor_signature, block_count)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (package_insert_no) DO UPDATE SET
                source_update_date=EXCLUDED.source_update_date,
                source_download_date=EXCLUDED.source_download_date,
                extractor_signature=EXCLUDED.extractor_signature,
                block_count=EXCLUDED.block_count, last_success_at=now()''',
                (package, updated, downloaded, self.version, count))

    def invalidate(self, package):
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self.table} WHERE package_insert_no=%s", (package,))
