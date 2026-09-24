"""Persistent document/theme update scope shared by extraction and publication."""
import logging

from psycopg2.extras import execute_values

from _sgml_note_common import checked_table_name
from _sgml_source_dates import as_date, date_unchanged

log = logging.getLogger(__name__)


def state_table_name(config):
    return checked_table_name(config.get('sgml_note_update_state_table',
                              'public.sgml_note_update_state'), 'sgml_note_update_state_table')


def needs_update(current, previous, baseline, months, force=False):
    if force:
        return True
    updated, downloaded = current
    if previous is None:
        return not date_unchanged(updated, None, baseline, months)
    old_update, old_download, pending = previous
    if downloaded and old_download and downloaded < old_download:
        return True
    if not date_unchanged(updated, old_update, old_download, months):
        return True
    return pending


def prepare_scope(conn, config, note_types, package=None, force=False, dry_run=False):
    """Freeze scope in a temp table; pending survives retries/model changes until 44."""
    state = state_table_name(config)
    source = checked_table_name(config.get('sgml_table', 'public.sgml_rawdata'), 'sgml_table')
    baseline_value = config.get('note_baseline_download_date')
    baseline = as_date(baseline_value)
    if baseline_value and baseline is None:
        raise ValueError('note_baseline_download_date must be YYYY-MM-DD')
    months = config.get('sgml_update_lookback_months', 1)
    if type(months) is not int or not 0 <= months <= 120:
        raise ValueError('sgml_update_lookback_months must be between 0 and 120')
    with conn.cursor() as cur:
        cur.execute('SELECT to_regclass(%s)', (state,))
        previous = {}
        if cur.fetchone()[0]:
            cur.execute(f'SELECT package_insert_no,note_type,source_update_date,source_download_date,pending FROM {state}')
            previous = {(p, t): (u, d, pending) for p, t, u, d, pending in cur.fetchall()}
        initialized_types = {t for p, t in previous}
        cur.execute(f'SELECT package_insert_no,source_update_date,source_download_date FROM {source}')
        sources = {}
        for p, u, d in cur.fetchall():
            value = (u, d)
            if p in sources and sources[p] != value:
                value = (None, None)
            sources[p] = value
        # Seed all documents for selected themes even during a single-document run.
        rows = [(p, t, u, d, needs_update((u, d), previous.get((p, t)),
                    baseline if t not in initialized_types else None, months,
                    force and (package is None or package == p)))
                for p, (u, d) in sources.items() for t in note_types]
        if not dry_run:
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {state} (
                package_insert_no text NOT NULL, note_type text NOT NULL,
                source_update_date date, source_download_date date,
                pending boolean NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (package_insert_no,note_type))''')
            if rows:
                execute_values(cur, f'''INSERT INTO {state}
                    (package_insert_no,note_type,source_update_date,source_download_date,pending) VALUES %s
                    ON CONFLICT (package_insert_no,note_type) DO UPDATE SET
                    source_update_date=EXCLUDED.source_update_date,
                    source_download_date=EXCLUDED.source_download_date,
                    pending=EXCLUDED.pending,updated_at=now()''', rows)
        cur.execute('''CREATE TEMP TABLE note_update_scope (
            package_insert_no text, note_type text, source_update_date date,
            source_download_date date, pending boolean,
            PRIMARY KEY (package_insert_no,note_type)) ON COMMIT PRESERVE ROWS''')
        selected = [r for r in rows if package is None or r[0] == package]
        if selected:
            execute_values(cur, 'INSERT INTO note_update_scope VALUES %s', selected)
    if not dry_run:
        conn.commit()
    active = {r[0] for r in selected if r[4]}
    log.info('文書更新日による絞込 documents=%s selected=%s skipped=%s baseline=%s',
             len({r[0] for r in selected}), len(active), len({r[0] for r in selected})-len(active), baseline)
    return state


def scope_filter(alias):
    return (f'EXISTS (SELECT 1 FROM note_update_scope scope '
            f'WHERE scope.package_insert_no={alias}.package_insert_no '
            f'AND scope.note_type={alias}.note_type AND scope.pending)')


def finish_scope(conn, state, incomplete):
    """Advance only fully published pairs, atomically with public facts."""
    failed = {(r['package_insert_no'], r['note_type']) for r in incomplete}
    with conn.cursor() as cur:
        cur.execute('SELECT package_insert_no,note_type,source_update_date,source_download_date FROM note_update_scope WHERE pending')
        for p, t, u, d in cur.fetchall():
            if (p, t) not in failed:
                cur.execute(f'''UPDATE {state} SET pending=false,updated_at=now()
                    WHERE package_insert_no=%s AND note_type=%s
                    AND source_update_date IS NOT DISTINCT FROM %s
                    AND source_download_date IS NOT DISTINCT FROM %s''', (p,t,u,d))
