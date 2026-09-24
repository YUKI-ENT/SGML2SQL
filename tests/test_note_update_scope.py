import importlib.util
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

from _sgml_note_update_scope import needs_update


class DateScopeTests(unittest.TestCase):
    def test_dates_and_pending_ignore_model(self):
        current = (date(2026, 1, 1), date(2026, 9, 23))
        self.assertFalse(needs_update(current, (*current, False), None, 1))
        self.assertTrue(needs_update(current, (*current, True), None, 1))
        self.assertTrue(needs_update(current, (*current, False), None, 1, True))
        self.assertTrue(needs_update((date(2026, 9, 1), current[1]), (*current, False), None, 1))
        self.assertTrue(needs_update((None,current[1]), (*current,False), None, 1))
        self.assertTrue(needs_update(current, None, None, 1))

    def test_migration_boundary(self):
        baseline = date(2026, 8, 17)
        self.assertFalse(needs_update((date(2026, 7, 16), None), None, baseline, 1))
        self.assertTrue(needs_update((date(2026, 7, 17), None), None, baseline, 1))


@unittest.skipUnless(os.environ.get('SGML_DATE_TEST_DSN'), 'disposable database required')
class PipelineScopeTests(unittest.TestCase):
    def test_preview_resume_models_and_publication(self):
        import psycopg2
        from psycopg2.extensions import parse_dsn
        root = Path(__file__).resolve().parents[1]
        modules = {}
        for num, name in [(43,'extract'),(44,'publish')]:
            spec = importlib.util.spec_from_file_location(f'note_scope_test_{num}',root / f'{num}_{name}_sgml_notes.py')
            mod = importlib.util.module_from_spec(spec)
            with patch('logging.FileHandler',return_value=logging.NullHandler()):spec.loader.exec_module(mod)
            modules[num] = mod
        conn = psycopg2.connect(os.environ['SGML_DATE_TEST_DSN'])
        schema = 'note_scope_' + uuid.uuid4().hex[:10]
        config = {'db':parse_dsn(os.environ['SGML_DATE_TEST_DSN']),
                  'sgml_table':f'{schema}.raw','sgml_note_block_table':f'{schema}.blocks',
                  'temp_sgml_note_candidate_table':f'{schema}.candidates',
                  'temp_sgml_note_run_table':f'{schema}.runs','temp_sgml_note_fact_table':f'{schema}.facts',
                  'sgml_note_table':f'{schema}.notes','sgml_note_update_state_table':f'{schema}.state',
                  'note_baseline_download_date':'2026-08-17','note_llm_wait':0,
                  'sgml_note_definitions':str(root / 'sgml_note_definitions.json')}
        try:
            with conn.cursor() as cur:
                cur.execute(f'''CREATE SCHEMA {schema};
                  CREATE TABLE {schema}.raw (package_insert_no text,source_update_date date,source_download_date date);
                  INSERT INTO {schema}.raw VALUES ('old','2026-01-01','2026-09-23'),('changed','2026-09-01','2026-09-23');
                  CREATE TABLE {schema}.blocks (block_id bigint,package_insert_no text,content_hash text,
                    block_text text, section_code text,section_type text,heading_path text,generic_name_ja text,
                    prepared_ym text,is_current boolean);
                  INSERT INTO {schema}.blocks VALUES
                    (1,'old','old-hash','尿中に排泄される。','16.5','Excretion','16.5',NULL,NULL,true),
                    (2,'changed','changed-hash','糞中に排泄される。','16.5','Excretion','16.5',NULL,NULL,true);
                  CREATE TABLE {schema}.candidates (candidate_id bigint,block_id bigint,package_insert_no text,
                    content_hash text,note_type text,definition_version text,is_current boolean);
                  INSERT INTO {schema}.candidates VALUES
                    (1,1,'old','old-hash','EXCRETION_ELIMINATION','excretion-v3',true),
                    (2,2,'changed','changed-hash','EXCRETION_ELIMINATION','excretion-v3',true),
                    (3,2,'changed','changed-hash','EXCRETION_ELIMINATION','excretion-v3',true)''')
            conn.commit()
            modules[44].create_table(conn,f'{schema}.notes')
            with conn.cursor() as cur:
                for p in ('old','changed'):
                    cur.execute(f'''INSERT INTO {schema}.notes
                      (package_insert_no,note_type,relation_type,subject_type,polarity,certainty,note_text,
                       evidence_text,source_block_id,source_hash,definition_version,prompt_version,model_name,fact_hash,review_status)
                      VALUES (%s,'EXCRETION_ELIMINATION','URINARY_EXCRETION','DRUG','POSITIVE','EXPLICIT','old note',
                      'old evidence',1,'old-hash','excretion-v3','sgml-note-v4','old-model',%s,'AUTO_VALIDATED')''',(p,p+'-fact'))
            conn.commit()
            with tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'config.json';path.write_text(json.dumps(config))
                def run(n,*args):
                    with patch.object(sys,'argv',['test','--config',str(path),'--note-type','EXCRETION_ELIMINATION','--model','new-model',*args]),patch.dict(os.environ,{'PGPASSWORD':''}):
                        modules[n].main()
                # Preview does not create persistent tables or call the model, even on first run.
                with patch.object(modules[43],'call_ollama',side_effect=AssertionError('dry run')):
                    run(43,'--dry-run')
                with conn.cursor() as cur:
                    cur.execute('SELECT to_regclass(%s)',(f'{schema}.state',));self.assertIsNone(cur.fetchone()[0])
                conn.commit()
                # Failure stays pending and prevents publication; old documents are never sent.
                with patch.object(modules[43],'call_ollama',side_effect=ValueError('test failure')) as call:
                    run(43,'--max-retries','0');self.assertEqual(call.call_count,1)
                with self.assertRaises(RuntimeError):run(44)
                # Partial publication must not acknowledge a failed pair.
                run(44,'--publish-partial')
                with conn.cursor() as cur:
                    cur.execute(f"SELECT pending FROM {schema}.state WHERE package_insert_no='changed'")
                    self.assertTrue(cur.fetchone()[0])
                conn.commit()
                with patch.object(modules[43],'call_ollama',return_value=('{"facts":[]}',{})) as call:
                    run(43);self.assertEqual(call.call_count,1)
                # Success is cached before publication; date scope is still pending.
                with patch.object(modules[43],'call_ollama',side_effect=AssertionError('cache hit')):run(43)
                run(44,'--dry-run')
                with conn.cursor() as cur:
                    cur.execute(f"SELECT pending FROM {schema}.state WHERE package_insert_no='changed'")
                    self.assertTrue(cur.fetchone()[0])
                conn.commit()
                run(44)
                with conn.cursor() as cur:
                    cur.execute(f'SELECT package_insert_no,model_name FROM {schema}.notes WHERE is_current')
                    self.assertEqual(cur.fetchall(),[('old','old-model')])
                conn.commit()
                # New model AND prompt cannot reopen unchanged documents after publication.
                with patch.object(modules[43],'call_ollama',side_effect=AssertionError('unchanged document')):
                    run(43,'--model','third-model','--prompt-version','new-prompt')
                # Removed chapter: no candidates, but changed document must remove old publication.
                with conn.cursor() as cur:
                    cur.execute(f"UPDATE {schema}.raw SET source_update_date='2026-09-18' WHERE package_insert_no='old'")
                    cur.execute(f"UPDATE {schema}.candidates SET is_current=false WHERE package_insert_no='old'")
                conn.commit()
                run(44)
                with conn.cursor() as cur:
                    cur.execute(f'SELECT count(*) FROM {schema}.notes WHERE is_current');self.assertEqual(cur.fetchone()[0],0)
        finally:
            conn.rollback()
            with conn.cursor() as cur:cur.execute(f'DROP SCHEMA {schema} CASCADE')
            conn.commit();conn.close()
