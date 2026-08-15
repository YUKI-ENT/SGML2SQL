# `sgml_note` アプリケーション利用ガイド

## 1. 目的

`public.sgml_note` は、PMDA添付文書から抽出し、原文照合などの検証を通過した薬物動態・QT関連情報を保存する公開用テーブルです。

このテーブルは、次の用途を想定しています。

- 薬剤詳細画面での薬物動態ノート表示
- カルテ要約や処方レビューを行うLLMへの根拠情報の注入
- CYP、トランスポーター、排泄経路、腎・肝機能障害時の変化、QT延長情報の構造化検索
- 将来作成する表示用ダイジェストやベクトル検索インデックスの根拠データ

`sgml_note` は、読みやすい文章に統合した最終要約ではなく、原文へ戻れる「原子ファクト層」です。アプリで簡潔に表示する場合は、後述の集約層を別途設けます。

## 2. 対象範囲

現在の`note_type`は次の4種類です。

| note_type | 内容 | 主な利用例 |
|---|---|---|
| `EXCRETION_ELIMINATION` | 尿・糞・胆汁への排泄、腎クリアランス、透析除去性など | 排泄経路の表示、腎排泄性の確認 |
| `METABOLISM_TRANSPORT` | 代謝酵素、基質性、阻害・誘導、トランスポーター | CYP・UGT・P-gpなどの検索 |
| `ORGAN_IMPAIRMENT` | 腎・肝機能低下時の曝露量、半減期、クリアランスの変化 | 腎・肝機能障害時の注意情報表示 |
| `QT_EFFECT` | QT/QTc延長、torsade de pointes | QT関連注意情報の表示 |

相互作用章はこのパイプラインの対象外です。薬剤相互作用は`public.sgml_interaction`を正本として利用します。`sgml_note`だけから併用禁忌や併用注意を判定してはいけません。

## 3. 2026-08-15公開時点のデータ規模

| note_type | facts | packages |
|---|---:|---:|
| `EXCRETION_ELIMINATION` | 16,566 | 5,611 |
| `METABOLISM_TRANSPORT` | 10,227 | 3,455 |
| `ORGAN_IMPAIRMENT` | 9,984 | 3,472 |
| `QT_EFFECT` | 1,434 | 1,025 |

同一内容のブロックが複数の添付文書で共有される場合、1件のLLM解析結果が複数の`package_insert_no`へ展開されます。そのため、解析件数、手動レビュー件数、公開後のpackages件数は一致しないことがあります。

## 4. データフロー

```text
public.sgml_rawdata
    ↓ 41: 意味ブロック化・差分検出
public.sgml_note_document_state
public.sgml_note_block
    ↓ 42: note_type別候補抽出
public.temp_sgml_note_candidate
    ↓ 43: LLM抽出・原文照合・自動検証
public.temp_sgml_note_run
public.temp_sgml_note_fact
    ↓ 43_6 / 43_7: 必要時のみ手動レビュー
public.sgml_note_manual_review
    ↓ 44: モデル優先順位に従って差分公開
public.sgml_note
```

アプリケーションは原則として`public.sgml_note`だけを参照します。`temp_`テーブルは処理履歴、再開、再検証、監査のための中間テーブルであり、通常の画面表示やRAG検索の直接参照先にはしません。

## 5. 公開テーブル`public.sgml_note`

### 5.1 識別・薬剤情報

| カラム | 型 | 内容 |
|---|---|---|
| `note_id` | `bigserial` | 公開ファクトのID |
| `package_insert_no` | `text` | 添付文書パッケージ識別子 |
| `prepared_ym` | `text` | 添付文書の作成年月 |
| `generic_name_ja` | `text` | 一般名等の表示名 |

アプリ側では、処方薬・医薬品コードから現在有効な`package_insert_no`へ到達できる対応表を別途確保します。名称のあいまい一致だけで薬剤を結び付けないでください。

### 5.2 構造化ファクト

| カラム | 型 | 内容 |
|---|---|---|
| `note_type` | `text` | 上記4種類のテーマ |
| `relation_type` | `text` | ファクトの関係分類 |
| `subject_type` | `text` | 現在は原則`DRUG` |
| `target_code` | `text` | CYP、UGT、QTなど検索用の正規化コード。NULLの場合あり |
| `target_name` | `text` | 対象名の表示値。空欄の場合あり |
| `polarity` | `text` | `POSITIVE`又は`NEGATIVE`。臨床上の安全・危険を直接表す値ではない |
| `certainty` | `text` | 現在の公開対象は原則`EXPLICIT` |
| `note_text` | `text` | 原文に基づく短いファクト記述 |
| `details_json` | `jsonb` | 割合、時間範囲、投与条件などの補助情報。項目はファクトごとに異なる |

`target_code`及び`details_json`は、すべての行で同じ粒度が保証される固定スキーマではありません。検索条件に使う場合も、`note_text`と`evidence_text`へ戻れる設計にします。

### 5.3 根拠・出典

| カラム | 型 | 内容 |
|---|---|---|
| `evidence_text` | `text` | 添付文書から抽出した連続する根拠原文 |
| `source_block_id` | `bigint` | `sgml_note_block.block_id`への参照用ID |
| `section_code` | `text` | 章・節番号 |
| `section_type` | `text` | XML上の節種別 |
| `heading_path` | `text` | 見出し階層 |
| `source_hash` | `text` | 公開元ブロックの内容ハッシュ |

画面では通常`note_text`を表示し、「根拠を見る」で`evidence_text`、章番号、見出しを展開します。RAG回答でも、最終的な根拠提示は`evidence_text`へ戻します。

### 5.4 バージョン・検証状態

| カラム | 型 | 内容 |
|---|---|---|
| `definition_version` | `text` | note定義の版 |
| `prompt_version` | `text` | LLMプロンプトの版 |
| `model_name` | `text` | 採用されたモデル名又は`human-review` |
| `fact_hash` | `text` | ファクト内容の安定ハッシュ |
| `review_status` | `text` | `AUTO_VALIDATED`又は`HUMAN_REVIEWED` |
| `is_current` | `boolean` | 現在有効な公開行かどうか |
| `first_published_at` | `timestamptz` | 初回公開日時 |
| `last_published_at` | `timestamptz` | 最終公開日時 |
| `superseded_at` | `timestamptz` | 旧版化した日時 |

通常の参照では必ず`WHERE is_current`を付けます。過去行は監査や差分確認用であり、現在のアプリ表示へ混在させません。

`HUMAN_REVIEWED`は人手で採否を確認したこと、`AUTO_VALIDATED`はLLM応答が機械検証を通過したことを示します。これらは有害性の強さや臨床エビデンスレベルを示すスコアではありません。

## 6. relation_type

### 6.1 EXCRETION_ELIMINATION

- `URINARY_EXCRETION`
- `URINARY_RECOVERY`
- `FECAL_RECOVERY`
- `BILIARY_EXCRETION`
- `RENAL_CLEARANCE`
- `DIALYZABLE`
- `NOT_DIALYZABLE`
- `OTHER_ELIMINATION`

### 6.2 METABOLISM_TRANSPORT

- `METABOLIZED_BY`
- `METABOLISM_PATHWAY`
- `HEPATIC_METABOLISM`
- `CLEARANCE_DEPENDS_ON_HEPATIC_BLOOD_FLOW`
- `SUBSTRATE_OF`
- `INHIBITS`
- `INDUCES`
- `NOT_METABOLIZED_BY`
- `NOT_SUBSTRATE_OF`
- `NOT_INHIBITS`
- `NOT_INDUCES`
- `EXPOSURE_INCREASED_BY_INHIBITION`
- `EXPOSURE_DECREASED_BY_INDUCTION`

### 6.3 ORGAN_IMPAIRMENT

- `EXPOSURE_INCREASES_WITH_RENAL_IMPAIRMENT`
- `EXPOSURE_DECREASES_WITH_RENAL_IMPAIRMENT`
- `EXPOSURE_INCREASES_WITH_HEPATIC_IMPAIRMENT`
- `EXPOSURE_DECREASES_WITH_HEPATIC_IMPAIRMENT`
- `HALF_LIFE_CHANGES_WITH_IMPAIRMENT`
- `CLEARANCE_CHANGES_WITH_IMPAIRMENT`
- `ELIMINATION_DELAYED_WITH_RENAL_IMPAIRMENT`
- `METABOLISM_DELAYED_WITH_HEPATIC_IMPAIRMENT`
- `PROTEIN_BINDING_DECREASES_WITH_HEPATIC_IMPAIRMENT`
- `NO_MEANINGFUL_PK_CHANGE`

### 6.4 QT_EFFECT

- `QT_PROLONGATION`
- `TORSADES_DE_POINTES`

relation_typeの追加・変更時は`sgml_note_definitions.json`の`definition_version`も更新し、41～44の差分処理と再検証を行います。

## 7. 基本検索SQL

### 7.1 1添付文書の全ノート

```sql
SELECT
    note_id,
    note_type,
    relation_type,
    target_code,
    target_name,
    note_text,
    details_json,
    evidence_text,
    section_code,
    heading_path,
    review_status
FROM public.sgml_note
WHERE is_current
  AND package_insert_no = $1
ORDER BY
    CASE note_type
        WHEN 'QT_EFFECT' THEN 1
        WHEN 'ORGAN_IMPAIRMENT' THEN 2
        WHEN 'METABOLISM_TRANSPORT' THEN 3
        WHEN 'EXCRETION_ELIMINATION' THEN 4
        ELSE 9
    END,
    relation_type,
    target_code NULLS LAST,
    note_id;
```

### 7.2 複数処方薬のノート

```sql
SELECT
    package_insert_no,
    generic_name_ja,
    note_type,
    relation_type,
    target_code,
    target_name,
    note_text,
    evidence_text,
    review_status
FROM public.sgml_note
WHERE is_current
  AND package_insert_no = ANY($1::text[])
ORDER BY package_insert_no, note_type, relation_type, target_code NULLS LAST;
```

### 7.3 CYP3A関連

```sql
SELECT
    package_insert_no,
    generic_name_ja,
    relation_type,
    target_code,
    target_name,
    note_text,
    evidence_text
FROM public.sgml_note
WHERE is_current
  AND note_type = 'METABOLISM_TRANSPORT'
  AND target_code IN ('CYP3A', 'CYP3A4', 'CYP3A5')
ORDER BY generic_name_ja, relation_type;
```

### 7.4 QT関連薬の存在確認

```sql
SELECT
    package_insert_no,
    generic_name_ja,
    bool_or(relation_type = 'QT_PROLONGATION') AS has_qt_prolongation,
    bool_or(relation_type = 'TORSADES_DE_POINTES') AS has_torsades
FROM public.sgml_note
WHERE is_current
  AND note_type = 'QT_EFFECT'
  AND package_insert_no = ANY($1::text[])
GROUP BY package_insert_no, generic_name_ja;
```

この結果は「QT関連記載が存在する」というフラグです。発現頻度、禁忌、個々の患者に対する投与可否まで表すものではありません。

### 7.5 根拠を含むJSON形式

```sql
SELECT
    package_insert_no,
    jsonb_agg(
        jsonb_build_object(
            'note_id', note_id,
            'note_type', note_type,
            'relation_type', relation_type,
            'target_code', target_code,
            'target_name', target_name,
            'claim', note_text,
            'details', details_json,
            'evidence', evidence_text,
            'section_code', section_code,
            'review_status', review_status
        )
        ORDER BY note_type, relation_type, note_id
    ) AS notes
FROM public.sgml_note
WHERE is_current
  AND package_insert_no = ANY($1::text[])
GROUP BY package_insert_no;
```

## 8. アプリケーション表示案

薬剤詳細画面では、以下の4区分を折りたたみ表示します。

1. QT関連
2. 腎・肝機能障害時
3. 代謝酵素・トランスポーター
4. 排泄・透析除去性

各行には`note_text`を表示し、必要に応じて以下を展開します。

- 根拠原文：`evidence_text`
- 出典位置：`section_code`、`heading_path`
- 抽出経路：`review_status`、`model_name`
- 構造化詳細：`details_json`

QT情報は注意喚起アイコンの対象にできますが、`ORGAN_IMPAIRMENT`や`EXCRETION_ELIMINATION`の存在だけで赤・黄・青などの安全判定を付けないでください。曝露量変化の記載は、そのまま禁忌、減量又は安全を意味しません。

## 9. RAGでの利用

### 9.1 推奨検索順序

1. 処方薬から`package_insert_no`を確定する
2. `package_insert_no`と`note_type`による構造化SQL検索を行う
3. 必要なファクトだけをLLMコンテキストへ入れる
4. 回答で使用した`note_id`を保存する
5. ユーザーへ根拠を示す場合は`evidence_text`へ戻る

薬剤が既に特定されている場合、ベクトル検索よりSQL完全一致を優先します。ベクトル検索は、自由文のカルテから関連テーマを探す場合や、薬剤横断のあいまい検索を行う場合の補助にします。

### 9.2 LLMへ渡す例

```json
{
  "drug": {
    "package_insert_no": "...",
    "generic_name_ja": "アムロジピン"
  },
  "pharmacokinetic_notes": [
    {
      "note_id": 123,
      "note_type": "EXCRETION_ELIMINATION",
      "relation_type": "URINARY_EXCRETION",
      "claim": "尿中未変化体排泄率は約8%であった",
      "evidence": "添付文書の根拠原文",
      "review_status": "AUTO_VALIDATED"
    }
  ]
}
```

LLMには次の制約を与えます。

- 入力されたファクトにない数値、因果関係、投与判断を追加しない
- 未変化体、総放射能、代謝物の値を混同しない
- ヒトと動物、単回と反復、製剤、投与経路、正常皮膚と損傷皮膚などの条件を混同しない
- 「主に尿中」「主に糞中」は原文又は比較可能な同一試験の値で裏付けられる場合だけ使用する
- 用量調節、中止、禁忌を独自に推奨しない
- 根拠不足の場合は判断不能と明記する
- 回答に使用した`note_id`を返す

## 10. 原子ファクトと表示用要約の分離

`sgml_note`の複数行をそのまま画面に並べると、同一試験の尿中・糞中排泄や複数投与条件が冗長に見えることがあります。一方、公開時に一つの文章へ潰すと、測定対象や条件を混同する危険があります。

そのため、既存の`sgml_note`を変更せず、将来は表示・RAG用の集約テーブルを追加する構成を推奨します。

```sql
CREATE TABLE public.sgml_note_digest (
    digest_id           bigserial PRIMARY KEY,
    package_insert_no   text NOT NULL,
    note_type           text NOT NULL,
    summary_text        text NOT NULL,
    summary_json        jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_note_ids     bigint[] NOT NULL,
    source_hash         text NOT NULL,
    summary_version     text NOT NULL,
    review_status       text NOT NULL,
    is_current          boolean NOT NULL DEFAULT true,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (package_insert_no, note_type, summary_version)
);
```

これは将来案であり、現時点では未実装です。`source_note_ids`を必須にし、要約から必ず原子ファクトと原文へ戻れるようにします。

排泄要約では最低限、次の軸を分離します。

- 測定対象：未変化体、総放射能、代謝物
- 排泄経路：尿、糞、胆汁、呼気、透析
- 時間範囲
- 単回・反復投与
- 用量、剤形、投与経路
- ヒト・動物、対象集団

要約生成にLLMを使用する場合も、入力は`sgml_note`の選択済みファクトに限定し、数値が入力内に存在すること、経路が逆転していないこと、`source_note_ids`が実在することをコードで再検証します。

## 11. ベクトル検索

初期実装では`sgml_note`への構造化SQL検索だけで開始できます。ベクトル検索を追加する場合は、原子ファクトを無差別にベクトル化するより、次の順序を推奨します。

1. `sgml_note_digest`を作成する
2. `summary_text`をベクトル化する
3. ベクトル検索結果から`source_note_ids`を取得する
4. `sgml_note`の原文根拠を再取得してLLMへ渡す

PostgreSQLでは`pgvector`を利用できますが、ベクトル類似度だけで医療上の最終判断を行わないでください。薬剤コード、`package_insert_no`、`note_type`などの構造化条件を併用します。

## 12. 更新・再公開

通常の更新手順は次の通りです。

```bash
python3 41_build_sgml_note_blocks.py
python3 42_build_sgml_note_candidates.py
python3 43_extract_sgml_notes.py --run-status new
```

未解決分を必要に応じて別モデル又は手動レビューで処理した後、手動判断を最優先にして一括公開します。

```bash
python3 44_publish_sgml_notes.py \
  --model human-review \
  --fallback-model gemma4:31b-cloud \
  --fallback-model gemma4:12b \
  --prompt-version sgml-note-v4 \
  --dry-run

python3 44_publish_sgml_notes.py \
  --model human-review \
  --fallback-model gemma4:31b-cloud \
  --fallback-model gemma4:12b \
  --prompt-version sgml-note-v4
```

`human-review`の`EXCLUDE`は成功済み・ファクト0件として扱われます。これにより、下位モデルのファクトが再公開されることを防ぎます。

## 13. 運用確認SQL

### 13.1 現在有効な件数

```sql
SELECT
    note_type,
    count(*) AS facts,
    count(DISTINCT package_insert_no) AS packages
FROM public.sgml_note
WHERE is_current
GROUP BY note_type
ORDER BY note_type;
```

### 13.2 モデル・検証経路別

```sql
SELECT
    note_type,
    model_name,
    review_status,
    count(*) AS facts,
    count(DISTINCT package_insert_no) AS packages
FROM public.sgml_note
WHERE is_current
GROUP BY note_type, model_name, review_status
ORDER BY note_type, review_status, model_name;
```

### 13.3 根拠のない公開行がないこと

```sql
SELECT count(*) AS invalid_rows
FROM public.sgml_note
WHERE is_current
  AND (
      note_text IS NULL OR btrim(note_text) = ''
      OR evidence_text IS NULL OR btrim(evidence_text) = ''
      OR review_status NOT IN ('AUTO_VALIDATED', 'HUMAN_REVIEWED')
  );
```

期待値は`0`です。

## 14. 医療利用上の制限

- `sgml_note`は添付文書情報の検索・提示を補助するものであり、単独で診断、処方変更、用量調節を決定するものではありません。
- 情報がないことを「問題なし」「安全」と解釈しないでください。
- `polarity=NEGATIVE`は安全を意味しません。例えば非基質、非阻害、曝露低下など、relation_typeに対する否定方向を表します。
- QT関連ファクトの存在は注意情報ですが、それだけで禁忌や中止を意味しません。
- 腎・肝機能障害時の曝露変化だけから用量調節を推定しないでください。用法用量、特定背景患者、禁忌等の原文も確認します。
- 同じ薬剤でも剤形、投与経路、含量、添付文書改訂版が異なる場合があります。必ず処方薬と対応する現在の`package_insert_no`を使用します。
- RAG出力には、使用した`note_id`及び根拠原文を追跡できる情報を残します。

## 15. アプリ実装の推奨順序

1. 医薬品コードから現在の`package_insert_no`への対応を確認する
2. 薬剤詳細画面に4区分の原子ファクト表示を追加する
3. 根拠原文の展開表示を追加する
4. 複数処方薬を一括取得するAPIを追加する
5. カルテ要約・処方レビューLLMへ、必要なnote_typeだけを構造化注入する
6. 回答と`note_id`の対応をログへ保存する
7. 排泄から`sgml_note_digest`の集約処理を試作する
8. 必要性を確認した後に`pgvector`による自由文検索を追加する

最初から全ファクトをベクトル検索へ流すより、薬剤を確定したSQL検索と根拠表示を先に実装する方が、動作確認と医療上の監査が容易です。
