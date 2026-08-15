# 妊婦・授乳データ アプリケーション利用ガイド

## 1. 目的

31～34系パイプラインは、PMDA添付文書の妊婦・授乳に関する記載を、原文を保持した文単位データと、アプリ表示用の代表判定に構造化します。

アプリケーションは原則として次の2テーブルを参照します。

| テーブル | 役割 |
|---|---|
| `public.sgml_women_summary` | 1添付文書・1対象区分につき1行の代表判定。薬剤一覧、警告色、カルテ要約の一次検索に使う |
| `public.sgml_women_statement` | 添付文書中の全文表現。代表判定の根拠表示、監査、RAGへの根拠注入に使う |

`temp_sgml_women_*`、`sgml_women_block`、`sgml_women_document_state`は抽出処理、再開、検証、監査のためのパイプライン内部テーブルです。通常の画面やRAGから直接参照しません。

## 2. 2026-08-15公開時点の規模

34の公開結果は次のとおりです。

| データ | 件数 |
|---|---:|
| `sgml_women_statement` | 36,394文 |
| `sgml_women_summary` | 22,456行 |
| 妊婦サマリー | 11,228行 |
| 授乳サマリー | 11,228行 |

色別のサマリー件数は次のとおりです。

| population_type | RED | YELLOW | BLUE | GRAY | 合計 |
|---|---:|---:|---:|---:|---:|
| `PREGNANCY` | 2,119 | 6,258 | 0 | 2,851 | 11,228 |
| `LACTATION` | 1,714 | 5,965 | 0 | 3,549 | 11,228 |

`BLUE`が0件なのは、今回の添付文書群に「明示的に使用可能」と確実に分類された代表判定がなかったためです。`GRAY`を安全の意味で`BLUE`へ読み替えてはいけません。

## 3. データフロー

```text
public.sgml_rawdata
    ↓ 31: 妊婦・授乳章の抽出、意味差分検出
public.sgml_women_document_state
public.sgml_women_block
    ↓ 32: 全文を文単位に分割、明示表現をルール分類
public.temp_sgml_women_candidate
    ↓ 33: 分類困難な推奨表現だけをLLM分類、原文全文照合
public.temp_sgml_women_run
public.temp_sgml_women_fact
    ↓ 33_5: 保存済み応答の無課金再検証（必要時）
    ↓ 34: 主モデルを優先し、fallbackモデルで補完して公開
public.sgml_women_statement
public.sgml_women_summary
```

ルールで明確に分類できる表現はLLMへ送りません。LLMは既存分類へ確実に入らない推奨表現だけを処理し、分類できない場合は`UNCLASSIFIABLE`として残します。LLMが返した根拠文は入力原文全文との一致検証を通過したものだけが採用されます。

## 4. 対象区分

`population_type`は次の2種類です。

| 値 | 内容 | 標準的な添付文書節 |
|---|---|---|
| `PREGNANCY` | 妊婦・妊娠可能な女性への投与 | 9.5 |
| `LACTATION` | 授乳婦への投与、授乳継続・中止 | 9.6 |

同じ添付文書について通常は2種類のサマリーが作られます。対象章が存在しない場合も、無記載であることを明示するため`SECTION_ABSENT`のサマリーを保持します。

## 5. `public.sgml_women_summary`

### 5.1 用途

薬剤一覧や薬剤詳細画面では、まずこのテーブルを参照します。主キーは`(package_insert_no, population_type)`です。

### 5.2 カラム

| カラム | 内容 |
|---|---|
| `package_insert_no` | 添付文書パッケージ識別子 |
| `population_type` | `PREGNANCY`又は`LACTATION` |
| `prepared_ym` | 添付文書作成年月 |
| `generic_name_ja` | 一般名等の表示名 |
| `assessment_code` | その対象区分で最も優先度が高い代表判定 |
| `display_level` | DB生成時の既定表示区分。アプリ独自の色分けでは使用しなくてもよい |
| `assessment_text` | 代表判定の定型表示文 |
| `reason_statement_id` | 代表判定を支持する`sgml_women_statement.statement_id` |
| `needs_review` | 判定不能表現が1件以上含まれるか |
| `has_unclassified` | `UNCLASSIFIABLE`文が1件以上含まれるか。現在は`needs_review`と同じ条件 |
| `statement_count` | 集約対象となった全文表現数 |
| `definition_version` | 抽出・分類定義の版 |
| `updated_at` | サマリー更新日時 |

### 5.3 代表判定の決め方

同じ薬剤・対象区分に複数の判定がある場合、次の優先度で最も強い判定を`assessment_code`へ採用します。表中の色はDBに保存される既定値であり、アプリは`assessment_code`から独自に色やアイコンを割り当てられます。

| 優先度 | assessment_code | 色 | 表示上の意味 |
|---:|---|---|---|
| 100 | `CONTRAINDICATED` | RED | 投与禁忌 |
| 90 | `AVOID` | RED | 投与又は授乳を避ける |
| 90 | `STOP_BREASTFEEDING` | RED | 授乳を中止する |
| 70 | `PREFER_AVOID` | YELLOW | 投与しないことが望ましい |
| 60 | `BENEFIT_RISK` | YELLOW | 有益性が危険性を上回る場合のみ |
| 50 | `CONSIDER_CONTINUE_OR_STOP` | YELLOW | 授乳の継続又は中止を検討 |
| 40 | `UNCLASSIFIABLE` | YELLOW | 既存分類へ確実に分類できない |
| 20 | `ACCEPTABLE` | BLUE | 明示的に使用可能 |

分類コードを持つ文がない場合は次のどちらかになります。

| assessment_code | 色 | 意味 |
|---|---|---|
| `NO_EXPLICIT_RECOMMENDATION` | GRAY | 関連章はあるが明確な推奨記載がない |
| `SECTION_ABSENT` | GRAY | 関連章が抽出されなかった |

`needs_review=true`は代表判定そのものが無効という意味ではありません。例えば明示的な`AVOID`と、別の判定不能文が併存すれば、`assessment_code=AVOID`、`display_level=RED`、`needs_review=true`になります。

## 6. `public.sgml_women_statement`

### 6.1 用途

全文表示、代表判定の根拠、判定不能表現の確認、RAG注入にはこのテーブルを使います。通常の参照では必ず`WHERE is_current`を付けます。

### 6.2 識別・原文

| カラム | 内容 |
|---|---|
| `statement_id` | 公開文ID。summaryの`reason_statement_id`から参照される |
| `package_insert_no` | 添付文書パッケージ識別子 |
| `population_type` | `PREGNANCY`又は`LACTATION` |
| `prepared_ym` | 添付文書作成年月 |
| `generic_name_ja` | 一般名等の表示名 |
| `evidence_text` | 添付文書から抽出した根拠原文 |
| `statement_hash` | 文内容、対象区分、同一文の出現順に基づく安定ハッシュ |

### 6.3 分類

| カラム | 内容 |
|---|---|
| `expression_type` | 文の種類。`RECOMMENDATION`、`MILK_TRANSFER`、`INFANT_EFFECT`、`ANIMAL_FINDING`、`PLACENTAL_TRANSFER`、`HUMAN_FINDING`、`OTHER_INFORMATION`など |
| `classification_code` | 推奨分類。単なる背景情報ではNULL |
| `recommendation_target` | `DRUG`、`BREASTFEEDING`又はNULL |
| `display_level` | 分類コードに対応する表示色。背景情報ではNULL |
| `assessment_text` | ルールの定型文又はLLMが原文に基づき作成した短い評価文 |

`classification_code IS NULL`は欠損エラーとは限りません。動物試験、乳汁移行、胎児・乳児への所見など、投与可否を直接指示しない原文を全文保持した`SOURCE`行です。

### 6.4 出典・追跡

| カラム | 内容 |
|---|---|
| `source_block_id` | `sgml_women_block.block_id`への追跡用ID |
| `section_code` | `9.5`又は`9.6` |
| `section_type` | XML上の節種別 |
| `heading_path` | 見出し |
| `source_hash` | 公開元ブロックの内容ハッシュ |
| `definition_version` | 分類定義の版 |
| `prompt_version` | LLM利用時のプロンプト版 |
| `model_name` | LLM利用時の採用モデル。ルール・原文行ではNULL |

### 6.5 抽出・レビュー状態

| カラム | 値と意味 |
|---|---|
| `extraction_method` | `RULE`=明示表現をルール分類、`LLM`=検証済みLLM分類、`SOURCE`=分類を伴わない原文、`UNCLASSIFIED`=分類不能 |
| `review_status` | `AUTO_VALIDATED`=既存分類へ確定又は原文行、`NEEDS_REVIEW`=`UNCLASSIFIABLE`として公開 |
| `is_current` | 現在有効な公開行か |
| `first_published_at` | 初回公開日時 |
| `last_published_at` | 最終公開日時 |
| `superseded_at` | 旧版化した日時 |

`temp_sgml_women_run.status='success'`と、公開後の`review_status='NEEDS_REVIEW'`は両立します。前者はLLM処理と機械検証が成功したこと、後者は医学的な推奨ランクを原文から確定できず`UNCLASSIFIABLE`になったことを意味します。

## 7. アプリ用SQL

### 7.1 1添付文書の妊婦・授乳サマリー

```sql
SELECT
    population_type,
    assessment_code,
    display_level,
    assessment_text,
    needs_review,
    statement_count,
    updated_at
FROM public.sgml_women_summary
WHERE package_insert_no = $1
ORDER BY
    CASE population_type
        WHEN 'PREGNANCY' THEN 1
        WHEN 'LACTATION' THEN 2
        ELSE 9
    END;
```

### 7.2 代表根拠を同時に取得

```sql
SELECT
    s.population_type,
    s.assessment_code,
    s.display_level,
    s.assessment_text,
    s.needs_review,
    r.statement_id AS reason_statement_id,
    r.evidence_text AS reason_evidence_text,
    r.section_code,
    r.heading_path,
    r.extraction_method,
    r.model_name
FROM public.sgml_women_summary s
LEFT JOIN public.sgml_women_statement r
  ON r.statement_id = s.reason_statement_id
 AND r.is_current
WHERE s.package_insert_no = $1
ORDER BY s.population_type;
```

### 7.3 全原文を表示

```sql
SELECT
    statement_id,
    population_type,
    expression_type,
    classification_code,
    recommendation_target,
    display_level,
    assessment_text,
    evidence_text,
    section_code,
    heading_path,
    extraction_method,
    model_name,
    review_status
FROM public.sgml_women_statement
WHERE is_current
  AND package_insert_no = $1
  AND population_type = $2
ORDER BY
    CASE display_level
        WHEN 'RED' THEN 1
        WHEN 'YELLOW' THEN 2
        WHEN 'BLUE' THEN 3
        ELSE 4
    END,
    source_block_id,
    statement_id;
```

### 7.4 複数処方薬の一覧

```sql
SELECT
    package_insert_no,
    generic_name_ja,
    population_type,
    assessment_code,
    display_level,
    assessment_text,
    needs_review
FROM public.sgml_women_summary
WHERE package_insert_no = ANY($1::text[])
ORDER BY
    CASE display_level
        WHEN 'RED' THEN 1
        WHEN 'YELLOW' THEN 2
        WHEN 'BLUE' THEN 3
        ELSE 4
    END,
    package_insert_no,
    population_type;
```

### 7.5 判定不能表現を取得

```sql
SELECT
    package_insert_no,
    generic_name_ja,
    population_type,
    assessment_text,
    evidence_text,
    section_code,
    heading_path,
    model_name
FROM public.sgml_women_statement
WHERE is_current
  AND classification_code = 'UNCLASSIFIABLE'
  AND package_insert_no = ANY($1::text[])
ORDER BY package_insert_no, population_type, statement_id;
```

### 7.6 公開データの集計確認

```sql
SELECT
    population_type,
    assessment_code,
    display_level,
    needs_review,
    count(*) AS summaries
FROM public.sgml_women_summary
GROUP BY population_type, assessment_code, display_level, needs_review
ORDER BY population_type, display_level, assessment_code;
```

## 8. 推奨画面構成

薬剤ごとに「妊婦」「授乳」のカードを1枚ずつ表示します。表示色は`display_level`へ固定せず、`assessment_code`を基にアプリ側で割り当てます。

- 主表示: `assessment_text`
- 色・アイコン: `assessment_code`からアプリ側で決定。`display_level`は既定値として参照可能
- 判定不能表現を含む場合: `needs_review=true`を「要原文確認」などの補助バッジで表示
- 根拠表示: `reason_statement_id`で結んだ`evidence_text`
- 詳細展開: 同じ`package_insert_no`と`population_type`の全statement
- 出典表示: `prepared_ym`、`section_code`、`heading_path`

`RED`でも`needs_review=true`の場合があります。赤判定を黄色へ変更するのではなく、赤の主表示に「別途、分類不能記載あり」を併記します。

## 9. RAG・カルテ要約への利用

RAGでは、サマリーだけで回答を生成せず、代表根拠又は関連する全文を一緒に注入します。推奨する最小単位は次のとおりです。

```json
{
  "package_insert_no": "...",
  "population_type": "PREGNANCY",
  "assessment_code": "BENEFIT_RISK",
  "display_level": "YELLOW",
  "assessment_text": "有益性が危険性を上回る場合のみ",
  "needs_review": false,
  "evidence_text": "添付文書の根拠原文",
  "prepared_ym": "...",
  "section_code": "9.5"
}
```

RAG向けの基本方針は次のとおりです。

- 処方薬を名称のあいまい一致だけで結ばず、アプリ側の医薬品マスタから正しい`package_insert_no`へ対応付ける
- まずsummaryで対象薬を絞り、statementから根拠原文を取得する
- `UNCLASSIFIABLE`はLLMに再推論させて安全・危険を断定せず、「添付文書に次の記載があるが既存分類へ確定できない」と提示する
- `SOURCE`行は背景情報として有用だが、単独で投与可否を断定する根拠にしない
- 生成回答には薬剤名、妊婦／授乳の区別、判定、根拠原文、添付文書年月を保持する
- 最終的な診療判断は患者背景、妊娠週数、投与量、代替薬、疾患リスク等と合わせて医療者が行う

妊婦・授乳情報に加えて薬物動態・QT情報を注入する場合は`public.sgml_note`、相互作用は`public.sgml_interaction`を別の正本として検索します。各テーブルの判定を混ぜて1つの未検証スコアへ変換しないでください。

## 10. 医療利用上の重要な注意

- `GRAY`は安全を意味しません。`SECTION_ABSENT`は関連章が抽出されなかったこと、`NO_EXPLICIT_RECOMMENDATION`は明確な推奨を検出しなかったことだけを示します。
- `BLUE`は明示的な使用可能表現がある場合だけです。無記載から推定してはいけません。
- `YELLOW`には条件付き投与、授乳継続・中止の検討、判定不能が含まれます。`assessment_code`も必ず表示又は参照します。
- `needs_review=true`は無視せず、関連する`UNCLASSIFIABLE`文を原文表示できる導線を設けます。
- 添付文書の改訂後は、statementでは`is_current=true`、summaryでは更新済み行と`updated_at`を使います。
- このDBだけで自動的な処方中止、代替薬決定、患者への断定的説明を行わないでください。

## 11. 更新と再公開

添付文書更新後の標準処理は次のとおりです。

```bash
python3 31_build_sgml_women_blocks.py
python3 32_build_sgml_women_candidates.py
python3 33_extract_sgml_women.py --model gemma4:12b --run-status new
python3 33_extract_sgml_women.py \
  --model gemma4:31b-cloud \
  --source-model gemma4:12b \
  --source-status review error \
  --run-status new
python3 34_publish_sgml_women.py \
  --model gemma4:12b \
  --fallback-model gemma4:31b-cloud \
  --dry-run
python3 34_publish_sgml_women.py \
  --model gemma4:12b \
  --fallback-model gemma4:31b-cloud
```

31はXML全体のハッシュだけでなく、妊婦・授乳ブロックの意味ハッシュを保存します。XMLの管理情報だけが変わり対象本文が同じ場合は、後段の再処理を抑制できます。33の解析結果は内容・定義・プロンプト・モデルに基づいてキャッシュされるため、中断後も成功済み解析を再送せず再開できます。

分類ルールだけを変更した場合は、ブロック抽出とLLM分類をやり直す必要はありません。32で現在の全文を再分類し、34で既存LLM成功結果を再利用して再公開します。

```bash
python3 32_build_sgml_women_candidates.py
python3 34_publish_sgml_women.py \
  --model gemma4:12b \
  --fallback-model gemma4:31b-cloud \
  --dry-run
python3 34_publish_sgml_women.py \
  --model gemma4:12b \
  --fallback-model gemma4:31b-cloud
```

バージョンは、ブロック抽出版、ルール定義版、LLM定義版に分離されています。ルールだけを更新してもLLM解析ハッシュは変わらないため、33を実行しない限りLLM呼び出しは発生しません。

