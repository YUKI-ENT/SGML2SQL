# 相互作用相手のコード化・評価ツール

`25_resolve_sgml_interactions.py` は、PostgreSQLの `sgml_rawdata.doc_xml` と薬剤名マスターを読み、原文の相手記載をコードへ対応付ける評価用実装です。
DB接続はREPEATABLE READ・読み取り専用で固定しています。結果は世代ごとにローカルのCSV・JSON・SQLiteへ出力します。
25番は公開中のPostgreSQLテーブルを変更しません。評価結果のOQSDrugへの本番連携・PostgreSQLへの公開は未実装です。
別途、従来の21・22番にも名称連結の修正を追加しました。以下の「従来テーブルの名称連結修正」を参照してください。

## 従来テーブルの名称連結修正

21・22番は共通の `_sgml_interaction_flat.py` を使い、相互作用原文の `<?enter?>` を改行として保持します。
例えば `葛根湯小青竜湯麻黄湯` は `葛根湯\n小青竜湯\n麻黄湯` になります。
配合剤名の中黒・ハイフンは分割せず、除外・経路条件を同じレコードに残します。
テーブルの列構成、併用禁忌/注意の区分、行の単位は従来どおりです。最初の見出しを代表群にする従来処理の変更は今回に含みません。

保存済み `sgml_rawdata.doc_xml` に改行情報が残っている場合、22番の再実行だけで従来テーブルへ反映できます。
原文がNULLの行は旧 `interactions_flat` を利用して警告し、原文から既に失われている区切りは推測で補いません。
古い原文に改行情報が残っていない場合は、元XMLの再取り込みが必要です。
今回の作業ではコード修正と検証まで行い、公開DBは再生成していません。

```bash
python 22_build_sgml_interaction.py
```

このコマンドは既存テーブルを再生成します。元XMLはストリーミングで読み、全件分を一度にメモリーへ読み込みません。
再生成DDLと全件投入を一つのトランザクションにし、XML解析・投入の失敗時は旧テーブルを維持します。
従来の `DROP ... CASCADE` 自体は残るため、独自に追加した依存ビュー・権限等の保存には別途移行対応が必要です。
保存済みの実データ100文書との比較で、旧処理と行数・改行を除く内容が一致し、59文書で改行が追加されることを確認しました。

## 実装内容

- 原文XMLの `<?enter?>`、Detail、SimpleList/Itemを使った抽出。
- 直前の見出しとリストの対応、入れ子の親子関係を保存。複数行の見出しで適用範囲が曖昧なら、子も未解決にする。
- 配合剤名の中黒・ハイフンを分割せず、「等」「など」を原文に残す。
- 一般名・標準名・販売名の正規化一致と、レビュー済み別名辞書によるコード対応。
- 名称部分一致は `needs_review`。名称の意味が曖昧な一致や、不適合コードを含む同名マスターも自動採用しない。
- 群の範囲、未対応XML、除外・条件、解析エラーは不明として保持。
- マスター、原文、抽出結果、対応根拠、プログラム版、辞書版、世代を保存。
- 別名辞書の更新は、DBへ接続せず保存済み抽出結果から再解決できる。
- 全桁YJコードによるA→B、B→Aの照合コマンド。

## 起動環境

Python 3.9以上と `psycopg2-binary` が必要です。オフラインのresolve/checkと単体テストは標準ライブラリのみで動作します。
通常はプロジェクトの `requirements.txt` を導入したPythonを使用してください。

今回のWindows環境では `python` がInkscape付属Pythonを指し、既存 `.venv` は存在しないPython 3.13を参照していました。
動作確認には `py -3.12` と `.codex_tmp/interaction_db_deps` に導入した接続ライブラリを使用しました。
この作業環境で同じ実行環境を使う場合、PowerShellで次を指定します。

```powershell
$env:PYTHONPATH = (Resolve-Path '.codex_tmp/interaction_db_deps').Path
$env:PYTHONIOENCODING = 'utf-8'
py -3.12 25_resolve_sgml_interactions.py --help
```

以下の `python` は、必要に応じて `py -3.12` に読み替えてください。

## 1. DBから評価データを作る

既存 `config.json` の `db` と `sgml_table` を使用します。接続情報は出力しません。
対象文書を絞っても名称マスターは全件を読み、サンプル内に相手薬がないために解決できない状況を避けます。

```bash
python 25_resolve_sgml_interactions.py build --package-insert-no 5200138C1045_1_08 --output-dir logs/interaction_maou_run1
python 25_resolve_sgml_interactions.py build --limit 100 --output-dir logs/interaction_sample_run1
```

`--limit` の既定値は100文書です。文書番号順の先頭から選ぶため、無作為標本ではありません。
`--package-insert-no` は複数回指定できます。指定した文書が見つからない場合は失敗します。
全件は `--limit 0` です。原文と全結果をメモリーに保持するため、先に少数文書で所要メモリー・出力容量を確認してください。

既に存在する出力フォルダーは上書きしません。検証と全ファイルの書き込みが完了した世代だけを指定フォルダー名に切り替えます。
失敗時は以前の出力が残り、今回の途中結果は `.interaction-staging-*` に残ります。途中結果をOQSDrugへ渡さないでください。

## 2. 結果を確認する

| ファイル | 内容 |
| --- | --- |
| report.md / summary.json | 件数・状態・未解決理由の集計 |
| targets.csv | 全相手記載。解決状態、原文、親子関係、コード、確認先 |
| unresolved.csv | 部分解決・未解決・条件未解決。日本語理由と次の対応付き |
| codes.csv | 採用・候補別のコード対応、許可する全桁YJ、根拠 |
| documents.csv | 文書単位の抽出成功・失敗・相互作用章なし・空章 |
| interactions.sqlite3 | 原文、対象、対応、全桁コードを関連付けた評価用DB |
| manifest.json | スキーマ版、世代、原文/マスター/辞書ハッシュ、生成日時 |
| sources.jsonl / master.jsonl | DBから取得した原文と名称マスターのスナップショット |
| extracted.jsonl / resolved.jsonl | 再解決用の抽出結果とコード解決結果 |
| aliases.json | 当該世代で使った別名辞書 |

CSVはExcelで開きやすいUTF-8 BOM・CRLFです。数式として解釈される文字から始まるセルは先頭にアポストロフィを付けます。
機械処理の正本はJSON/SQLiteです。コードは文字列として扱ってください。

### 状態の読み方

| 軸 | 状態 | 意味 |
| --- | --- | --- |
| 相手記載 | resolved | 入力マスター内で当該具体名の対応範囲が確定 |
| 相手記載 | partial | 例示薬などの一部を対応付けたが、群・列挙の残りは不明 |
| 相手記載 | unresolved | 確定部分なし。候補だけあってもこの状態 |
| 対応コード | accepted | 自動規則または出典付き別名辞書で採用した対応 |
| 対応コード | needs_review | 部分一致・名称の曖昧さなどによる未採用候補 |
| 条件 | none | この抽出で条件記載を検出していない |
| 条件 | unresolved | 条件・除外の記載を検出したが評価できない |

相互作用行は全対象がresolvedならresolved、一部だけ確定ならpartial、それ以外はunresolvedです。
群と例示薬を別対象として数えるため、相手記載の解決率は薬剤ペアの検出率ではありません。
`accepted` は対応付けの採用状態であり、人手確認済みや臨床的な安全性の意味ではありません。

今回は条件評価を未実装とし、条件を検出したDrugName全体のコード生成を停止します。
`none` も自然言語中の全条件が存在しないことを保証しません。完全一致と原文構造の規則が適用できる範囲の評価です。
群の全範囲を確定する辞書・薬効分類による候補展開は未実装です。
SummaryOfCombination、参照要素、未対応の内部要素等は原文を残して未解決にします。

## 3. 不明を確認し、辞書を更新する

1. `unresolved.csv` の併用禁忌や頻出項目から原文・XML位置を確認する。
2. 具体名の別名なら、マスターと照合し、正しい全桁YJコード集合をレビューする。
3. 確認済み内容を別名辞書JSONへ登録し、別の出力フォルダーへ再解決する。
4. 抽出境界・見出しの問題は辞書でごまかさず、抽出器と回帰テストを修正する。
5. 群や除外・経路条件は、その範囲を扱う機能を追加するまで不明のまま残す。

別名辞書の構造例（コード・名称はテスト専用の架空値）:

```json
{
  "version": "review-2026-09-23-01",
  "entries": [
    {
      "name": "確認した別名",
      "yj_codes": ["1111111A1111"],
      "scope_complete": true,
      "source": "確認に使用した文書・版・箇所",
      "reviewer": "確認者",
      "reviewed_at": "2026-09-23"
    }
  ]
}
```

マスターに存在しないコード、レビュー情報欠落、正規化後の別名重複、範囲未確定の登録は拒否します。
`scope_complete` は確認者の判断の記録であり、ソフトが医学的妥当性を保証するものではありません。
別名辞書は既存の完全一致より優先するため、変更履歴も別途管理してください。群・条件の未解決を解除する用途には使えません。

```bash
python 25_resolve_sgml_interactions.py resolve --input-dir logs/interaction_sample_run1 --aliases reviewed_aliases.json --previous logs/interaction_sample_run1 --output-dir logs/interaction_sample_run2
```

`--aliases` 省略時は入力世代の辞書を引き継ぎます。保存済み抽出結果とマスターの改変はハッシュ検証で拒否します。
抽出器の版が変わった場合はDBの原文から `build` し直します。既存の手動判断を行IDで自動継承する仕組みはありません。
`--previous` は件数差分です。対象文書や辞書が異なる実行間の差を、そのまま精度改善と解釈しないでください。

## 4. 双方向照合とOQSDrug向け契約

```bash
python 25_resolve_sgml_interactions.py check --database logs/interaction_sample_run1/interactions.sqlite3 --a-yj 1111111A1111 --b-yj 2222222A1111
```

上のコードは書式例です。実際の全桁YJを指定します。
出力は `matches`（採用一致・要確認候補）、`unresolved`（各薬剤に残る不明）、`coverage`（当該自薬の原文取得状態）です。
不明は特定ペアの陽性結果ではなく、当該薬剤に残る未評価範囲です。`has_unassessed_scope` が全体の未評価有無を示します。
チェック結果に「安全」という判定は生成しません。

自薬は `document_drug.yj_code` の全桁一致、相手も許可全桁コードの一致を必須にします。
`partner_yj7` は索引キーですが、すべての対応は `requires_full_yj` とし、7桁のみ一致する別製品には拡張しません。
以下はSQLite評価DBの照合SQL例です。本番PostgreSQLの既存テーブルとは別契約です。

```sql
SELECT i.package_insert_no, i.section_type, i.partner_text,
       i.symptoms_measures_ja, i.mechanism_ja,
       t.target_key, t.target_name, t.resolution_status,
       c.review_status, c.match_method, c.evidence_json
FROM document_drug d
JOIN sgml_interaction i USING (package_insert_no)
JOIN sgml_interaction_target t USING (interaction_key)
JOIN sgml_interaction_target_code c USING (target_key)
JOIN sgml_interaction_target_full_code f USING (target_key, partner_yj7, match_method)
WHERE d.yj_code = :own_yj AND f.yj_code = :partner_yj
  AND c.review_status = 'accepted'
  AND t.condition_status = 'none'
  AND t.mention_role <> 'exclusion';

-- 別クエリでも同一SQLiteファイルを利用し、世代を固定する。
SELECT generation_id, schema_version, manifest_json FROM metadata;
SELECT unresolved_reason, count(*) FROM sgml_interaction_target
WHERE resolution_status <> 'resolved' GROUP BY unresolved_reason;
```

`target_key`、`interaction_key` は元文書の内容ハッシュ・位置を含みます。文書が変わると変わるため、旧レビューを無条件に流用しません。
対応は対象＋相手7桁＋方法、全桁コードはそれに全桁YJを加えた複合主キーで重複を防ぎます。
同じコードへの複数の出典・一致列は `evidence_json` 内に保存します。外部キー・検索列に索引を設けています。

## 検証結果と残る作業

2026-09-23、実DBの名称マスター17,658行を使用。うちYJ欠落・形式不適合506行はコード辞書から除外し、件数を記録しました。
同名の不適合行がある名称は、他に有効コードがあっても自動採用を停止します。

| 評価対象 | 相互作用行 | 相手記載 | resolved | partial | unresolved |
| --- | ---: | ---: | ---: | ---: | ---: |
| 問題例 5200138C1045_1_08 | 2 | 28 | 13 | 6 | 9 |
| 文書番号順の先頭100文書 | 615 | 1,355 | 378 | 71 | 906 |

100文書の自動採用コード対応は582件、要確認候補は417件。対応件数は薬剤数・相手記載数とは異なります。
この標本に偏りがあるため、全体の成功率とはみなしません。誤対応・見落としの人手評価は未実施です。
問題例の元XMLから相互作用節を切り出して `tests/fixtures/interaction_maou.xml` に保存し、複数見出しとPI境界を回帰テストにしました。

```bash
python -m unittest discover -s tests -v
```

未解決群の辞書化、条件・除外の構造化、参照要素の展開、PostgreSQLへの世代付き公開と既存22の移行、OQSDrug側の組み込みは今後の実装対象です。
現時点では、原文・成功・不明の一覧を実データで確認し、辞書を改善して再評価するところまで実行できます。
