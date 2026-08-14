# SGML2SQL ― PMDA「マイ医薬品」SGML → PostgreSQL 変換ツール群

## 概要
**SGML2SQL** は、PMDA が提供する **[「マイ医薬品」サービス](https://push.info.pmda.go.jp/mypage/view/mypage/login.html)からダウンロードした SGML 形式の薬剤添付文書** を、
**[OQSDrug2（オンライン資格確認薬歴健診歴取得ツール）](https://github.com/YUKI-ENT/OQSDrug2)で利用するPostgreSQL データベース形式** に変換するためのスクリプト群です。

---

## 使用方法
1. このリポジトリをローカル環境にクローンします。
  ```bash
  git clone https://github.com/YUKI-ENT/SGML2SQL.git
  cd SGML2SQL
  ```

2.  **[マイ医薬品サービス](https://push.info.pmda.go.jp/mypage/view/mypage/login.html)**  にアカウントを作り、一括ダウンロードメニューからダウンロードします。
  ![スクリーンショット 2025-11-18 221657](https://github.com/user-attachments/assets/493657f4-b982-4a8a-81fd-6e4ea8ee465c)

`SGML/XML`にチェックを入れ、一括ダウンロードします。約900MBあるので、ダウンロードに時間がかかります。
  ![スクリーンショット 2025-11-18 221842](https://github.com/user-attachments/assets/1c5ab114-ebdd-420b-9780-0d05b1646d22)

3. ダウンロードしたzipファイルを `SGML2SQL/SGML` フォルダにコピーして解凍します。Unzipするとき、Linuxでは `-O cp932`とSJIS指定で解凍します。
  ```bash
  cd SGML
  unzip -O cp932 pmda_all_sgml_xml_20251116.zip
  ```

4. **Python仮想環境のセットアップ**
   - パッケージリストを更新し、venvとpipをインストール(python3、 venvが未導入の場合)
     ```bash
     sudo apt update
     sudo apt install -y python3-venv python3-pip
     ```
   - 仮想環境を作成
     ```
     cd ~/SGML2SQL
     # 仮想環境を作成
     python3 -m venv ./venv
     #アクティベート: 成功すると、(venv) yuki@ai-server:~/SGML2SQL $ のようなプロンプトになります
     source ./venv/bin/activate
     # 依存関係のインストール
     pip install -r requirements.txt
      ```
5. 既存データバックアップ（以前のバージョンの薬剤添付文書データがある場合）
   - `dump_sgml.sh` を編集し、postgreSQLサーバーアドレスやユーザー名を環境に合わせて書き換えてください。その後実行すると`backup/`フォルダに`sgml_yyyymmdd.backup`のようなファイル名でバックアップが作成されます(約230MB) 。このデータファイルはpg_restoreやOQSDrugからリストアできます。
     ```bash
     nano dump_sgml.sh
     bash dump_sgml.sh
     ```

6. **`config.json`の作成編集**
   - 添付の`config.json.sample`を`config.json`としてコピーし、編集します。
   - ` "db": {"host": "localhost" `、`user`、`password`、`DI_folder`あたりを環境に合わせて書き換えてください。
7. **21_sgml2rawdata.pyの実行**
   - SGMLファイルからSQLサーバーにXMLデータをアップロードします。
    ```bash
    python3 21_sgml2rawdata.py
    ```
    5-10分くらいかかります。エラーやログは`logs/`フォルダに出力されます。成功すると、postgreSQLサーバーに`sgml_rawdata`テーブルが作成されます。

9. **22_build_sgml_interaction.pyの実行**
   - こちらは`sgml_rawdata`をもとに、薬剤相互作用データの抽出を行います。
   ```bash
    python3 22_build_sgml_interaction.py
    ```
    10-20秒くらいで終了します。成功すると、`sgml_interaction`テーブルが作成されます。

10. **16章「薬物動態」のLLM抽出（試験段階）**
    - 先に16章を節・文字チャンク単位で `temp_sgml_pk_blocks` へ抽出します。
      ```bash
      python3 23_extract_sgml_pharmacokinetics.py
      ```
    - 次にOllamaで代謝酵素、阻害・誘導、トランスポーター、代謝物、排泄概要を抽出します。
      ```bash
      python3 24_build_sgml_pharmacokinetics.py
      ```
    - LLM処理履歴と検証前ファクトは `temp_` で始まるテーブルに保存されます。OQSDrugが利用する `ai_` テーブルは使用しません。
    - 配布対象の最終出力は `sgml_pharmacokinetics` です。全チャンクの処理に成功するまで、この最終テーブルは更新されません。
    - 各LLMリクエスト後のGPU冷却待機は `config.json` の `gpu_cooling_wait` で指定します。一時的に変更する場合は次のように指定できます。
      ```bash
      python3 24_build_sgml_pharmacokinetics.py --wait-seconds 30
      ```
    - 最初は1添付文書だけで確認することを推奨します。
      ```bash
      python3 23_extract_sgml_pharmacokinetics.py --package-insert-no 6149003F2020_3_05
      python3 24_build_sgml_pharmacokinetics.py --package-insert-no 6149003F2020_3_05 --model gpt-oss:20b --prompt-version pk-feature-v2 --no-publish
      ```

11. **汎用 `sgml_note` パイプライン（仮実装）**
    - `sgml_rawdata` 作成後、添付文書を意味ブロックへ差分展開します。XML全体が変更されても、意味ブロックのハッシュが同じならLLM処理は再実行されません。
      ```bash
      python3 41_build_sgml_note_blocks.py
      ```
    - `sgml_note_definitions.json` のテーマ定義を使い、キーワードでLLM投入候補を絞ります。
      ```bash
      python3 42_build_sgml_note_candidates.py
      ```
    - 候補をOllamaで抽出します。成功結果は入力ハッシュ、定義版、プロンプト版、モデル名でキャッシュされます。
      ```bash
      python3 43_extract_sgml_notes.py
      ```
    - 原文一致等の検証に成功したファクトだけを `sgml_note` へ公開します。最初は `--dry-run` を推奨します。
      ```bash
      python3 44_publish_sgml_notes.py --dry-run
      python3 44_publish_sgml_notes.py
      ```
    - 初回確認は1添付文書に限定できます。
      ```bash
      python3 41_build_sgml_note_blocks.py --package-insert-no 6149003F2020_3_05
      python3 42_build_sgml_note_candidates.py --package-insert-no 6149003F2020_3_05
      python3 43_extract_sgml_notes.py --package-insert-no 6149003F2020_3_05 --limit 5
      python3 44_publish_sgml_notes.py --package-insert-no 6149003F2020_3_05 --dry-run
      ```
    - 抽出対象を増やす場合は `sgml_note_definitions.json` に `note_type`、候補語、許可する関係、テーマ固有指示を追加します。
    - 相互作用章は既存の `sgml_interaction` を正本とし、既定のノートブロック対象から除外しています。
    - 複数成分・複数経路をまとめた排泄割合は1件の合計値として扱います。同じ根拠文中に1回だけ現れる割合を複数ファクトへ複製した応答は自動検証で拒否します。
    - 中断後は同じコマンドを再実行できます。成功済みの同一文章はハッシュキャッシュから再利用され、LLMには再送信されません。
    - 長時間処理の再開では、既存のreview/errorを飛ばして未処理だけを進められます。後からreview又はerrorだけを個別に再試行できます。
      ```bash
      python3 43_extract_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --run-status new
      python3 43_extract_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --run-status review
      python3 43_extract_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --run-status error
      ```
    - あるモデルでreview/errorになった候補だけを別モデルで救済できます。`--run-status new`を併用すると、cloud側で既に実行済みの候補も再送しません。
      ```bash
      python3 43_extract_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:31b-cloud --run-status new --source-model gemma4:12b --source-status review error
      ```
      公開時は主モデルを優先し、主モデルにsuccessがない候補だけcloudのsuccessで補完できます。
      ```bash
      python3 44_publish_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --fallback-model gemma4:31b-cloud --dry-run
      ```
    - 検証規則を改善した場合は、保存済み`raw_response`をLLMへ再送せず再検証できます。最初にdry-runし、結果を確認してから反映します。
      ```bash
      python3 43_5_revalidate_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --model gemma4:31b-cloud
      python3 43_5_revalidate_sgml_notes.py --note-type METABOLISM_TRANSPORT --model gemma4:12b --model gemma4:31b-cloud --execute
      ```
    - 残ったreviewは2つのCSVへ出力し、ケースの`decision/reviewer/review_comment`と、採用するfactの`include`及び内容を編集できます。`--model`の記載順で参照するreview応答を優先します。
      ```bash
      python3 43_6_export_sgml_note_reviews.py --note-type METABOLISM_TRANSPORT --model gemma4:31b-cloud --model gemma4:12b --output-dir ./manual_review/metabolism
      ```
    - 編集後は必ずdry-runで原文一致等を再検証し、問題がなければ`human-review`として反映します。元のLLM runは変更しません。
      ```bash
      python3 43_7_import_sgml_note_reviews.py --cases ./manual_review/metabolism/sgml_note_review_cases.csv --facts ./manual_review/metabolism/sgml_note_review_facts.csv
      python3 43_7_import_sgml_note_reviews.py --cases ./manual_review/metabolism/sgml_note_review_cases.csv --facts ./manual_review/metabolism/sgml_note_review_facts.csv --execute
      ```
      公開時は手動判断を最優先にします。`EXCLUDE`はsuccessかつfactなしとして、下位モデルのfactも公開対象から除外します。
      ```bash
      python3 44_publish_sgml_notes.py --note-type METABOLISM_TRANSPORT --model human-review --fallback-model gemma4:12b --fallback-model gemma4:31b-cloud --dry-run
      ```
    - 41～43の結果を完全初期化する場合は、最初に対象件数を確認してから実行します。
      ```bash
      python3 40_reset_sgml_notes.py
      python3 40_reset_sgml_notes.py --execute
      ```
      44で公開した `sgml_note` も含める場合は `--include-published --execute` を指定します。

12. **妊婦・授乳の全文表現とアプリ向け判定（31–35系）**
    - 既存の `31_rawdata2women.py` / `32_label_women_risk.py` は旧方式として残しています。
    - 関連章を差分管理可能なblockへ抽出します。章がない場合も `SECTION_ABSENT` 判定用の状態を保存します。
      ```bash
      python3 31_build_sgml_women_blocks.py
      ```
    - block内の全文を文単位で保存し、明示的な定型表現だけをルール分類します。
      ```bash
      python3 32_build_sgml_women_candidates.py
      ```
    - 既存分類へ入らない推奨表現だけをLLMへ送り、分類不能なら `UNCLASSIFIABLE` のまま残します。
      ```bash
      python3 33_extract_sgml_women.py --limit 100
      ```
    - `sgml_women_statement`へ全表現を、`sgml_women_summary`へアプリ用の代表判定を公開します。
      ```bash
      python3 34_publish_sgml_women.py --dry-run
      python3 34_publish_sgml_women.py
      ```
    - `display_level` は `RED`（禁忌・回避）、`YELLOW`（条件付き・判定不能）、`BLUE`（明示的に使用可能）、`GRAY`（記載なし）です。無記載を安全とは扱いません。
    - 中間テーブルのリセットは次の通りです。公開結果も消す場合だけ `--include-published` を付けます。
      ```bash
      python3 35_reset_sgml_women.py
      python3 35_reset_sgml_women.py --execute
      python3 35_reset_sgml_women.py --include-published --execute
      ```
