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
