# SGML2SQL ― PMDA「マイ医薬品」SGML → PostgreSQL 変換ツール群

## 概要
**SGML2SQL** は、PMDA が提供する **「マイ医薬品」サービスでダウンロードした SGML 形式の薬剤添付文書** を  
解析・平坦化し、**PostgreSQL データベースへアップロードするためのスクリプト群**です。

本ツールは、**OQSDrug（オンライン資格確認 + 薬剤データベース統合システム）** での利用を前提に設計しています。

- PMDA の SGML → Python パーサ  
- 添付文書（薬効・禁忌・相互作用など）の構造化  
- PostgreSQL への自動テーブル生成・INSERT  
- OQSDrug 用に最適化された JSON / SQL スキーマ  

---

## 特徴

### ✔ SGML の複雑な階層構造をフラット化  
PMDA の SGML はタグ構造が深く複雑ですが、本ツールでは以下を平坦化して扱いやすい JSON / SQL に変換します。

- 一般名 / 薬効分類  
- 使用上の注意（禁忌／慎重投与）  
- 相互作用（相手薬剤リスト、症状、メカニズム、等）  
- 用法・用量  
- 妊婦投与 / 小児投与  
- その他の注意  

### ✔ PostgreSQL のテーブルを自動生成  
設定ファイル `config.json` に従って、以下のテーブルを作成します。

- `sgml_rawdata`
- `sgml_interaction`
- `sgml_contraindication`（必要に応じて）
- その他、OQSDrug 構造に合わせた付随テーブル



---

## ディレクトリ構成（例）

