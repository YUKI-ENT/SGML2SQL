# `doc_xml` 原文保存への変更

## 概要

`21_sgml2rawdata.py` の `doc_xml` 保存方法を、Python `ElementTree` による再シリアライズから、PMDA配布XMLファイルの原文保存へ変更した。

変更前：

```python
doc_xml = ET.tostring(root, encoding="unicode")
```

変更後：

```python
doc_xml = read_original_xml_text(xml_path)
```

XMLの解析とJSON・相互作用データの生成には、従来どおり `ElementTree` の解析結果を使用する。今回変更されるのは `sgml_rawdata.doc_xml` に格納する文字列だけである。

## `doc_xml` に現れる差分

原文保存後は、従来の `doc_xml` から失われていた次の情報が保持される。

- XML宣言（例：`<?xml version="1.0" encoding="utf-8"?>`）
- XMLコメント（例：`<!--６.用法及び用量-->`）
- 処理命令（特に改行位置を示す `<?enter?>`）
- 原文の改行コードとインデント
- 原文の名前空間表記
- 原文の空要素表記など、XMLとして同義でも字面が異なる表現

UTF-8 BOMが存在する場合、BOMだけはDBへ渡すUnicode文字列から除去される。XML本文は変更しない。

### メリスロンでの具体例

原文：

```xml
<Lang xml:lang="ja">下記の疾患に伴うめまい、めまい感<?enter?>メニエール病、メニエール症候群、眩暈症</Lang>
```

従来の `doc_xml`：

```xml
<ns0:Lang xml:lang="ja">下記の疾患に伴うめまい、めまい感メニエール病、メニエール症候群、眩暈症</ns0:Lang>
```

変更後の `doc_xml` は原文と同じく `<?enter?>` を保持する。

製品名参照も原文どおり保持される。

```xml
<Lang xml:lang="ja">〈<ApprovalBrandNameRef ref="BRD_Drug1" />〉</Lang>
```

参照先は `ApprovalEtc/DetailBrandName[@id='BRD_Drug1']` の `ApprovalBrandName` である。OQSDrugは `ApprovalBrandNameRef/@ref` を参照先の日本語製品名へ展開して表示する必要がある。

## OQSDrug側で必要な対応

OQSDrugは旧形式と原文形式の両方を読めるようにする。

1. 名前空間プレフィックス（`ns0:` など）に依存せず、名前空間URIとローカル名で要素を判定する。
2. 子ノードを走査する場合、要素だけでなくコメントと処理命令が存在し得ることを考慮する。
3. `<?enter?>` を画面上の改行として扱う。
4. `ApprovalBrandNameRef/@ref` を `DetailBrandName/@id` と照合し、製品名へ展開する。
5. XML宣言が存在する入力を受け付ける。
6. DBから得たXML文字列をHTMLへ直接連結せず、XMLとして解析してHTMLエスケープする。

LINQ to XMLを使用する場合、要素列挙には `Nodes()` の無条件な `XElement` キャストではなく、`Elements()` またはノード種別の判定を使用する。原文には `XComment` と `XProcessingInstruction` が含まれる。

## Python後続処理への影響

### 22 interaction

`22_build_sgml_interaction.py` は `doc_xml` ではなく `interactions_flat` を読むため、今回の変更による直接影響はない。

### 31 women

`31_build_sgml_women_blocks.py` は `doc_xml::text` を読み込む。原文保存への切替によりXML文字列全体の `raw_xml_hash` が変わるため、初回は再解析対象になる。

現在の `ElementTree.fromstring()` はコメントと処理命令を抽出ツリーから除外するため、`<?enter?>` を明示的に改行へ変換しない限り、抽出される本文は従来と基本的に同じである。意味ハッシュが同一なら実質的なブロック更新は抑制される。

### 41 note

`41_build_sgml_note_blocks.py` も同様に `raw_xml_hash` が変わり、初回は再解析される。抽出後の `semantic_manifest_hash` が同一なら、意味内容が同じものとして処理される。

`<?enter?>` を改行として利用する変更は今回には含めない。将来31・41で対応する場合は `block_text`、`content_hash`、`block_uid`、候補およびAI抽出キャッシュへ影響するため、抽出器バージョンを上げて別変更として再構築する。

## 変更されないもの

- `approval_etc_json` などのJSON列の生成方法
- `interactions_json`
- `interactions_flat`
- ブランド単位の行生成
- 主キーおよびUPSERT処理
- `raw_xml_path`

## 移行時の注意

21は起動時に `sgml_rawdata` をDROPして再作成する。21の再実行後、旧形式の `doc_xml` と新しい原文形式が同一テーブル内に混在することはない。

再取込後は少なくとも次を確認する。

```sql
SELECT
    package_insert_no,
    position('<?enter?>' in doc_xml::text) > 0 AS has_enter_pi,
    position('<!--' in doc_xml::text) > 0 AS has_comment
FROM sgml_rawdata
WHERE package_insert_no = '1339005F1296_1_14';
```

期待値は `has_enter_pi = true`、`has_comment = true`。

OQSDrugの原文形式対応を展開してから、21による本番データの再取込を行うこと。
