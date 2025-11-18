#!/bin/bash

set -e  # エラーで即終了（安全）

HOST="localhost"
PORT=5432
USER="postgres"
PASS=""
DB="OQSDrug_data"

# ダンプ対象テーブル（必要に応じて追加）
TABLES=(
  "sgml_rawdata"
  "sgml_interaction"
)

# 出力フォルダ（必要なら変更）
OUTDIR="./backup"
mkdir -p "$OUTDIR"

# 日付＋時刻（例：20250209-153045）
TS=$(date +"%Y%m%d-%H%M%S")

# 出力ファイル名
OUTFILE="${OUTDIR}/sgml_${TS}.backup"

echo "=== SGML dump start ==="
echo "Output file: ${OUTFILE}"
echo "Dumping tables: ${TABLES[@]}"

# パスワードを環境変数に渡す
#export PGPASSWORD="${PASS}"

# pg_dump 実行
pg_dump -h "$HOST" -p "$PORT" -U "$USER" \
  -F c -Z 9 --no-owner --no-privileges \
  $(printf -- "-t %s " "${TABLES[@]}") \
  -f "$OUTFILE" "$DB"

echo "=== Dump completed successfully ==="
echo "Saved to: $OUTFILE"
