#!/usr/bin/env bash
set -euo pipefail

umask 077

opt_dir="${OPT_DIR:-/opt/origin-api}"
db="${CATALOG_DB:-$opt_dir/catalog.sqlite}"
dst_dir="${BACKUP_DIR:-$opt_dir/backups}"
keep_days="${BACKUP_KEEP_DAYS:-14}"

mkdir -p "$dst_dir"

ts="$(date -u +%Y%m%d-%H%M%S)"
tmp="$dst_dir/catalog-$ts.sqlite.tmp"
out="$dst_dir/catalog-$ts.sqlite"

sqlite3 "$db" ".timeout 30000" ".backup '$tmp'"
mv -f "$tmp" "$out"
gzip -f -1 "$out"

find "$dst_dir" -maxdepth 1 -type f -name 'catalog-*.sqlite.gz' -mtime +"$keep_days" -delete

