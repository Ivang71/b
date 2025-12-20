#!/usr/bin/env python3
import os
import sqlite3
import sys


def main() -> int:
    db = os.environ.get("CATALOG_DB") or "catalog.sqlite"
    if not os.path.isfile(db):
        print(f"missing {db}", file=sys.stderr)
        return 1

    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=30000")

    cols = [(r[1], r[2] or "", int(r[5])) for r in con.execute("PRAGMA table_info(series)")]
    if not cols:
        print("missing series table", file=sys.stderr)
        return 1

    if "genres" not in {c[0] for c in cols}:
        print("nothing to do")
        return 0

    keep = [c for c in cols if c[0] != "genres"]
    pk = [name for (name, _t, ispk) in keep if ispk]

    idx_sql = [
        r[0]
        for r in con.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='series' AND sql IS NOT NULL"
        )
        if r[0] and "genres" not in r[0].lower()
    ]

    defs = [f'"{n}" {t}'.rstrip() for (n, t, _pk) in keep]
    if pk:
        defs.append("PRIMARY KEY(%s)" % ",".join(f'"{n}"' for n in pk))

    col_list = ",".join(f'"{n}"' for (n, _t, _pk) in keep)

    con.execute("BEGIN")
    con.execute("DROP TABLE IF EXISTS series_new")
    con.execute(f"CREATE TABLE series_new({','.join(defs)})")
    con.execute(f"INSERT INTO series_new({col_list}) SELECT {col_list} FROM series")
    con.execute("DROP TABLE series")
    con.execute("ALTER TABLE series_new RENAME TO series")
    for s in idx_sql:
        con.execute(s)
    con.execute("COMMIT")

    print("dropped series.genres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
