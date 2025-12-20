#!/usr/bin/env python3
import os
import sqlite3
import sys
from typing import List, Optional, Tuple


def esc_sql(s: str) -> str:
    return s.replace("'", "''")


def detect_table(db_path: str, candidates: List[str]) -> Optional[str]:
    con = sqlite3.connect(db_path)
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for t in candidates:
            if t in tables:
                return t
        return None
    finally:
        con.close()


def table_schema(con: sqlite3.Connection, schema: str, table: str) -> Tuple[List[Tuple[str, str, int]], List[str]]:
    cols = [(r[1], r[2] or "", int(r[5])) for r in con.execute(f"PRAGMA {schema}.table_info({table})")]
    if not cols:
        raise RuntimeError(f"missing table {schema}.{table}")
    pk = [name for (name, _t, ispk) in cols if ispk]
    return cols, pk


def create_table_like(con: sqlite3.Connection, dst_table: str, src_schema: str, src_table: str):
    cols, pk = table_schema(con, src_schema, src_table)
    defs = [f'"{n}" {t}'.rstrip() for (n, t, _pk) in cols]
    if pk:
        defs.append("PRIMARY KEY(%s)" % ",".join(f'"{n}"' for n in pk))
    con.execute(f"DROP TABLE IF EXISTS {dst_table}")
    con.execute(f"CREATE TABLE {dst_table}(%s)" % ",".join(defs))


def copy_table(con: sqlite3.Connection, dst_table: str, src_schema: str, src_table: str):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({dst_table})")]
    col_list = ",".join(f'"{c}"' for c in cols)
    con.execute(f'INSERT INTO "{dst_table}"({col_list}) SELECT {col_list} FROM {src_schema}."{src_table}"')


def assert_has_table(con: sqlite3.Connection, schema: str, table: str):
    ok = con.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone()
    if not ok:
        tables = [r[0] for r in con.execute(f"SELECT name FROM {schema}.sqlite_master WHERE type='table' ORDER BY 1")]
        raise RuntimeError(f"missing table {schema}.{table}; available in {schema}: {tables}")


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 2:
        a, b = args
        a = os.path.abspath(a)
        b = os.path.abspath(b)
    elif len(args) == 0:
        cwd = os.getcwd()
        dbs = [
            os.path.join(cwd, f)
            for f in os.listdir(cwd)
            if f.lower().endswith(".sqlite") and f.lower() != "catalog.sqlite"
        ]
        if len(dbs) != 2:
            print(f"expected 2 sqlite paths as args, or exactly 2 .sqlite files in current dir; found {len(dbs)}", file=sys.stderr)
            for f in sorted(dbs):
                print(f, file=sys.stderr)
            return 1
        a, b = dbs[0], dbs[1]
    else:
        print("usage: merge_movie_tv_dbs.py [movies.sqlite series.sqlite]", file=sys.stderr)
        return 1

    if not (os.path.isfile(a) and os.path.isfile(b)):
        print("both sqlite paths must exist", file=sys.stderr)
        return 1

    a_movies = detect_table(a, ["movies"])
    b_movies = detect_table(b, ["movies"])
    a_series = detect_table(a, ["series", "tv_series"])
    b_series = detect_table(b, ["series", "tv_series"])

    if (a_movies and a_series) or (b_movies and b_series):
        print("one db should be movies-only and the other series-only", file=sys.stderr)
        return 1

    if not ((a_movies and b_series) or (b_movies and a_series)):
        print("could not find a movies table in one db and a series/tv_series table in the other", file=sys.stderr)
        return 1

    movie_db, movie_table = (a, "movies") if a_movies else (b, "movies")
    series_db, series_table = (b, b_series) if a_movies else (a, a_series)

    out_path = os.path.join(os.getcwd(), "catalog.sqlite")
    out = sqlite3.connect(out_path)
    out.execute("PRAGMA journal_mode=WAL")
    out.execute("PRAGMA synchronous=OFF")
    out.execute("PRAGMA busy_timeout=30000")

    out.execute(f"ATTACH DATABASE '{esc_sql(movie_db)}' AS m")
    out.execute(f"ATTACH DATABASE '{esc_sql(series_db)}' AS s")

    try:
        assert_has_table(out, "m", movie_table)
        assert_has_table(out, "s", series_table)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    create_table_like(out, "movies", "m", movie_table)
    create_table_like(out, "series", "s", series_table)

    out.execute("DELETE FROM movies")
    out.execute("DELETE FROM series")

    copy_table(out, "movies", "m", movie_table)
    copy_table(out, "series", "s", series_table)

    out.commit()
    out.execute("DETACH DATABASE m")
    out.execute("DETACH DATABASE s")
    out.close()

    print("wrote catalog.sqlite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
