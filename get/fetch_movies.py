#!/usr/bin/env python3
import os
import sqlite3
import sys
import time
from typing import Dict, Optional, Tuple

import requests


def load_env(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return out


def pick_token(env: Dict[str, str]) -> Optional[Tuple[str, str]]:
    for k in ("TMDB_BEARER_TOKEN", "TMDB_API_READ_ACCESS_TOKEN", "TMDB_ACCESS_TOKEN", "TMDB_TOKEN"):
        v = env.get(k)
        if v:
            return ("bearer", v)
    v = env.get("TMDB_API_KEY")
    return ("api_key", v) if v else None


def tmdb_get(session: requests.Session, auth: Tuple[str, str], url: str, params: Dict[str, str]):
    headers = {"accept": "application/json"}
    p = dict(params)
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        p["api_key"] = auth[1]

    for attempt in range(6):
        try:
            r = session.get(url, headers=headers, params=p, timeout=25)
        except requests.RequestException:
            time.sleep(0.5 * (2**attempt))
            continue
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            time.sleep(float(ra) if ra and ra.isdigit() else 1.0)
            continue
        if 500 <= r.status_code < 600:
            time.sleep(0.5 * (2**attempt))
            continue
        return r
    return None


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print("missing TMDB token in .env", file=sys.stderr)
        return 1

    min_votes = int(float(env.get("TMDB_MIN_VOTE_COUNT") or "100"))
    max_pages = int(float(env.get("TMDB_MAX_PAGES") or "2500"))
    lang = (env.get("TMDB_LANGUAGE") or "en-US").strip() or "en-US"
    region = (env.get("TMDB_REGION") or "").strip() or None

    out_db = env.get("TMDB_OUT_DB") or f"TMDB_discover_movies_vote_count_gte{min_votes}.sqlite"

    con = sqlite3.connect(os.path.join(cwd, out_db))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA busy_timeout=30000")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS movies(
          id INTEGER PRIMARY KEY,
          title TEXT,
          vote_average REAL,
          vote_count INTEGER,
          release_date TEXT,
          adult INTEGER,
          backdrop_path TEXT,
          original_language TEXT,
          original_title TEXT,
          overview TEXT,
          popularity REAL,
          poster_path TEXT
        )
        """.strip()
    )

    session = requests.Session()

    inserted = 0
    page = 1
    while page <= max_pages:
        params = {
            "sort_by": "vote_count.desc",
            "vote_count.gte": str(min_votes),
            "include_adult": "false",
            "language": lang,
            "page": str(page),
        }
        if region:
            params["region"] = region

        r = tmdb_get(session, auth, "https://api.themoviedb.org/3/discover/movie", params)
        if not r or r.status_code != 200:
            break
        data = r.json() or {}
        results = data.get("results") or []
        if not results:
            break

        rows = []
        for m in results:
            if not isinstance(m, dict):
                continue
            mid = int(m.get("id") or 0)
            if mid <= 0:
                continue
            rows.append(
                (
                    mid,
                    m.get("title") or None,
                    float(m.get("vote_average") or 0.0),
                    int(m.get("vote_count") or 0),
                    m.get("release_date") or None,
                    1 if m.get("adult") else 0,
                    m.get("backdrop_path") or None,
                    m.get("original_language") or None,
                    m.get("original_title") or None,
                    m.get("overview") or None,
                    float(m.get("popularity") or 0.0),
                    m.get("poster_path") or None,
                )
            )

        con.executemany(
            """
            INSERT INTO movies(
              id,title,vote_average,vote_count,release_date,adult,backdrop_path,original_language,original_title,overview,popularity,poster_path
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              title=excluded.title,
              vote_average=excluded.vote_average,
              vote_count=excluded.vote_count,
              release_date=excluded.release_date,
              adult=excluded.adult,
              backdrop_path=excluded.backdrop_path,
              original_language=excluded.original_language,
              original_title=excluded.original_title,
              overview=excluded.overview,
              popularity=excluded.popularity,
              poster_path=excluded.poster_path
            """.strip(),
            rows,
        )
        inserted += len(rows)

        total_pages = int(data.get("total_pages") or 0)
        if total_pages and page >= total_pages:
            break
        page += 1

    con.execute("CREATE INDEX IF NOT EXISTS movies_vote_count_idx ON movies(vote_count DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS movies_popularity_idx ON movies(popularity DESC)")
    con.commit()
    con.close()

    print(f"wrote {out_db} rows={inserted} min_vote_count={min_votes} pages={page-1}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

