#!/usr/bin/env python3
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
from typing import Dict, List, Optional, Tuple

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
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k:
                    out[k] = v
    except FileNotFoundError:
        pass
    return out


def pick_token(env: Dict[str, str]) -> Optional[Tuple[str, str]]:
    for k in ("TMDB_BEARER_TOKEN", "TMDB_API_READ_ACCESS_TOKEN", "TMDB_ACCESS_TOKEN", "TMDB_TOKEN"):
        if env.get(k):
            return ("bearer", env[k])
    if env.get("TMDB_API_KEY"):
        return ("api_key", env["TMDB_API_KEY"])
    return None


class RateLimiter:
    def __init__(self, rps: float):
        self.dt = 1.0 / rps
        self.lock = Lock()
        self.next_t = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if self.next_t <= now:
                self.next_t = now + self.dt
                return
            t = self.next_t
            self.next_t += self.dt
        time.sleep(max(0.0, t - now))


def fetch_translations(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int):
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/translations"
    headers = {"accept": "application/json"}
    params = None
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params = {"api_key": auth[1]}

    for attempt in range(6):
        limiter.wait()
        try:
            r = session_factory().get(url, headers=headers, params=params, timeout=25)
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
        if r.status_code != 200:
            return (media_type, tmdb_id, None)

        try:
            return (media_type, tmdb_id, r.json())
        except ValueError:
            return (media_type, tmdb_id, None)

    return (media_type, tmdb_id, None)


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print("missing TMDB token in .env (TMDB_BEARER_TOKEN/TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY)", file=sys.stderr)
        return 1

    db_path = os.path.join(cwd, "catalog.sqlite")
    if not os.path.isfile(db_path):
        print("missing catalog.sqlite", file=sys.stderr)
        return 1

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA busy_timeout=30000")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_translations(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          iso_639_1 TEXT NOT NULL,
          iso_3166_1 TEXT NOT NULL,
          title TEXT,
          overview TEXT,
          tagline TEXT,
          homepage TEXT,
          PRIMARY KEY(media_type, tmdb_id, iso_639_1, iso_3166_1)
        )
        """.strip()
    )
    con.execute("CREATE INDEX IF NOT EXISTS title_translations_lookup_idx ON title_translations(media_type, tmdb_id, iso_639_1)")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_translations_done(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          fetched_at INTEGER NOT NULL,
          PRIMARY KEY(media_type, tmdb_id)
        )
        """.strip()
    )

    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "movies" not in tables or "series" not in tables:
        print("catalog.sqlite must have movies and series tables", file=sys.stderr)
        return 1

    vote_min = 0
    rps = 47
    workers = 50

    work: List[Tuple[str, int]] = []
    for mid, in con.execute(
        """
        SELECT id FROM movies
        WHERE COALESCE(vote_count,0) >= ?
          AND NOT EXISTS (SELECT 1 FROM title_translations_done d WHERE d.media_type='movie' AND d.tmdb_id=movies.id)
        """.strip(),
        (vote_min,),
    ):
        work.append(("movie", int(mid)))

    for sid, in con.execute(
        """
        SELECT id FROM series
        WHERE COALESCE(vote_count,0) >= ?
          AND NOT EXISTS (SELECT 1 FROM title_translations_done d WHERE d.media_type='tv' AND d.tmdb_id=series.id)
        """.strip(),
        (vote_min,),
    ):
        work.append(("tv", int(sid)))

    if not work:
        print("nothing to do")
        return 0

    limiter = RateLimiter(rps)
    tls = local()

    def session_factory() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = requests.Session()
            tls.s = s
        return s

    rows: List[Tuple[str, int, str, str, str, str, str, str]] = []
    done_rows: List[Tuple[str, int, int]] = []
    done = 0

    def flush():
        nonlocal rows, done_rows
        if rows:
            con.executemany(
                """
                INSERT INTO title_translations(media_type, tmdb_id, iso_639_1, iso_3166_1, title, overview, tagline, homepage)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(media_type, tmdb_id, iso_639_1, iso_3166_1) DO UPDATE SET
                  title=excluded.title,
                  overview=excluded.overview,
                  tagline=excluded.tagline,
                  homepage=excluded.homepage
                """.strip(),
                rows,
            )
        if done_rows:
            con.executemany(
                """
                INSERT INTO title_translations_done(media_type, tmdb_id, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                done_rows,
            )
        con.commit()
        rows = []
        done_rows = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_translations, session_factory, limiter, auth, mt, tid): (mt, tid) for (mt, tid) in work}
        for fut in as_completed(futs):
            mt, tid, data = fut.result()
            now = int(time.time())

            if isinstance(data, dict):
                for t in data.get("translations") or []:
                    if not isinstance(t, dict):
                        continue
                    iso639 = (t.get("iso_639_1") or "und").strip() or "und"
                    iso3166 = (t.get("iso_3166_1") or "ZZ").strip() or "ZZ"
                    d = t.get("data") or {}
                    if not isinstance(d, dict):
                        continue
                    title = (d.get("title") or d.get("name") or "").strip()
                    overview = (d.get("overview") or "").strip()
                    tagline = (d.get("tagline") or "").strip()
                    homepage = (d.get("homepage") or "").strip()
                    if not (title or overview or tagline or homepage):
                        continue
                    rows.append((mt, tid, iso639, iso3166, title, overview, tagline, homepage))

            done_rows.append((mt, tid, now))
            done += 1
            if (len(rows) + len(done_rows)) >= 2000:
                flush()
            if done % 500 == 0:
                print(f"{done}/{len(work)}")

    flush()
    con.close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
