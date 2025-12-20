#!/usr/bin/env python3
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
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


def fetch_videos(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int, lang: str):
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/videos"
    headers = {"accept": "application/json"}
    params = {"language": lang}
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params["api_key"] = auth[1]

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


def pick_first_trailer(payload: dict):
    for it in payload.get("results") or []:
        if not isinstance(it, dict):
            continue
        if (it.get("type") or "").lower() == "trailer":
            return it
    return None


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print("missing TMDB token in .env", file=sys.stderr)
        return 1

    db_path = env.get("CATALOG_DB") or os.path.join(cwd, "catalog.sqlite")
    if not os.path.isfile(db_path):
        print(f"missing catalog sqlite: {db_path}", file=sys.stderr)
        return 1

    rps = 47
    workers = 50
    lang = "en-US"

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA busy_timeout=30000")

    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "movies" not in tables or "series" not in tables:
        print("catalog.sqlite must have movies and series tables", file=sys.stderr)
        return 1

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_videos(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          video_id TEXT,
          key TEXT,
          site TEXT,
          name TEXT,
          type TEXT,
          official INTEGER,
          published_at TEXT,
          iso_639_1 TEXT,
          iso_3166_1 TEXT,
          size INTEGER,
          PRIMARY KEY(media_type, tmdb_id)
        )
        """.strip()
    )
    con.execute("CREATE INDEX IF NOT EXISTS title_videos_key_idx ON title_videos(site, key)")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_videos_done(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          fetched_at INTEGER NOT NULL,
          PRIMARY KEY(media_type, tmdb_id)
        )
        """.strip()
    )

    work = []
    for mid, in con.execute(
        """
        SELECT id FROM movies
        WHERE NOT EXISTS (SELECT 1 FROM title_videos_done d WHERE d.media_type='movie' AND d.tmdb_id=movies.id)
        """.strip()
    ):
        work.append(("movie", int(mid)))

    for sid, in con.execute(
        """
        SELECT id FROM series
        WHERE NOT EXISTS (SELECT 1 FROM title_videos_done d WHERE d.media_type='tv' AND d.tmdb_id=series.id)
        """.strip()
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

    rows = []
    done_rows = []
    done = 0

    def flush():
        nonlocal rows, done_rows
        if rows:
            con.executemany(
                """
                INSERT INTO title_videos(media_type, tmdb_id, video_id, key, site, name, type, official, published_at, iso_639_1, iso_3166_1, size)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                  video_id=excluded.video_id,
                  key=excluded.key,
                  site=excluded.site,
                  name=excluded.name,
                  type=excluded.type,
                  official=excluded.official,
                  published_at=excluded.published_at,
                  iso_639_1=excluded.iso_639_1,
                  iso_3166_1=excluded.iso_3166_1,
                  size=excluded.size
                """.strip(),
                rows,
            )
            rows = []
        if done_rows:
            con.executemany(
                """
                INSERT INTO title_videos_done(media_type, tmdb_id, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                done_rows,
            )
            done_rows = []
        con.commit()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_videos, session_factory, limiter, auth, mt, tid, lang): (mt, tid) for (mt, tid) in work}
        for fut in as_completed(futs):
            mt, tid = futs[fut]
            _mt, _tid, data = fut.result()
            now = int(time.time())

            if isinstance(data, dict):
                t = pick_first_trailer(data)
                if isinstance(t, dict):
                    rows.append(
                        (
                            mt,
                            tid,
                            t.get("id"),
                            t.get("key"),
                            t.get("site"),
                            t.get("name"),
                            t.get("type"),
                            1 if t.get("official") else 0,
                            t.get("published_at"),
                            t.get("iso_639_1"),
                            t.get("iso_3166_1"),
                            t.get("size"),
                        )
                    )

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
