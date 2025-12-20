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


def fetch_credits(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int):
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/credits"
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
        print("missing TMDB token in .env", file=sys.stderr)
        return 1

    db_path = env.get("CATALOG_DB") or os.path.join(cwd, "catalog.sqlite")
    if not os.path.isfile(db_path):
        print(f"missing catalog sqlite: {db_path}", file=sys.stderr)
        return 1

    rps = 47
    workers = 50
    cast_limit = 24

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
        CREATE TABLE IF NOT EXISTS title_cast(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          person_id INTEGER NOT NULL,
          credit_id TEXT NOT NULL,
          cast_id INTEGER,
          name TEXT,
          original_name TEXT,
          character TEXT,
          ord INTEGER,
          known_for_department TEXT,
          gender INTEGER,
          popularity REAL,
          profile_path TEXT,
          PRIMARY KEY(media_type, tmdb_id, credit_id)
        )
        """.strip()
    )
    con.execute("CREATE INDEX IF NOT EXISTS title_cast_lookup_idx ON title_cast(media_type, tmdb_id, ord)")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_cast_done(
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
        WHERE NOT EXISTS (SELECT 1 FROM title_cast_done d WHERE d.media_type='movie' AND d.tmdb_id=movies.id)
        """.strip()
    ):
        work.append(("movie", int(mid)))

    for sid, in con.execute(
        """
        SELECT id FROM series
        WHERE NOT EXISTS (SELECT 1 FROM title_cast_done d WHERE d.media_type='tv' AND d.tmdb_id=series.id)
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
                INSERT INTO title_cast(
                  media_type, tmdb_id, person_id, credit_id, cast_id, name, original_name, character, ord,
                  known_for_department, gender, popularity, profile_path
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(media_type, tmdb_id, credit_id) DO UPDATE SET
                  person_id=excluded.person_id,
                  cast_id=excluded.cast_id,
                  name=excluded.name,
                  original_name=excluded.original_name,
                  character=excluded.character,
                  ord=excluded.ord,
                  known_for_department=excluded.known_for_department,
                  gender=excluded.gender,
                  popularity=excluded.popularity,
                  profile_path=excluded.profile_path
                """.strip(),
                rows,
            )
            rows = []
        if done_rows:
            con.executemany(
                """
                INSERT INTO title_cast_done(media_type, tmdb_id, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                done_rows,
            )
            done_rows = []
        con.commit()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_credits, session_factory, limiter, auth, mt, tid): (mt, tid) for (mt, tid) in work}
        for fut in as_completed(futs):
            mt, tid = futs[fut]
            _mt, _tid, data = fut.result()
            now = int(time.time())

            if isinstance(data, dict):
                cast = data.get("cast") or []
                if isinstance(cast, list):
                    for i, c in enumerate(cast[:cast_limit]):
                        if not isinstance(c, dict):
                            continue
                        credit_id = c.get("credit_id")
                        pid = c.get("id")
                        if not credit_id or pid is None:
                            continue
                        rows.append(
                            (
                                mt,
                                tid,
                                int(pid),
                                str(credit_id),
                                c.get("cast_id"),
                                c.get("name"),
                                c.get("original_name"),
                                c.get("character"),
                                c.get("order") if c.get("order") is not None else i,
                                c.get("known_for_department"),
                                c.get("gender"),
                                c.get("popularity"),
                                c.get("profile_path"),
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
