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


def tmdb_get(session: requests.Session, limiter: RateLimiter, auth: Tuple[str, str], url: str, params: Dict[str, str]):
    headers = {"accept": "application/json"}
    p = dict(params)
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        p["api_key"] = auth[1]

    for attempt in range(6):
        limiter.wait()
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


def fetch_genre_list(session: requests.Session, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, lang: str):
    url = f"https://api.themoviedb.org/3/genre/{media_type}/list"
    r = tmdb_get(session, limiter, auth, url, {"language": lang})
    if not r or r.status_code != 200:
        return []
    try:
        data = r.json()
    except ValueError:
        return []
    out = []
    for g in data.get("genres") or []:
        if not isinstance(g, dict):
            continue
        gid = g.get("id")
        name = g.get("name")
        if gid is None:
            continue
        try:
            gid = int(gid)
        except Exception:
            continue
        if not name:
            continue
        out.append((media_type, gid, str(name)))
    return out


def fetch_title_genres(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int, lang: str):
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
    r = tmdb_get(session_factory(), limiter, auth, url, {"language": lang})
    if not r or r.status_code != 200:
        return (media_type, tmdb_id, None)
    try:
        data = r.json()
    except ValueError:
        return (media_type, tmdb_id, None)

    items = []
    for g in data.get("genres") or []:
        if not isinstance(g, dict):
            continue
        gid = g.get("id")
        name = g.get("name")
        if gid is None:
            continue
        try:
            gid = int(gid)
        except Exception:
            continue
        items.append((gid, str(name) if name else None))
    return (media_type, tmdb_id, items)


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

    rps = float(env.get("TMDB_RPS") or "30")
    workers = int(env.get("TMDB_WORKERS") or "32")
    lang = (env.get("TMDB_LANGUAGE") or "en-US").strip() or "en-US"

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
        CREATE TABLE IF NOT EXISTS genres(
          media_type TEXT NOT NULL,
          genre_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          PRIMARY KEY(media_type, genre_id)
        )
        """.strip()
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_genres(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          genre_id INTEGER NOT NULL,
          PRIMARY KEY(media_type, tmdb_id, genre_id)
        )
        """.strip()
    )
    con.execute("CREATE INDEX IF NOT EXISTS title_genres_lookup_idx ON title_genres(media_type, tmdb_id)")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS title_genres_done(
          media_type TEXT NOT NULL,
          tmdb_id INTEGER NOT NULL,
          fetched_at INTEGER NOT NULL,
          PRIMARY KEY(media_type, tmdb_id)
        )
        """.strip()
    )

    limiter = RateLimiter(rps)
    s0 = requests.Session()

    gl = []
    gl += fetch_genre_list(s0, limiter, auth, "movie", lang)
    gl += fetch_genre_list(s0, limiter, auth, "tv", lang)
    if gl:
        con.executemany(
            """
            INSERT INTO genres(media_type, genre_id, name)
            VALUES(?,?,?)
            ON CONFLICT(media_type, genre_id) DO UPDATE SET name=excluded.name
            """.strip(),
            gl,
        )
        con.commit()

    work = []
    for mid, in con.execute(
        """
        SELECT id FROM movies
        WHERE NOT EXISTS (SELECT 1 FROM title_genres_done d WHERE d.media_type='movie' AND d.tmdb_id=movies.id)
        """.strip()
    ):
        work.append(("movie", int(mid)))

    for sid, in con.execute(
        """
        SELECT id FROM series
        WHERE NOT EXISTS (SELECT 1 FROM title_genres_done d WHERE d.media_type='tv' AND d.tmdb_id=series.id)
        """.strip()
    ):
        work.append(("tv", int(sid)))

    if not work:
        print("nothing to do")
        return 0

    tls = local()

    def session_factory() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = requests.Session()
            tls.s = s
        return s

    tg_rows = []
    g_rows = []
    done_rows = []
    done = 0

    def flush():
        nonlocal tg_rows, g_rows, done_rows
        if g_rows:
            con.executemany(
                """
                INSERT INTO genres(media_type, genre_id, name)
                VALUES(?,?,?)
                ON CONFLICT(media_type, genre_id) DO UPDATE SET name=excluded.name
                """.strip(),
                g_rows,
            )
            g_rows = []
        if tg_rows:
            con.executemany(
                "INSERT OR IGNORE INTO title_genres(media_type, tmdb_id, genre_id) VALUES(?,?,?)",
                tg_rows,
            )
            tg_rows = []
        if done_rows:
            con.executemany(
                """
                INSERT INTO title_genres_done(media_type, tmdb_id, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                done_rows,
            )
            done_rows = []
        con.commit()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(fetch_title_genres, session_factory, limiter, auth, mt, tid, lang): (mt, tid) for (mt, tid) in work
        }
        for fut in as_completed(futs):
            mt, tid = futs[fut]
            _mt, _tid, items = fut.result()
            now = int(time.time())

            if items:
                for gid, name in items:
                    tg_rows.append((mt, tid, gid))
                    if name:
                        g_rows.append((mt, gid, name))

            done_rows.append((mt, tid, now))
            done += 1
            if (len(tg_rows) + len(g_rows) + len(done_rows)) >= 4000:
                flush()
            if done % 1000 == 0:
                print(f"{done}/{len(work)}")

    flush()
    con.close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
