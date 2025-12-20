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


def tmdb_get(session: requests.Session, limiter: RateLimiter, auth: Tuple[str, str], url: str, params: Optional[Dict[str, str]] = None):
    headers = {"accept": "application/json"}
    p = dict(params or {})
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


def fetch_tv_details(session_factory, limiter: RateLimiter, auth: Tuple[str, str], series_id: int, lang: str):
    r = tmdb_get(session_factory(), limiter, auth, f"https://api.themoviedb.org/3/tv/{series_id}", {"language": lang})
    if not r or r.status_code != 200:
        return (series_id, None)
    try:
        return (series_id, r.json())
    except ValueError:
        return (series_id, None)


def fetch_season(session_factory, limiter: RateLimiter, auth: Tuple[str, str], series_id: int, season_number: int, lang: str):
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    r = tmdb_get(session_factory(), limiter, auth, url, {"language": lang})
    if not r or r.status_code != 200:
        return (series_id, season_number, None)
    try:
        return (series_id, season_number, r.json())
    except ValueError:
        return (series_id, season_number, None)


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

    vote_min = 0
    rps = 47
    workers = 50
    lang = (env.get("TMDB_LANGUAGE") or "en-US").strip() or "en-US"

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA busy_timeout=30000")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_seasons(
          series_id INTEGER NOT NULL,
          season_number INTEGER NOT NULL,
          season_id INTEGER,
          name TEXT,
          overview TEXT,
          air_date TEXT,
          poster_path TEXT,
          episode_count INTEGER,
          PRIMARY KEY(series_id, season_number)
        )
        """.strip()
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_episodes(
          series_id INTEGER NOT NULL,
          season_number INTEGER NOT NULL,
          episode_number INTEGER NOT NULL,
          episode_id INTEGER,
          name TEXT,
          overview TEXT,
          air_date TEXT,
          runtime INTEGER,
          still_path TEXT,
          vote_average REAL,
          vote_count INTEGER,
          PRIMARY KEY(series_id, season_number, episode_number)
        )
        """.strip()
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_season_done(
          series_id INTEGER NOT NULL,
          season_number INTEGER NOT NULL,
          fetched_at INTEGER NOT NULL,
          PRIMARY KEY(series_id, season_number)
        )
        """.strip()
    )

    series_ids = [
        int(r[0])
        for r in con.execute(
            "SELECT id FROM series WHERE COALESCE(vote_count,0) >= ? ORDER BY vote_count DESC", (vote_min,)
        )
    ]
    if not series_ids:
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

    seasons_to_fetch = []
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_tv_details, session_factory, limiter, auth, sid, lang): sid for sid in series_ids}
        for fut in as_completed(futs):
            sid, data = fut.result()
            done += 1
            if data and isinstance(data, dict):
                for s in data.get("seasons") or []:
                    if not isinstance(s, dict):
                        continue
                    sn = s.get("season_number")
                    if sn is None:
                        continue
                    try:
                        sn = int(sn)
                    except Exception:
                        continue

                    con.execute(
                        """
                        INSERT INTO tv_seasons(series_id, season_number, season_id, name, overview, air_date, poster_path, episode_count)
                        VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(series_id, season_number) DO UPDATE SET
                          season_id=excluded.season_id,
                          name=excluded.name,
                          overview=excluded.overview,
                          air_date=excluded.air_date,
                          poster_path=excluded.poster_path,
                          episode_count=excluded.episode_count
                        """.strip(),
                        (
                            sid,
                            sn,
                            s.get("id"),
                            s.get("name"),
                            s.get("overview"),
                            s.get("air_date"),
                            s.get("poster_path"),
                            s.get("episode_count"),
                        ),
                    )

                    already = con.execute(
                        "SELECT 1 FROM tv_season_done WHERE series_id=? AND season_number=? LIMIT 1", (sid, sn)
                    ).fetchone()
                    if not already:
                        seasons_to_fetch.append((sid, sn))

            if done % 200 == 0:
                con.commit()
                print(f"series_details={done}/{len(series_ids)} seasons_pending={len(seasons_to_fetch)}")

    con.commit()

    if not seasons_to_fetch:
        print("nothing to do")
        return 0

    ep_rows = []
    done_rows = []
    fetched = 0

    def flush():
        nonlocal ep_rows, done_rows
        if ep_rows:
            con.executemany(
                """
                INSERT INTO tv_episodes(series_id, season_number, episode_number, episode_id, name, overview, air_date, runtime, still_path, vote_average, vote_count)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(series_id, season_number, episode_number) DO UPDATE SET
                  episode_id=excluded.episode_id,
                  name=excluded.name,
                  overview=excluded.overview,
                  air_date=excluded.air_date,
                  runtime=excluded.runtime,
                  still_path=excluded.still_path,
                  vote_average=excluded.vote_average,
                  vote_count=excluded.vote_count
                """.strip(),
                ep_rows,
            )
            ep_rows = []
        if done_rows:
            con.executemany(
                """
                INSERT INTO tv_season_done(series_id, season_number, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(series_id, season_number) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                done_rows,
            )
            done_rows = []
        con.commit()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(fetch_season, session_factory, limiter, auth, sid, sn, lang): (sid, sn) for (sid, sn) in seasons_to_fetch
        }
        for fut in as_completed(futs):
            sid, sn = futs[fut]
            _sid, _sn, data = fut.result()
            fetched += 1
            now = int(time.time())

            if data and isinstance(data, dict):
                for e in data.get("episodes") or []:
                    if not isinstance(e, dict):
                        continue
                    en = e.get("episode_number")
                    if en is None:
                        continue
                    try:
                        en = int(en)
                    except Exception:
                        continue
                    ep_rows.append(
                        (
                            sid,
                            sn,
                            en,
                            e.get("id"),
                            e.get("name"),
                            e.get("overview"),
                            e.get("air_date"),
                            e.get("runtime"),
                            e.get("still_path"),
                            e.get("vote_average"),
                            e.get("vote_count"),
                        )
                    )
                done_rows.append((sid, sn, now))

            if (len(ep_rows) + len(done_rows)) >= 2000:
                flush()
            if fetched % 200 == 0:
                print(f"seasons_fetched={fetched}/{len(seasons_to_fetch)}")

    flush()

    con.execute("CREATE INDEX IF NOT EXISTS tv_episodes_series_season_idx ON tv_episodes(series_id, season_number, episode_number)")
    con.commit()
    con.close()

    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

