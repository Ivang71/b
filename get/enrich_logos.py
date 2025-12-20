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
    out = {}
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
    for k in (
        "TMDB_BEARER_TOKEN",
        "TMDB_API_READ_ACCESS_TOKEN",
        "TMDB_ACCESS_TOKEN",
        "TMDB_TOKEN",
    ):
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


def fetch_best_logos(session_factory, limiter: RateLimiter, auth: Tuple[str, str], movie_id: int) -> Dict[str, Tuple[str, float]]:
    limiter.wait()
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/images"
    headers = {"accept": "application/json"}
    params = None
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params = {"api_key": auth[1]}

    for attempt in range(5):
        try:
            r = session_factory().get(url, headers=headers, params=params, timeout=20)
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
            return {}

        try:
            data = r.json()
        except ValueError:
            return {}

        best: Dict[str, Tuple[str, float]] = {}
        for it in data.get("logos") or []:
            fp = it.get("file_path")
            ar = it.get("aspect_ratio")
            loc = it.get("iso_639_1") or "und"
            if not fp or ar is None:
                continue
            try:
                ar = float(ar)
            except Exception:
                continue
            cur = best.get(loc)
            if cur is None or ar < cur[1]:
                best[loc] = (fp, ar)
        return best

    return {}


def main():
    cwd = os.getcwd()
    sqlite_files = [f for f in os.listdir(cwd) if f.lower().endswith(".sqlite")]
    if len(sqlite_files) != 1:
        print(f"expected exactly 1 .sqlite in current dir, found {len(sqlite_files)}", file=sys.stderr)
        for f in sorted(sqlite_files):
            print(f, file=sys.stderr)
        return 1

    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print(
            "missing TMDB token in .env (TMDB_BEARER_TOKEN/TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY)",
            file=sys.stderr,
        )
        return 1

    db_path = os.path.join(cwd, sqlite_files[0])
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA busy_timeout=30000")

    try:
        con.execute("ALTER TABLE movies ADD COLUMN logos_json TEXT")
    except sqlite3.OperationalError:
        pass

    ids = [
        r[0]
        for r in con.execute(
            "SELECT id FROM movies WHERE COALESCE(vote_count,0) > 500 AND (logos_json IS NULL OR logos_json='')"
        )
    ]

    if not ids:
        print("nothing to do")
        return 0

    limiter = RateLimiter(47)
    tls = local()

    def session_factory() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = requests.Session()
            tls.s = s
        return s

    movie_updates: List[Tuple[str, int]] = []
    done = 0

    def flush():
        nonlocal movie_updates
        if not movie_updates:
            return
        con.executemany("UPDATE movies SET logos_json=? WHERE id=?", movie_updates)
        con.commit()
        movie_updates = []

    max_workers = 50

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_best_logos, session_factory, limiter, auth, mid): mid for mid in ids}
        for fut in as_completed(futs):
            mid = futs[fut]
            best = {}
            try:
                best = fut.result()
            except Exception:
                best = {}

            if best:
                s = "{%s}" % ",".join(
                    '"%s":"%s"' % (k.replace('"', ""), v[0].replace('"', ""))
                    for k, v in sorted(best.items())
                )
                movie_updates.append((s, mid))

            done += 1
            if len(movie_updates) >= 2000:
                flush()
            if done % 500 == 0:
                print(f"{done}/{len(ids)}")

    flush()
    con.close()
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
