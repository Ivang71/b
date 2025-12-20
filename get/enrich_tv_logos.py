#!/usr/bin/env python3
import csv
import json
import os
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


def fetch_best_tv_logos(session_factory, limiter: RateLimiter, auth: Tuple[str, str], tv_id: int):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/images"
    headers = {"accept": "application/json"}
    params = None
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params = {"api_key": auth[1]}

    for attempt in range(5):
        limiter.wait()
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
            return (tv_id, None)

        try:
            data = r.json()
        except ValueError:
            return (tv_id, None)

        best = {}
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
        return (tv_id, best)

    return (tv_id, None)


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print("missing TMDB token in .env (TMDB_BEARER_TOKEN/TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY)", file=sys.stderr)
        return 1

    rps = 47
    workers = 50
    limiter = RateLimiter(rps)
    tls = local()

    def session_factory() -> requests.Session:
        s = getattr(tls, "s", None)
        if s is None:
            s = requests.Session()
            tls.s = s
        return s

    csvs = [f for f in os.listdir(cwd) if f.lower().endswith(".csv")]
    picks = []
    for fn in csvs:
        try:
            with open(os.path.join(cwd, fn), "r", encoding="utf-8", newline="") as f:
                r = csv.reader(f)
                header = next(r, [])
        except Exception:
            continue
        hs = {h.strip() for h in header}
        if "first_air_date" in hs and "number_of_seasons" in hs and "vote_count" in hs:
            picks.append(fn)
    if len(picks) != 1:
        print(f"expected exactly 1 tv-series csv (has first_air_date/number_of_seasons/vote_count), found {len(picks)}", file=sys.stderr)
        for f in sorted(picks):
            print(f, file=sys.stderr)
        return 1

    in_csv = os.path.join(cwd, picks[0])
    out_db = os.path.join(cwd, os.path.splitext(picks[0])[0] + "_enriched.sqlite")

    with open(in_csv, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fieldnames = list(r.fieldnames or [])

    if not rows:
        print("empty csv")
        return 0

    if "logos_json" not in fieldnames:
        fieldnames.append("logos_json")

    def to_int(x):
        try:
            return int(float((x or "").strip() or "0"))
        except Exception:
            return 0

    def to_float(x):
        try:
            return float((x or "").strip() or "0")
        except Exception:
            return 0.0

    def to_bool_int(x):
        s = (x or "").strip().lower()
        if s in ("1", "true", "t", "yes", "y"):
            return 1
        if s in ("0", "false", "f", "no", "n"):
            return 0
        return None

    work = []
    filtered = []
    for row in rows:
        tid = to_int(row.get("id"))
        if tid <= 0:
            continue
        if to_int(row.get("vote_count")) <= 200:
            continue
        filtered.append(row)
        if not (row.get("logos_json") or "").strip():
            work.append(tid)

    if not filtered:
        print("no rows with vote_count > 200")
        return 0

    import sqlite3

    con = sqlite3.connect(out_db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA busy_timeout=30000")

    con.execute("DROP TABLE IF EXISTS series")
    con.execute(
        """
        CREATE TABLE series(
          id INTEGER PRIMARY KEY,
          name TEXT,
          vote_average REAL,
          vote_count INTEGER,
          status TEXT,
          first_air_date TEXT,
          last_air_date TEXT,
          number_of_seasons INTEGER,
          number_of_episodes INTEGER,
          in_production INTEGER,
          adult INTEGER,
          backdrop_path TEXT,
          poster_path TEXT,
          original_language TEXT,
          original_name TEXT,
          overview TEXT,
          popularity REAL,
          tagline TEXT,
          genres TEXT,
          networks TEXT,
          origin_country TEXT,
          spoken_languages TEXT,
          logos_json TEXT
        )
        """.strip()
    )

    def row_to_tuple(row):
        return (
            to_int(row.get("id")),
            row.get("name"),
            to_float(row.get("vote_average")),
            to_int(row.get("vote_count")),
            row.get("status"),
            row.get("first_air_date"),
            row.get("last_air_date"),
            to_int(row.get("number_of_seasons")),
            to_int(row.get("number_of_episodes")),
            to_bool_int(row.get("in_production")),
            to_bool_int(row.get("adult")),
            row.get("backdrop_path"),
            row.get("poster_path"),
            row.get("original_language"),
            row.get("original_name"),
            row.get("overview"),
            to_float(row.get("popularity")),
            row.get("tagline"),
            row.get("genres"),
            row.get("networks"),
            row.get("origin_country"),
            row.get("spoken_languages"),
            (row.get("logos_json") or "").strip() or None,
        )

    con.executemany(
        """
        INSERT INTO series(
          id,name,vote_average,vote_count,status,first_air_date,last_air_date,number_of_seasons,
          number_of_episodes,in_production,adult,backdrop_path,poster_path,original_language,
          original_name,overview,popularity,tagline,genres,networks,origin_country,spoken_languages,logos_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """.strip(),
        [row_to_tuple(r) for r in filtered],
    )
    con.commit()

    done = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_best_tv_logos, session_factory, limiter, auth, tid): tid for tid in work}
        for fut in as_completed(futs):
            tid = futs[fut]
            tv_id, best = fut.result()
            if best:
                s = json.dumps({k: v[0] for k, v in best.items()}, separators=(",", ":"))
                con.execute("UPDATE series SET logos_json=? WHERE id=?", (s, tv_id))

            done += 1
            if done % 500 == 0:
                con.commit()
                print(f"{done}/{len(work)}")

    con.commit()
    con.execute("CREATE INDEX IF NOT EXISTS series_vote_count_idx ON series(vote_count DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS series_popularity_idx ON series(popularity DESC)")
    con.commit()
    con.close()
    print(f"wrote {os.path.basename(out_db)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
