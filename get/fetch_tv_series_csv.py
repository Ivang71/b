#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from threading import Lock
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
            r = session.get(url, headers=headers, params=p, timeout=20)
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


def get_latest_tv_id(session: requests.Session, limiter: RateLimiter, auth: Tuple[str, str]) -> int:
    r = tmdb_get(session, limiter, auth, "https://api.themoviedb.org/3/tv/latest")
    if not r or r.status_code != 200:
        raise RuntimeError(f"failed to fetch latest tv id: {None if not r else r.status_code}")
    j = r.json()
    return int(j.get("id") or 0)


def fetch_tv_details(session: requests.Session, limiter: RateLimiter, auth: Tuple[str, str], tv_id: int):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    r = tmdb_get(session, limiter, auth, url, {"language": "en-US"})
    if not r:
        return (tv_id, "err", None)
    if r.status_code == 404:
        return (tv_id, "404", None)
    if r.status_code != 200:
        return (tv_id, str(r.status_code), None)
    try:
        return (tv_id, "200", r.json())
    except ValueError:
        return (tv_id, "err", None)


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print("missing TMDB token in .env (TMDB_BEARER_TOKEN/TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY)", file=sys.stderr)
        return 1

    rps = 47
    workers = 50
    stop_404 = 470

    limiter = RateLimiter(rps)
    base_session = requests.Session()

    latest_id = get_latest_tv_id(base_session, limiter, auth)
    if latest_id <= 0:
        print("latest tv id invalid", file=sys.stderr)
        return 1

    out_csv = env.get("TMDB_TV_OUT") or "TMDB_tv_series_en.csv"
    header = [
        "id",
        "name",
        "vote_average",
        "vote_count",
        "status",
        "first_air_date",
        "last_air_date",
        "number_of_seasons",
        "number_of_episodes",
        "in_production",
        "adult",
        "backdrop_path",
        "poster_path",
        "original_language",
        "original_name",
        "overview",
        "popularity",
        "tagline",
        "genres",
        "networks",
        "origin_country",
        "spoken_languages",
        "seasons_json",
    ]

    next_id = 1
    expected = 1
    consec_404 = 0
    seen = 0
    kept = 0

    inflight_limit = max(256, workers * 8)
    pending = set()
    results: Dict[int, Tuple[str, object]] = {}

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            while True:
                while next_id <= latest_id and len(pending) < inflight_limit:
                    s = requests.Session()
                    fut = ex.submit(fetch_tv_details, s, limiter, auth, next_id)
                    pending.add(fut)
                    next_id += 1

                if not pending and expected > latest_id:
                    break

                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    tv_id, st, data = fut.result()
                    results[tv_id] = (st, data)

                while expected in results:
                    st, data = results.pop(expected)
                    seen += 1
                    if st == "404":
                        consec_404 += 1
                    else:
                        consec_404 = 0

                    if st == "200" and isinstance(data, dict):
                        def join_names(key: str) -> str:
                            xs = data.get(key) or []
                            return ", ".join(x.get("name") for x in xs if isinstance(x, dict) and x.get("name"))

                        seasons = []
                        for s in data.get("seasons") or []:
                            if not isinstance(s, dict):
                                continue
                            seasons.append(
                                {
                                    "season_number": s.get("season_number"),
                                    "episode_count": s.get("episode_count"),
                                    "id": s.get("id"),
                                    "name": s.get("name"),
                                    "air_date": s.get("air_date"),
                                    "poster_path": s.get("poster_path"),
                                }
                            )

                        w.writerow(
                            [
                                data.get("id"),
                                data.get("name"),
                                data.get("vote_average"),
                                data.get("vote_count"),
                                data.get("status"),
                                data.get("first_air_date"),
                                data.get("last_air_date"),
                                data.get("number_of_seasons"),
                                data.get("number_of_episodes"),
                                data.get("in_production"),
                                data.get("adult"),
                                data.get("backdrop_path"),
                                data.get("poster_path"),
                                data.get("original_language"),
                                data.get("original_name"),
                                (data.get("overview") or "").replace("\n", " ").strip(),
                                data.get("popularity"),
                                data.get("tagline"),
                                join_names("genres"),
                                join_names("networks"),
                                ", ".join(data.get("origin_country") or []),
                                join_names("spoken_languages"),
                                json.dumps(seasons, separators=(",", ":")),
                            ]
                        )
                        kept += 1

                    if seen % 1000 == 0:
                        print(f"seen={seen} kept={kept} expected={expected} latest={latest_id} consec404={consec_404}", file=sys.stderr)

                    expected += 1
                    if consec_404 >= stop_404:
                        pending.clear()
                        break

                if consec_404 >= stop_404:
                    break

    print(f"wrote {out_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
