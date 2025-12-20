#!/usr/bin/env python3
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from threading import Lock, local
from typing import Dict, Iterable, List, Optional, Tuple

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


def fetch_images(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int):
    limiter.wait()
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/images"
    headers = {"accept": "application/json"}
    params = None
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params = {"api_key": auth[1]}

    for attempt in range(5):
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
            data = r.json()
        except ValueError:
            return (media_type, tmdb_id, None)

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
        return (media_type, tmdb_id, best)

    return (media_type, tmdb_id, None)


def fetch_translations(session_factory, limiter: RateLimiter, auth: Tuple[str, str], media_type: str, tmdb_id: int):
    limiter.wait()
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/translations"
    headers = {"accept": "application/json"}
    params = None
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params = {"api_key": auth[1]}

    for attempt in range(6):
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


def fetch_tv_details(session_factory, limiter: RateLimiter, auth: Tuple[str, str], series_id: int, lang: str):
    limiter.wait()
    url = f"https://api.themoviedb.org/3/tv/{series_id}"
    headers = {"accept": "application/json"}
    params = {"language": lang}
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params["api_key"] = auth[1]

    for attempt in range(6):
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
            return (series_id, None)
        try:
            return (series_id, r.json())
        except ValueError:
            return (series_id, None)
    return (series_id, None)


def fetch_tv_season(session_factory, limiter: RateLimiter, auth: Tuple[str, str], series_id: int, season_number: int, lang: str):
    limiter.wait()
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    headers = {"accept": "application/json"}
    params = {"language": lang}
    if auth[0] == "bearer":
        headers["Authorization"] = f"Bearer {auth[1]}"
    else:
        params["api_key"] = auth[1]

    for attempt in range(6):
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
            return (series_id, season_number, None)
        try:
            return (series_id, season_number, r.json())
        except ValueError:
            return (series_id, season_number, None)
    return (series_id, season_number, None)


def iso(d: date) -> str:
    return d.isoformat()


def chunk(xs: List[int], n: int) -> Iterable[List[int]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def ensure_col(con: sqlite3.Connection, table: str, col: str, decl: str):
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    if col in cols:
        return
    try:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    except sqlite3.OperationalError:
        pass


def upsert(con: sqlite3.Connection, table: str, rows: List[Dict[str, object]]):
    if not rows:
        return 0
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
    colset = set(cols)
    keys = [k for k in rows[0].keys() if k in colset and k != "logos_json"]
    if "id" not in keys:
        keys = ["id"] + [k for k in keys if k != "id"]
    keys = [k for k in keys if k in colset]
    if not keys:
        return 0

    ph = ",".join("?" for _ in keys)
    cl = ",".join(keys)
    upd = ",".join(f"{k}=excluded.{k}" for k in keys if k != "id")

    con.executemany(
        f"INSERT INTO {table}({cl}) VALUES({ph}) ON CONFLICT(id) DO UPDATE SET {upd}",
        [[r.get(k) for k in keys] for r in rows],
    )
    return len(rows)


def main() -> int:
    cwd = os.getcwd()
    env = {**load_env(os.path.join(cwd, ".env")), **os.environ}
    auth = pick_token(env)
    if not auth:
        print(
            "missing TMDB token in .env (TMDB_BEARER_TOKEN/TMDB_API_READ_ACCESS_TOKEN or TMDB_API_KEY)",
            file=sys.stderr,
        )
        return 1

    db_path = env.get("CATALOG_DB") or os.path.join(cwd, "catalog.sqlite")
    if not os.path.isfile(db_path):
        print(f"missing catalog sqlite: {db_path}", file=sys.stderr)
        return 1

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
        CREATE TABLE IF NOT EXISTS sync_runs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ran_at INTEGER NOT NULL,
          window_days INTEGER NOT NULL,
          region TEXT,
          movie_rows INTEGER NOT NULL,
          tv_rows INTEGER NOT NULL,
          movie_logos INTEGER NOT NULL,
          tv_logos INTEGER NOT NULL,
          movie_trans INTEGER NOT NULL,
          tv_trans INTEGER NOT NULL,
          tv_seasons INTEGER NOT NULL,
          tv_episodes INTEGER NOT NULL
        )
        """.strip()
    )
    ensure_col(con, "sync_runs", "tv_seasons", "INTEGER NOT NULL DEFAULT 0")
    ensure_col(con, "sync_runs", "tv_episodes", "INTEGER NOT NULL DEFAULT 0")

    ensure_col(con, "movies", "logos_json", "TEXT")
    ensure_col(con, "series", "logos_json", "TEXT")

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
    con.execute("CREATE INDEX IF NOT EXISTS title_translations_lookup_idx ON title_translations(media_type, tmdb_id, iso_639_1)")

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

    window_days = int(float(env.get("TMDB_WINDOW_DAYS") or "7"))
    region = (env.get("TMDB_REGION") or "").strip() or None
    lang = (env.get("TMDB_LANGUAGE") or "en-US").strip() or "en-US"
    max_pages = int(float(env.get("TMDB_MAX_PAGES") or "20"))
    min_movie_votes = int(float(env.get("TMDB_MIN_MOVIE_VOTE_COUNT") or "26"))

    today = date.today()
    start = today - timedelta(days=window_days)
    end = today + timedelta(days=1)

    sess = requests.Session()

    movie_ids: List[int] = []
    tv_ids: List[int] = []

    movie_rows = 0
    tv_rows = 0

    page = 1
    while page <= max_pages:
        params = {
            "sort_by": "primary_release_date.desc",
            "primary_release_date.gte": iso(start),
            "primary_release_date.lte": iso(end),
            "vote_count.gte": str(min_movie_votes),
            "include_adult": "false",
            "language": lang,
            "page": str(page),
        }
        if region:
            params["region"] = region
        r = tmdb_get(sess, auth, "https://api.themoviedb.org/3/discover/movie", params)
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
            if int(m.get("vote_count") or 0) < min_movie_votes:
                continue
            movie_ids.append(mid)
            rows.append(
                {
                    "id": mid,
                    "title": m.get("title") or None,
                    "vote_average": float(m.get("vote_average") or 0.0),
                    "vote_count": int(m.get("vote_count") or 0),
                    "release_date": m.get("release_date") or None,
                    "adult": 1 if m.get("adult") else 0,
                    "backdrop_path": m.get("backdrop_path") or None,
                    "original_language": m.get("original_language") or None,
                    "original_title": m.get("original_title") or None,
                    "overview": m.get("overview") or None,
                    "popularity": float(m.get("popularity") or 0.0),
                    "poster_path": m.get("poster_path") or None,
                }
            )

        movie_rows += upsert(con, "movies", rows)

        total_pages = int(data.get("total_pages") or 0)
        if total_pages and page >= total_pages:
            break
        page += 1

    page = 1
    while page <= max_pages:
        params = {
            "sort_by": "first_air_date.desc",
            "first_air_date.gte": iso(start),
            "first_air_date.lte": iso(end),
            "language": lang,
            "page": str(page),
        }
        if region:
            params["region"] = region
        r = tmdb_get(sess, auth, "https://api.themoviedb.org/3/discover/tv", params)
        if not r or r.status_code != 200:
            break
        data = r.json() or {}
        results = data.get("results") or []
        if not results:
            break

        rows = []
        for t in results:
            if not isinstance(t, dict):
                continue
            tid = int(t.get("id") or 0)
            if tid <= 0:
                continue
            tv_ids.append(tid)
            rows.append(
                {
                    "id": tid,
                    "name": t.get("name") or None,
                    "vote_average": float(t.get("vote_average") or 0.0),
                    "vote_count": int(t.get("vote_count") or 0),
                    "first_air_date": t.get("first_air_date") or None,
                    "overview": t.get("overview") or None,
                    "popularity": float(t.get("popularity") or 0.0),
                    "poster_path": t.get("poster_path") or None,
                    "backdrop_path": t.get("backdrop_path") or None,
                    "original_language": t.get("original_language") or None,
                    "original_name": t.get("original_name") or None,
                }
            )

        tv_rows += upsert(con, "series", rows)

        total_pages = int(data.get("total_pages") or 0)
        if total_pages and page >= total_pages:
            break
        page += 1

    movie_ids = sorted(set(movie_ids))
    tv_ids = sorted(set(tv_ids))

    def ids_missing_logos(table: str, ids: List[int]) -> List[int]:
        if not ids:
            return []
        out: List[int] = []
        for ch in chunk(ids, 900):
            q = ",".join("?" for _ in ch)
            out.extend(
                [
                    int(r[0])
                    for r in con.execute(
                        f"SELECT id FROM {table} WHERE id IN ({q}) AND (logos_json IS NULL OR logos_json='')",
                        ch,
                    )
                ]
            )
        return out

    def ids_missing_trans(media_type: str, ids: List[int]) -> List[int]:
        if not ids:
            return []
        have = set()
        for ch in chunk(ids, 900):
            q = ",".join("?" for _ in ch)
            have.update(
                [
                    int(r[0])
                    for r in con.execute(
                        f"SELECT tmdb_id FROM title_translations_done WHERE media_type=? AND tmdb_id IN ({q})",
                        [media_type, *ch],
                    )
                ]
            )
        return [i for i in ids if i not in have]

    need_movie_logos = ids_missing_logos("movies", movie_ids)
    need_tv_logos = ids_missing_logos("series", tv_ids)
    need_movie_trans = ids_missing_trans("movie", movie_ids)
    need_tv_trans = ids_missing_trans("tv", tv_ids)

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

    movie_logos = 0
    tv_logos = 0
    movie_trans = 0
    tv_trans = 0
    tv_seasons = 0
    tv_episodes = 0

    logo_updates: List[Tuple[str, int]] = []
    trans_rows: List[Tuple[str, int, str, str, str, str, str, str]] = []
    trans_done: List[Tuple[str, int, int]] = []

    def flush():
        nonlocal logo_updates, trans_rows, trans_done
        if logo_updates:
            con.executemany("UPDATE movies SET logos_json=? WHERE id=?", logo_updates)
            logo_updates = []
        if trans_rows:
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
                trans_rows,
            )
            trans_rows = []
        if trans_done:
            con.executemany(
                """
                INSERT INTO title_translations_done(media_type, tmdb_id, fetched_at)
                VALUES(?,?,?)
                ON CONFLICT(media_type, tmdb_id) DO UPDATE SET fetched_at=excluded.fetched_at
                """.strip(),
                trans_done,
            )
            trans_done = []
        con.commit()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = []
        for mid in need_movie_logos:
            futs.append(ex.submit(fetch_images, session_factory, limiter, auth, "movie", mid))
        for tid in need_tv_logos:
            futs.append(ex.submit(fetch_images, session_factory, limiter, auth, "tv", tid))
        for mid in need_movie_trans:
            futs.append(ex.submit(fetch_translations, session_factory, limiter, auth, "movie", mid))
        for tid in need_tv_trans:
            futs.append(ex.submit(fetch_translations, session_factory, limiter, auth, "tv", tid))

        for fut in as_completed(futs):
            res = fut.result()
            now = int(time.time())

            if len(res) == 3 and res[0] in ("movie", "tv") and isinstance(res[2], dict) and "translations" not in res[2]:
                media_type, tid, best = res
                if best:
                    s = json.dumps({k: v[0] for k, v in best.items() if isinstance(v, (list, tuple)) and v}, separators=(",", ":"))
                    if s != "{}":
                        if media_type == "movie":
                            logo_updates.append((s, tid))
                            movie_logos += 1
                        else:
                            con.execute("UPDATE series SET logos_json=? WHERE id=?", (s, tid))
                            tv_logos += 1
                if (len(logo_updates) + len(trans_rows) + len(trans_done)) >= 2000:
                    flush()
                continue

            media_type, tid, data = res
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
                    trans_rows.append((media_type, tid, iso639, iso3166, title, overview, tagline, homepage))
            trans_done.append((media_type, tid, now))
            if media_type == "movie":
                movie_trans += 1
            else:
                tv_trans += 1

            if (len(logo_updates) + len(trans_rows) + len(trans_done)) >= 2000:
                flush()

    flush()

    ep_vote_min = int(float(env.get("TMDB_EPISODES_VOTE_MIN") or "500"))
    ep_max_series = int(float(env.get("TMDB_EPISODES_MAX_SERIES") or "20"))
    series_for_eps = [
        int(r[0])
        for r in con.execute(
            f"SELECT id FROM series WHERE id IN ({','.join('?' for _ in tv_ids)}) AND COALESCE(vote_count,0) >= ? ORDER BY vote_count DESC LIMIT ?",
            [*tv_ids, ep_vote_min, ep_max_series],
        )
    ] if tv_ids else []

    seasons_to_fetch: List[Tuple[int, int]] = []
    if series_for_eps:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(fetch_tv_details, session_factory, limiter, auth, sid, lang) for sid in series_for_eps]
            for fut in as_completed(futs):
                sid, data = fut.result()
                if not (data and isinstance(data, dict)):
                    continue
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
                    tv_seasons += 1
                    if not con.execute(
                        "SELECT 1 FROM tv_season_done WHERE series_id=? AND season_number=? LIMIT 1", (sid, sn)
                    ).fetchone():
                        seasons_to_fetch.append((sid, sn))
        con.commit()

    if seasons_to_fetch:
        ep_rows: List[Tuple[int, int, int, int, str, str, str, int, str, float, int]] = []
        done_rows: List[Tuple[int, int, int]] = []

        def flush_eps():
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
                ex.submit(fetch_tv_season, session_factory, limiter, auth, sid, sn, lang): (sid, sn)
                for (sid, sn) in seasons_to_fetch
            }
            for fut in as_completed(futs):
                sid, sn = futs[fut]
                _sid, _sn, data = fut.result()
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
                                float(e.get("vote_average") or 0.0),
                                int(e.get("vote_count") or 0),
                            )
                        )
                        tv_episodes += 1
                    done_rows.append((sid, sn, now))
                if (len(ep_rows) + len(done_rows)) >= 2000:
                    flush_eps()
        flush_eps()

    con.execute(
        "INSERT INTO sync_runs(ran_at, window_days, region, movie_rows, tv_rows, movie_logos, tv_logos, movie_trans, tv_trans, tv_seasons, tv_episodes) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (int(time.time()), window_days, region, movie_rows, tv_rows, movie_logos, tv_logos, movie_trans, tv_trans, tv_seasons, tv_episodes),
    )
    con.commit()
    con.close()

    print(
        f"movies={movie_rows} series={tv_rows} movie_logos={movie_logos} tv_logos={tv_logos} movie_trans={movie_trans} tv_trans={tv_trans} tv_seasons={tv_seasons} tv_episodes={tv_episodes}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

