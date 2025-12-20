import json
import os
import sqlite3
import ssl
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Lock, local
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from .lang import _lang_tag
from .util import _load_dotenv, _pick_logo, _year


class _TokenBucket:
    def __init__(self, rate: float, capacity: float | None = None):
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        self.tokens = self.capacity
        self.t = time.time()
        self.lock = Lock()

    def acquire(self, n: float = 1.0):
        if self.rate <= 0:
            return
        while True:
            now = time.time()
            with self.lock:
                dt = now - self.t
                if dt > 0:
                    self.tokens = min(self.capacity, self.tokens + dt * self.rate)
                    self.t = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                need = (n - self.tokens) / self.rate
            if need > 0:
                time.sleep(need)


class App:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.page_size = 48
        self.similar_cache: dict[tuple[str, int, str], tuple[float, list]] = {}
        self.similar_lock = Lock()
        self.similar_ttl_s = 3 * 24 * 3600
        disable_dotenv = (os.environ.get("DISABLE_DOTENV") or "").strip().lower() in ("1", "true", "yes")
        file_env = {} if disable_dotenv else _load_dotenv(os.path.join(os.getcwd(), ".env"))

        env_key = os.environ.get("TMDB_API_KEY") or ""
        file_key = (file_env.get("TMDB_API_KEY") or "").strip()
        self.tmdb_key = env_key.strip() or file_key

        env_proxy = (os.environ.get("TMDB_PROXY") or "").strip()
        file_proxy = (file_env.get("TMDB_PROXY") or "").strip()
        self.tmdb_proxy = env_proxy or file_proxy
        self._tmdb_opener = None
        self.home_cache: dict[str, tuple] = {}
        self.home_lock = Lock()
        self.home_ttl_s = 90 * 60
        self.trending_cache: dict[tuple[str, str], tuple[float, list]] = {}
        self.trending_lock = Lock()
        self.trending_ttl_s = 90 * 60
        self.logo_cache: dict[tuple[str, int, str], tuple[float, str | None]] = {}
        self.logo_lock = Lock()
        self.logo_ttl_s = 3 * 24 * 3600
        self.backfill_recent: dict[tuple[str, int, str, int], float] = {}
        self.backfill_lock = Lock()
        self.backfill_ttl_s = 10 * 60
        self.tmdb_rps = float(os.environ.get("TMDB_RPS") or "47")
        fg_default = 7.0
        fg_cfg = float(os.environ.get("TMDB_RPS_FOREGROUND") or fg_default)
        fg = min(fg_cfg, self.tmdb_rps - 1) if self.tmdb_rps > 1 else self.tmdb_rps
        bg = max(0.0, self.tmdb_rps - fg)
        self.tmdb_fg_limiter = _TokenBucket(fg, capacity=max(1.0, fg))
        self.tmdb_bg_limiter = _TokenBucket(bg, capacity=max(1.0, bg)) if bg > 0 else None
        self.tmdb_tls = local()
        self.backfill_workers = int(os.environ.get("BACKFILL_WORKERS") or "8")
        self.backfill_ex = ThreadPoolExecutor(max_workers=max(1, self.backfill_workers))
        self.backfill_inflight: set[tuple[str, int, str, int]] = set()
        self.backfill_queue_limit = int(os.environ.get("BACKFILL_QUEUE_LIMIT") or "2000")
        self.tmdb_init_lock = Lock()

        con = sqlite3.connect(self.db_path)
        try:
            con.row_factory = sqlite3.Row
            self.movies_cols = {r["name"] for r in con.execute("PRAGMA table_info(movies)")}
            self.series_cols = {r["name"] for r in con.execute("PRAGMA table_info(series)")}
            self.has_genres = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='genres'").fetchone() is not None
            )
            self.has_title_genres = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='title_genres'").fetchone()
                is not None
            )
            self.has_translations = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='title_translations'").fetchone()
                is not None
            )
            self.has_videos = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='title_videos'").fetchone() is not None
            )
            self.has_cast = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='title_cast'").fetchone() is not None
            self.has_seasons = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tv_seasons'").fetchone() is not None
            )
            self.has_episodes = (
                con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tv_episodes'").fetchone() is not None
            )
        finally:
            con.close()

    def _tmdb_open(self, url: str, timeout_s: float):
        if not self.tmdb_key:
            return None
        if getattr(self.tmdb_tls, "bg", False) and self.tmdb_bg_limiter:
            self.tmdb_bg_limiter.acquire(1)
        else:
            self.tmdb_fg_limiter.acquire(1)
        if self._tmdb_opener is None:
            with self.tmdb_init_lock:
                if self._tmdb_opener is None:
                    ctx = ssl.create_default_context()
                    https = HTTPSHandler(context=ctx)
                    if self.tmdb_proxy:
                        proxy = ProxyHandler({"http": self.tmdb_proxy, "https": self.tmdb_proxy})
                        self._tmdb_opener = build_opener(proxy, https)
                    else:
                        self._tmdb_opener = build_opener(https)

        req = Request(url, headers={"accept": "application/json"})
        return self._tmdb_opener.open(req, timeout=timeout_s)

    def _tmdb_get_json(self, url: str, timeout_s: float):
        try:
            resp = self._tmdb_open(url, timeout_s)
            if not resp:
                return (None, None)
            with resp:
                st = getattr(resp, "status", None)
                if st != 200:
                    return (st, None)
                data = json.loads(resp.read().decode("utf-8"))
                return (st, data if isinstance(data, dict) else None)
        except Exception:
            return (None, None)

    def _tmdb_fetch_title_any(self, tid: int, lang_tag: str):
        if not self.tmdb_key:
            return (None, None, None)

        def fetch(mt: str):
            st, data = self._tmdb_get_json(
                f"https://api.themoviedb.org/3/{mt}/{tid}?api_key={self.tmdb_key}&language={lang_tag}",
                8,
            )
            return (mt, st, data)

        with ThreadPoolExecutor(max_workers=2) as ex:
            f_movie = ex.submit(fetch, "movie")
            f_tv = ex.submit(fetch, "tv")
            done, pending = wait((f_movie, f_tv), return_when=FIRST_COMPLETED)
            first = next(iter(done)).result()
            if first[1] == 200 and first[2]:
                for p in pending:
                    p.cancel()
                return first
            other = next(iter(pending)).result()
            if other[1] == 200 and other[2]:
                return other
            return first if first[1] == 200 else other

    def _missing_parts(self, con: sqlite3.Connection, media_type: str, tid: int, iso639: str, iso3166: str | None, full: bool):
        if not self.tmdb_key or media_type not in ("movie", "tv") or tid <= 0:
            return None
        lang_tag = _lang_tag(iso639, iso3166)
        cols = self.movies_cols if media_type == "movie" else self.series_cols
        tbl = "movies" if media_type == "movie" else "series"
        base = con.execute(f"SELECT id{',logos_json' if 'logos_json' in cols else ''} FROM {tbl} WHERE id=? LIMIT 1", (tid,)).fetchone()
        need_base = base is None
        need_logos = False
        if "logos_json" in cols:
            need_logos = need_base or not (dict(base).get("logos_json") if base else None)

        need_tr = False
        if self.has_translations:
            if iso3166:
                need_tr = (
                    con.execute(
                        "SELECT 1 FROM title_translations WHERE media_type=? AND tmdb_id=? AND iso_639_1=? AND iso_3166_1=? LIMIT 1",
                        (media_type, tid, iso639, iso3166),
                    ).fetchone()
                    is None
                )
            else:
                need_tr = (
                    con.execute(
                        "SELECT 1 FROM title_translations WHERE media_type=? AND tmdb_id=? AND iso_639_1=? LIMIT 1",
                        (media_type, tid, iso639),
                    ).fetchone()
                    is None
                )

        need_cast = False
        need_vid = False
        need_tv = False
        if full:
            if self.has_cast:
                need_cast = con.execute("SELECT 1 FROM title_cast WHERE media_type=? AND tmdb_id=? LIMIT 1", (media_type, tid)).fetchone() is None
            if self.has_videos:
                need_vid = con.execute("SELECT 1 FROM title_videos WHERE media_type=? AND tmdb_id=? LIMIT 1", (media_type, tid)).fetchone() is None
            if media_type == "tv":
                if self.has_seasons:
                    need_tv = need_tv or (con.execute("SELECT 1 FROM tv_seasons WHERE series_id=? LIMIT 1", (tid,)).fetchone() is None)
                if self.has_episodes:
                    need_tv = need_tv or (con.execute("SELECT 1 FROM tv_episodes WHERE series_id=? LIMIT 1", (tid,)).fetchone() is None)

        if not (need_base or need_logos or need_tr or need_cast or need_vid or need_tv):
            return None
        return {
            "lang_tag": lang_tag,
            "need_base": need_base,
            "need_logos": need_logos,
            "need_translations": need_tr,
            "need_cast": need_cast,
            "need_videos": need_vid,
            "need_tv": need_tv,
        }

    def _schedule_backfill(self, media_type: str, tid: int, iso639: str, iso3166: str | None, full: bool):
        if not self.tmdb_key or media_type not in ("movie", "tv") or tid <= 0:
            return
        lang_tag = _lang_tag(iso639, iso3166)
        k = (media_type, tid, lang_tag, 1 if full else 0)
        now = time.time()
        with self.backfill_lock:
            t0 = self.backfill_recent.get(k)
            if t0 and (now - t0) < self.backfill_ttl_s:
                return
            self.backfill_recent[k] = now
            if len(self.backfill_inflight) >= self.backfill_queue_limit or k in self.backfill_inflight:
                return
            self.backfill_inflight.add(k)

        def run():
            self.tmdb_tls.bg = True
            con = self._con()
            try:
                miss = self._missing_parts(con, media_type, tid, iso639, iso3166, full)
                if not miss:
                    return
                data = None
                if miss.get("need_base") or (media_type == "tv" and miss.get("need_tv")):
                    st, data = self._tmdb_get_json(
                        f"https://api.themoviedb.org/3/{media_type}/{tid}?api_key={self.tmdb_key}&language={miss['lang_tag']}",
                        10 if full else 8,
                    )
                    if st != 200 or not data:
                        return
                    self._upsert_tmdb_base(con, media_type, tid, data)
                    if media_type == "tv" and miss.get("need_tv"):
                        self._upsert_tmdb_tv_seasons_episodes(con, tid, miss["lang_tag"], data)
                if miss.get("need_logos"):
                    self._upsert_tmdb_logos(con, media_type, tid, miss["lang_tag"])
                if miss.get("need_videos"):
                    self._upsert_tmdb_videos(con, media_type, tid, miss["lang_tag"])
                if miss.get("need_cast"):
                    self._upsert_tmdb_cast(con, media_type, tid)
                if miss.get("need_translations"):
                    self._upsert_tmdb_translations(con, media_type, tid)
                con.commit()
            finally:
                con.close()
                self.tmdb_tls.bg = False
                with self.backfill_lock:
                    self.backfill_inflight.discard(k)

        self.backfill_ex.submit(run)

    def _upsert_tmdb_base(self, con: sqlite3.Connection, media_type: str, tid: int, data: dict):
        if media_type not in ("movie", "tv"):
            return
        if media_type == "movie":
            title = data.get("title") or None
            overview = data.get("overview") or None
            vote_average = data.get("vote_average")
            vote_count = data.get("vote_count")
            release_date = data.get("release_date") or None
            popularity = data.get("popularity")
            poster_path = data.get("poster_path") or None
            backdrop_path = data.get("backdrop_path") or None
            genres = ", ".join(g.get("name") for g in (data.get("genres") or []) if isinstance(g, dict) and g.get("name")) or None

            cols = self.movies_cols
            fields = ["id"]
            vals = [tid]

            def add(col: str, v):
                if col in cols:
                    fields.append(col)
                    vals.append(v)

            add("title", title)
            add("overview", overview)
            add("vote_average", vote_average)
            add("vote_count", vote_count)
            add("release_date", release_date)
            add("popularity", popularity)
            add("poster_path", poster_path)
            add("backdrop_path", backdrop_path)
            add("genres", genres)

            set_cols = [c for c in fields if c != "id"]
            sql = f"""
            INSERT INTO movies({",".join(fields)}) VALUES({",".join("?" for _ in fields)})
            ON CONFLICT(id) DO UPDATE SET {",".join(f"{c}=excluded.{c}" for c in set_cols)}
            """.strip()
            con.execute(sql, tuple(vals))
            return

        name = data.get("name") or None
        overview = data.get("overview") or None
        vote_average = data.get("vote_average")
        vote_count = data.get("vote_count")
        first_air_date = data.get("first_air_date") or None
        popularity = data.get("popularity")
        poster_path = data.get("poster_path") or None
        backdrop_path = data.get("backdrop_path") or None
        genres = ", ".join(g.get("name") for g in (data.get("genres") or []) if isinstance(g, dict) and g.get("name")) or None
        networks = ", ".join(n.get("name") for n in (data.get("networks") or []) if isinstance(n, dict) and n.get("name")) or None
        number_of_seasons = data.get("number_of_seasons")
        number_of_episodes = data.get("number_of_episodes")

        cols = self.series_cols
        fields = ["id"]
        vals = [tid]

        def add(col: str, v):
            if col in cols:
                fields.append(col)
                vals.append(v)

        add("name", name)
        add("overview", overview)
        add("vote_average", vote_average)
        add("vote_count", vote_count)
        add("first_air_date", first_air_date)
        add("popularity", popularity)
        add("poster_path", poster_path)
        add("backdrop_path", backdrop_path)
        add("genres", genres)
        add("networks", networks)
        add("number_of_seasons", number_of_seasons)
        add("number_of_episodes", number_of_episodes)

        set_cols = [c for c in fields if c != "id"]
        sql = f"""
        INSERT INTO series({",".join(fields)}) VALUES({",".join("?" for _ in fields)})
        ON CONFLICT(id) DO UPDATE SET {",".join(f"{c}=excluded.{c}" for c in set_cols)}
        """.strip()
        con.execute(sql, tuple(vals))

    def _upsert_tmdb_logos(self, con: sqlite3.Connection, media_type: str, tid: int, lang_tag: str):
        if "logos_json" not in (self.movies_cols if media_type == "movie" else self.series_cols):
            return

        def harvest(idata: dict):
            logos = idata.get("logos") or []
            if not isinstance(logos, list):
                return None
            by_lang = {}
            for it in logos:
                if not isinstance(it, dict):
                    continue
                fp = it.get("file_path")
                if not fp:
                    continue
                loc = (it.get("iso_639_1") or "und").strip() or "und"
                if loc not in by_lang:
                    by_lang[loc] = fp
            return by_lang or None

        st_i, idata = self._tmdb_get_json(
            f"https://api.themoviedb.org/3/{media_type}/{tid}/images?api_key={self.tmdb_key}&include_image_language={lang_tag},en,null",
            10,
        )
        by_lang = harvest(idata) if (st_i == 200 and idata) else None
        if by_lang is None:
            st_i, idata = self._tmdb_get_json(
                f"https://api.themoviedb.org/3/{media_type}/{tid}/images?api_key={self.tmdb_key}",
                10,
            )
            by_lang = harvest(idata) if (st_i == 200 and idata) else None
        if not by_lang:
            return
        j = json.dumps(by_lang, ensure_ascii=False, separators=(",", ":"))
        if media_type == "movie":
            con.execute("UPDATE movies SET logos_json=? WHERE id=?", (j, tid))
        else:
            con.execute("UPDATE series SET logos_json=? WHERE id=?", (j, tid))

    def _upsert_tmdb_videos(self, con: sqlite3.Connection, media_type: str, tid: int, lang_tag: str):
        if not self.has_videos:
            return
        st_v, vdata = self._tmdb_get_json(
            f"https://api.themoviedb.org/3/{media_type}/{tid}/videos?api_key={self.tmdb_key}&language={lang_tag}",
            10,
        )
        if st_v != 200 or not vdata:
            return
        for it in vdata.get("results") or []:
            if not isinstance(it, dict):
                continue
            key = it.get("key") or None
            site = it.get("site") or None
            if not key:
                continue
            con.execute(
                """
                INSERT INTO title_videos(media_type,tmdb_id,video_id,key,site,name,type,official,published_at,iso_639_1,iso_3166_1,size)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(media_type,tmdb_id) DO UPDATE SET
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
                (
                    media_type,
                    tid,
                    it.get("id") or None,
                    key,
                    site,
                    it.get("name") or None,
                    it.get("type") or None,
                    1 if it.get("official") else 0,
                    it.get("published_at") or None,
                    it.get("iso_639_1") or None,
                    it.get("iso_3166_1") or None,
                    it.get("size") if isinstance(it.get("size"), int) else None,
                ),
            )
            break

    def _upsert_tmdb_cast(self, con: sqlite3.Connection, media_type: str, tid: int):
        if not self.has_cast:
            return
        st_c, cdata = self._tmdb_get_json(
            f"https://api.themoviedb.org/3/{media_type}/{tid}/credits?api_key={self.tmdb_key}",
            10,
        )
        if st_c != 200 or not cdata:
            return
        con.execute("DELETE FROM title_cast WHERE media_type=? AND tmdb_id=?", (media_type, tid))
        for it in (cdata.get("cast") or [])[:24]:
            if not isinstance(it, dict):
                continue
            pid = it.get("id")
            cid = it.get("credit_id")
            if pid is None or cid is None:
                continue
            con.execute(
                """
                INSERT INTO title_cast(media_type,tmdb_id,person_id,credit_id,cast_id,name,character,ord,profile_path)
                VALUES(?,?,?,?,?,?,?,?,?)
                """.strip(),
                (
                    media_type,
                    tid,
                    pid,
                    cid,
                    it.get("cast_id"),
                    it.get("name") or None,
                    it.get("character") or None,
                    it.get("order"),
                    it.get("profile_path"),
                ),
            )

    def _upsert_tmdb_translations(self, con: sqlite3.Connection, media_type: str, tid: int):
        if not self.has_translations:
            return
        st_t, tdata = self._tmdb_get_json(
            f"https://api.themoviedb.org/3/{media_type}/{tid}/translations?api_key={self.tmdb_key}",
            12,
        )
        if st_t != 200 or not tdata:
            return
        for it in tdata.get("translations") or []:
            if not isinstance(it, dict):
                continue
            iso_639_1 = (it.get("iso_639_1") or "").strip().lower()
            iso_3166_1 = (it.get("iso_3166_1") or "").strip().upper()
            if not iso_639_1 or not iso_3166_1:
                continue
            td = it.get("data") or {}
            if not isinstance(td, dict):
                continue
            title = td.get("title") or td.get("name") or None
            overview = td.get("overview") or None
            tagline = td.get("tagline") or None
            homepage = td.get("homepage") or None
            con.execute(
                """
                INSERT INTO title_translations(media_type,tmdb_id,iso_639_1,iso_3166_1,title,overview,tagline,homepage)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(media_type,tmdb_id,iso_639_1,iso_3166_1) DO UPDATE SET
                  title=excluded.title,
                  overview=excluded.overview,
                  tagline=excluded.tagline,
                  homepage=excluded.homepage
                """.strip(),
                (media_type, tid, iso_639_1, iso_3166_1, title, overview, tagline, homepage),
            )

    def _upsert_tmdb_tv_seasons_episodes(self, con: sqlite3.Connection, tid: int, lang_tag: str, data: dict):
        if self.has_seasons:
            seasons = data.get("seasons") or []
            if isinstance(seasons, list):
                for s in seasons:
                    if not isinstance(s, dict):
                        continue
                    sn = int(s.get("season_number") or 0)
                    if sn <= 0:
                        continue
                    con.execute(
                        """
                        INSERT INTO tv_seasons(series_id,season_number,season_id,name,overview,air_date,poster_path,episode_count)
                        VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(series_id,season_number) DO UPDATE SET
                          season_id=excluded.season_id,
                          name=excluded.name,
                          overview=excluded.overview,
                          air_date=excluded.air_date,
                          poster_path=excluded.poster_path,
                          episode_count=excluded.episode_count
                        """.strip(),
                        (
                            tid,
                            sn,
                            s.get("id"),
                            s.get("name") or None,
                            s.get("overview") or None,
                            s.get("air_date") or None,
                            s.get("poster_path") or None,
                            s.get("episode_count"),
                        ),
                    )

        if not self.has_episodes:
            return
        sn = None
        for s in data.get("seasons") or []:
            if not isinstance(s, dict):
                continue
            x = int(s.get("season_number") or 0)
            if x > 0:
                sn = x
                break
        if sn is None:
            return
        st_s, sdata = self._tmdb_get_json(
            f"https://api.themoviedb.org/3/tv/{tid}/season/{sn}?api_key={self.tmdb_key}&language={lang_tag}",
            12,
        )
        if st_s != 200 or not sdata:
            return
        for ep in sdata.get("episodes") or []:
            if not isinstance(ep, dict):
                continue
            en = int(ep.get("episode_number") or 0)
            if en <= 0:
                continue
            con.execute(
                """
                INSERT INTO tv_episodes(series_id,season_number,episode_number,episode_id,name,overview,air_date,runtime,still_path,vote_average,vote_count)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(series_id,season_number,episode_number) DO UPDATE SET
                  episode_id=excluded.episode_id,
                  name=excluded.name,
                  overview=excluded.overview,
                  air_date=excluded.air_date,
                  runtime=excluded.runtime,
                  still_path=excluded.still_path,
                  vote_average=excluded.vote_average,
                  vote_count=excluded.vote_count
                """.strip(),
                (
                    tid,
                    sn,
                    en,
                    ep.get("id"),
                    ep.get("name") or None,
                    ep.get("overview") or None,
                    ep.get("air_date") or None,
                    ep.get("runtime"),
                    ep.get("still_path") or None,
                    ep.get("vote_average"),
                    ep.get("vote_count"),
                ),
            )

    def _upsert_tmdb_title(self, media_type: str, tid: int, lang_tag: str, data: dict):
        if media_type not in ("movie", "tv"):
            return
        con = self._con()
        try:
            self._upsert_tmdb_base(con, media_type, tid, data)
            self._upsert_tmdb_logos(con, media_type, tid, lang_tag)
            self._upsert_tmdb_videos(con, media_type, tid, lang_tag)
            self._upsert_tmdb_cast(con, media_type, tid)
            self._upsert_tmdb_translations(con, media_type, tid)
            if media_type == "tv":
                self._upsert_tmdb_tv_seasons_episodes(con, tid, lang_tag, data)
            con.commit()
        finally:
            con.close()

    def _con(self):
        con = sqlite3.connect(self.db_path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def _translated(self, con: sqlite3.Connection, media_type: str, tid: int, iso639: str, iso3166: str | None):
        if not self.has_translations:
            return (None, None)
        if iso3166:
            r = con.execute(
                """
                SELECT title, overview
                FROM title_translations
                WHERE media_type=? AND tmdb_id=? AND iso_639_1=? AND iso_3166_1=?
                LIMIT 1
                """.strip(),
                (media_type, tid, iso639, iso3166),
            ).fetchone()
            if r:
                return (r["title"], r["overview"])
        r = con.execute(
            """
            SELECT title, overview
            FROM title_translations
            WHERE media_type=? AND tmdb_id=? AND iso_639_1=?
            LIMIT 1
            """.strip(),
            (media_type, tid, iso639),
        ).fetchone()
        if r:
            return (r["title"], r["overview"])
        return (None, None)

    def _card_from_row(self, media_type: str, r: sqlite3.Row, iso639: str, iso3166: str | None, with_description: bool = False):
        if media_type == "movie":
            tid = int(r["id"])
            t_title, _t_over = self._translated(r["_con"], "movie", tid, iso639, iso3166) if "_con" in r.keys() else (None, None)
            title = (t_title or r["title"] or "").strip()
            description = None
            if with_description:
                s = (_t_over or r.get("overview") or "").strip()
                description = (s[:240] + "…") if len(s) > 240 else (s or None)
            poster = r.get("poster_path")
            backdrop = r.get("backdrop_path")
            logo = _pick_logo(r.get("logos_json"), iso639) or poster
            return {
                "id": tid,
                "kind": "movie",
                "name": title,
                "description": description,
                "year": _year(r.get("release_date")),
                "rating": r.get("vote_average"),
                "poster": poster,
                "logo": logo,
                "backdrop": backdrop,
            }

        tid = int(r["id"])
        t_title, _t_over = self._translated(r["_con"], "tv", tid, iso639, iso3166) if "_con" in r.keys() else (None, None)
        title = (t_title or r["name"] or "").strip()
        description = None
        if with_description:
            s = (_t_over or r.get("overview") or "").strip()
            description = (s[:240] + "…") if len(s) > 240 else (s or None)
        poster = r.get("poster_path")
        backdrop = r.get("backdrop_path")
        logo = _pick_logo(r.get("logos_json"), iso639) or poster
        return {
            "id": tid,
            "kind": "series",
            "name": title,
            "description": description,
            "year": _year(r.get("first_air_date")),
            "rating": r.get("vote_average"),
            "poster": poster,
            "logo": logo,
            "backdrop": backdrop,
        }

    def _tmdb_logo(self, media_type: str, tid: int, iso639: str):
        if not self.tmdb_key:
            return None
        k = (media_type, tid, iso639)
        now = time.time()
        with self.logo_lock:
            cur = self.logo_cache.get(k)
            if cur and (now - cur[0]) < self.logo_ttl_s:
                return cur[1]

        def pick_best(data: dict):
            best = None
            best_ar = None
            logos = data.get("logos") or []
            if not isinstance(logos, list):
                return None
            for it in logos:
                if not isinstance(it, dict):
                    continue
                fp = it.get("file_path")
                if not fp:
                    continue
                loc = (it.get("iso_639_1") or "und").strip() or "und"
                ar = it.get("aspect_ratio")
                try:
                    ar = float(ar) if ar is not None else None
                except Exception:
                    ar = None
                score = 2
                if loc == iso639:
                    score = 0
                elif loc == "en":
                    score = 1
                if best is None or score < best[0] or (score == best[0] and ar is not None and (best_ar is None or ar < best_ar)):
                    best = (score, fp)
                    best_ar = ar
            return best[1] if best else None

        url = f"https://api.themoviedb.org/3/{media_type}/{tid}/images?api_key={self.tmdb_key}&include_image_language={iso639},en,null"
        url_all = f"https://api.themoviedb.org/3/{media_type}/{tid}/images?api_key={self.tmdb_key}"
        best = None
        try:
            resp = self._tmdb_open(url, 8)
            if resp:
                with resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        best = pick_best(data)
            if best is None:
                resp = self._tmdb_open(url_all, 8)
                if resp:
                    with resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode("utf-8"))
                            best = pick_best(data)
        except Exception:
            best = None

        val = best
        with self.logo_lock:
            self.logo_cache[k] = (now, val)
        return val

    def _enrich_card(self, con: sqlite3.Connection, card: dict, iso639: str, iso3166: str | None):
        tid = int(card.get("id") or 0)
        kind = card.get("kind")
        if tid <= 0 or kind not in ("movie", "series"):
            return card
        media_type = "movie" if kind == "movie" else "tv"
        if self._missing_parts(con, media_type, tid, iso639, iso3166, full=False):
            self._schedule_backfill(media_type, tid, iso639, iso3166, full=False)

        if kind == "movie":
            r = con.execute("SELECT title, overview, logos_json, backdrop_path FROM movies WHERE id=? LIMIT 1", (tid,)).fetchone()
            if r:
                r = dict(r)
                t_title, t_over = self._translated(con, "movie", tid, iso639, iso3166)
                nm = (t_title or r.get("title") or "").strip()
                if nm:
                    card["name"] = nm
                if not (card.get("description") or "").strip():
                    s = (t_over or r.get("overview") or "").strip()
                    card["description"] = (s[:240] + "…") if len(s) > 240 else (s or None)
                card["logo"] = _pick_logo(r.get("logos_json"), iso639) or card.get("logo")
                if not card.get("backdrop"):
                    card["backdrop"] = r.get("backdrop_path")
        else:
            r = con.execute("SELECT name, overview, logos_json, backdrop_path FROM series WHERE id=? LIMIT 1", (tid,)).fetchone()
            if r:
                r = dict(r)
                t_title, t_over = self._translated(con, "tv", tid, iso639, iso3166)
                nm = (t_title or r.get("name") or "").strip()
                if nm:
                    card["name"] = nm
                if not (card.get("description") or "").strip():
                    s = (t_over or r.get("overview") or "").strip()
                    card["description"] = (s[:240] + "…") if len(s) > 240 else (s or None)
                card["logo"] = _pick_logo(r.get("logos_json"), iso639) or card.get("logo")
                if not card.get("backdrop"):
                    card["backdrop"] = r.get("backdrop_path")

        poster = card.get("poster")
        if not card.get("logo"):
            card["logo"] = poster
        if "backdrop" not in card:
            card["backdrop"] = None
        return card

    def _tmdb_similar(self, kind: str, tid: int, iso639: str, iso3166: str | None):
        if not self.tmdb_key:
            return []
        lang_tag = _lang_tag(iso639, iso3166)
        k = (kind, tid, lang_tag)
        now = time.time()
        with self.similar_lock:
            cur = self.similar_cache.get(k)
            if cur and (now - cur[0]) < self.similar_ttl_s:
                return cur[1]

        media_type = "movie" if kind == "movie" else "tv"
        url = f"https://api.themoviedb.org/3/{media_type}/{tid}/similar?api_key={self.tmdb_key}&language={iso639}"
        out = []
        try:
            resp = self._tmdb_open(url, 6)
            if not resp:
                return []
            with resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    res = data.get("results") or []
                    if isinstance(res, list):
                        for it in res[:24]:
                            if not isinstance(it, dict):
                                continue
                            i = int(it.get("id") or 0)
                            if i <= 0:
                                continue
                            out.append(
                                {
                                    "id": i,
                                    "kind": "movie" if media_type == "movie" else "series",
                                    "name": (it.get("title") or it.get("name") or "").strip(),
                                    "year": _year(it.get("release_date") or it.get("first_air_date")),
                                    "rating": it.get("vote_average"),
                                    "poster": it.get("poster_path"),
                                    "logo": it.get("poster_path"),
                                    "backdrop": it.get("backdrop_path"),
                                }
                            )
        except Exception:
            out = []

        if out:
            con = self._con()
            try:
                out = [self._enrich_card(con, c, iso639, iso3166) for c in out]
            finally:
                con.close()

        with self.similar_lock:
            self.similar_cache[k] = (now, out)
        return out

    def _tmdb_trending(self, window: str, lang: str):
        if not self.tmdb_key:
            return []
        if window not in ("day", "week"):
            return []
        k = (window, lang)
        now = time.time()
        with self.trending_lock:
            cur = self.trending_cache.get(k)
            if cur and (now - cur[0]) < self.trending_ttl_s:
                return cur[1]

        url = f"https://api.themoviedb.org/3/trending/all/{window}?api_key={self.tmdb_key}&language={lang}"
        out = []
        try:
            resp = self._tmdb_open(url, 6)
            if not resp:
                return []
            with resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    res = data.get("results") or []
                    if isinstance(res, list):
                        out = [it for it in res if isinstance(it, dict)]
        except Exception:
            out = []

        with self.trending_lock:
            self.trending_cache[k] = (now, out)
        return out
