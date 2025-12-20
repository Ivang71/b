import json
import gzip
import os
import random
import socket
import sqlite3
import ssl
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from urllib.parse import parse_qs, unquote, urlparse

from .app import App
from .constants import BROWSE_TABS, HOME_GENRES, PROVIDERS, PROVIDER_NEEDLES
from .lang import _lang_tag, _pick_lang
from .util import _pick_logo, _year

try:
    import brotli as _brotli
except Exception:
    _brotli = None


def _br_compress(raw: bytes) -> bytes | None:
    if not _brotli:
        return None
    q = int(os.environ.get("BROTLI_QUALITY") or "5")
    q = 0 if q < 0 else (11 if q > 11 else q)
    try:
        return _brotli.compress(raw, quality=q)
    except Exception:
        return None


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    _rl_lock = Lock()
    _rl: dict[str, tuple[float, float]] = {}

    def log_message(self, *_args):
        return

    def setup(self):
        super().setup()
        try:
            self.connection.settimeout(float(os.environ.get("CONN_TIMEOUT_S") or "15"))
        except Exception:
            pass

    def _cors_origin(self) -> str | None:
        origin = (self.headers.get("Origin") or "").strip()
        if not origin:
            return None
        u = urlparse(origin)
        if u.scheme not in ("http", "https") or not u.netloc:
            return None
        host = (u.hostname or "").strip().lower()
        if not host:
            return None
        allow_local = (os.environ.get("CORS_ALLOW_LOCALHOST") or "").strip().lower() in ("1", "true", "yes")
        if allow_local and host in ("localhost", "127.0.0.1", "::1"):
            return origin
        raw = os.environ.get("CORS_ALLOW_HOSTS") or ""
        allow = {h.strip().lower() for h in raw.replace(" ", ",").split(",") if h.strip()}
        if host in allow and u.scheme == "https":
            return origin
        return None

    def _cors_send(self):
        origin = self._cors_origin()
        if not origin:
            return
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Vary", "Origin")

    def _sec_send(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        if getattr(self.server, "is_tls", False):
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

    def _client_ip(self) -> str:
        ip = (self.headers.get("CF-Connecting-IP") or "").strip()
        if ip:
            return ip
        xff = (self.headers.get("X-Forwarded-For") or "").strip()
        if xff:
            return xff.split(",", 1)[0].strip()
        return (self.client_address[0] or "").strip()

    def _rate_allow(self) -> bool:
        raw = (os.environ.get("RATE_LIMIT_RPS") or "").strip()
        rps = float(raw) if raw else 3.0
        raw = (os.environ.get("RATE_LIMIT_BURST") or "").strip()
        burst = float(raw) if raw else 120.0
        if rps <= 0 or burst <= 0:
            return True
        ip = self._client_ip()
        now = time.monotonic()
        with self._rl_lock:
            tokens, last = self._rl.get(ip, (burst, now))
            tokens = min(burst, tokens + (now - last) * rps)
            if tokens < 1.0:
                self._rl[ip] = (tokens, now)
                if len(self._rl) > 20000:
                    self._rl.clear()
                return False
            self._rl[ip] = (tokens - 1.0, now)
            if len(self._rl) > 20000:
                self._rl.clear()
            return True

    def _send(self, code: int, body: bytes, content_type: str = "text/plain; charset=utf-8"):
        self.send_response(code)
        self._cors_send()
        self._sec_send()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        try:
            self.wfile.flush()
        except Exception:
            pass
        self.close_connection = True

    def _send_json(self, code: int, obj):
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ae = (self.headers.get("Accept-Encoding") or "").lower()
        force_gzip = (os.environ.get("FORCE_GZIP") or "").strip().lower() in ("1", "true", "yes")
        via_proxy = bool((self.headers.get("CF-Connecting-IP") or "").strip() or (self.headers.get("X-Forwarded-For") or "").strip())
        if force_gzip or via_proxy or "gzip" in ae:
            body = gzip.compress(raw, compresslevel=5)
            self.send_response(code)
            self._cors_send()
            self._sec_send()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            try:
                self.wfile.flush()
            except Exception:
                pass
            self.close_connection = True
            return
        self._send(code, raw, "application/json; charset=utf-8")

    def _send_json_bytes(self, code: int, body: bytes, encoding: str | None):
        self.send_response(code)
        self._cors_send()
        self._sec_send()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if encoding:
            self.send_header("Content-Encoding", encoding)
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        try:
            self.wfile.flush()
        except Exception:
            pass
        self.close_connection = True

    def do_OPTIONS(self):
        if not self._rate_allow():
            self.send_response(429)
            self._cors_send()
            self._sec_send()
            self.send_header("Retry-After", "1")
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return
        origin = self._cors_origin()
        if origin:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            req_hdrs = (self.headers.get("Access-Control-Request-Headers") or "").strip()
            if req_hdrs:
                self.send_header("Access-Control-Allow-Headers", req_hdrs)
            self.send_header("Access-Control-Max-Age", "600")
            self._sec_send()
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return
        self.send_response(204)
        self._sec_send()
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def do_GET(self):
        if not self._rate_allow():
            self._send(429, b"rate limited\n")
            return
        wt_raw = (os.environ.get("WRITE_TIMEOUT_S") or "").strip()
        try:
            self.connection.settimeout(float(wt_raw) if wt_raw else None)
        except Exception:
            pass
        u = urlparse(self.path)
        path = u.path
        qs = parse_qs(u.query or "")
        iso639, iso3166 = _pick_lang(qs, self.headers.get("Accept-Language"))

        if path in ("/ping", "/health"):
            self._send(200, b"ok\n")
            return

        if path == "/v1/home":
            body, enc = self.server.app_home_bytes(iso639, iso3166, self.headers.get("Accept-Encoding"))
            self._send_json_bytes(200, body, enc)
            return

        if path.startswith("/v1/titles/"):
            tid_s = path.split("/v1/titles/", 1)[1].strip("/")
            try:
                tid = int(tid_s)
            except Exception:
                self._send(404, b"not found\n")
                return
            out = self.server.app_title(tid, iso639, iso3166)
            if out is None:
                self._send(404, b"not found\n")
                return
            self._send_json(200, out)
            return

        if path.startswith("/v1/browse/"):
            rest = path.split("/v1/browse/", 1)[1]
            parts = [p for p in rest.split("/") if p]
            if len(parts) != 2:
                self._send(404, b"not found\n")
                return
            tab = parts[0]
            try:
                page = int(parts[1])
            except Exception:
                self._send(404, b"not found\n")
                return
            out = self.server.app_browse(tab, page, iso639, iso3166)
            if out is None:
                self._send(404, b"not found\n")
                return
            self._send_json(200, out)
            return

        if path == "/v1/search":
            self._send_json(200, self.server.app_search_page(iso639, iso3166))
            return

        if path.startswith("/v1/search/"):
            rest = path.split("/v1/search/", 1)[1]
            parts = [p for p in rest.split("/") if p]
            if len(parts) not in (1, 2):
                self._send(404, b"not found\n")
                return
            q = unquote(parts[0])
            out = self.server.app_search(q, iso639, iso3166)
            self._send_json(200, out)
            return

        self._send(404, b"not found\n")


class DualStackHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6

    def server_bind(self):
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except OSError:
            pass
        super().server_bind()


class APIServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, app: App):
        super().__init__(server_address, RequestHandlerClass)
        self.app = app

    def _genre_needles(self, s: str):
        s = (s or "").strip()
        if not s:
            return ()
        if s == "Science Fiction":
            return ("Science Fiction", "Sci-Fi & Fantasy", "Sci-Fi")
        return (s,)

    def app_home(self, iso639: str, iso3166: str | None):
        lang_tag = _lang_tag(iso639, iso3166)
        now = time.time()
        with self.app.home_lock:
            cur = self.app.home_cache.get(lang_tag)
            if cur and (now - cur[0]) < self.app.home_ttl_s:
                return cur[1]

        con = self.app._con()
        try:
            con.row_factory = sqlite3.Row

            def movie_cards(sql: str, params=()):
                rows = con.execute(sql, params).fetchall()
                out = []
                for r in rows:
                    rr = dict(r)
                    rr["_con"] = con
                    out.append(self.app._card_from_row("movie", rr, iso639, iso3166, with_description=False))
                return out

            def series_cards(sql: str, params=()):
                rows = con.execute(sql, params).fetchall()
                out = []
                for r in rows:
                    rr = dict(r)
                    rr["_con"] = con
                    out.append(self.app._card_from_row("tv", rr, iso639, iso3166, with_description=False))
                return out

            def movie_cards_with_desc(sql: str, params=()):
                rows = con.execute(sql, params).fetchall()
                out = []
                for r in rows:
                    rr = dict(r)
                    rr["_con"] = con
                    out.append(self.app._card_from_row("movie", rr, iso639, iso3166, with_description=True))
                return out

            def series_cards_with_desc(sql: str, params=()):
                rows = con.execute(sql, params).fetchall()
                out = []
                for r in rows:
                    rr = dict(r)
                    rr["_con"] = con
                    out.append(self.app._card_from_row("tv", rr, iso639, iso3166, with_description=True))
                return out

            def tmdb_card(it: dict):
                mt = (it.get("media_type") or "").strip().lower()
                if mt == "movie":
                    tid = int(it.get("id") or 0)
                    d = (it.get("overview") or "").strip()
                    return {
                        "id": tid,
                        "kind": "movie",
                        "name": (it.get("title") or "").strip(),
                        "description": (d[:240] + "…") if len(d) > 240 else (d or None),
                        "year": _year(it.get("release_date")),
                        "rating": it.get("vote_average"),
                        "poster": it.get("poster_path"),
                        "logo": it.get("poster_path"),
                        "backdrop": it.get("backdrop_path"),
                    }
                if mt == "tv":
                    tid = int(it.get("id") or 0)
                    d = (it.get("overview") or "").strip()
                    return {
                        "id": tid,
                        "kind": "series",
                        "name": (it.get("name") or "").strip(),
                        "description": (d[:240] + "…") if len(d) > 240 else (d or None),
                        "year": _year(it.get("first_air_date")),
                        "rating": it.get("vote_average"),
                        "poster": it.get("poster_path"),
                        "logo": it.get("poster_path"),
                        "backdrop": it.get("backdrop_path"),
                    }
                return None

            slider = []
            top10_today = []
            trending_today = []
            if self.app.tmdb_key:
                day = []
                for it in self.app._tmdb_trending("day", lang_tag):
                    mt = (it.get("media_type") or "").lower()
                    if mt not in ("movie", "tv"):
                        continue
                    try:
                        if int(it.get("id") or 0) <= 0:
                            continue
                    except Exception:
                        continue
                    day.append(it)
                if day:
                    slider_picks = random.sample(day, k=min(10, len(day)))
                    for it in slider_picks:
                        c = tmdb_card(it)
                        if c:
                            slider.append(self.app._enrich_card(con, c, iso639, iso3166))

                    picks = random.sample(day, k=min(10, len(day)))
                    for it in picks:
                        c = tmdb_card(it)
                        if c:
                            top10_today.append(self.app._enrich_card(con, c, iso639, iso3166))

                week = []
                for it in self.app._tmdb_trending("week", lang_tag):
                    mt = (it.get("media_type") or "").lower()
                    if mt not in ("movie", "tv"):
                        continue
                    try:
                        if int(it.get("id") or 0) <= 0:
                            continue
                    except Exception:
                        continue
                    week.append(it)
                if week:
                    for it in week:
                        c = tmdb_card(it)
                        if c:
                            trending_today.append(self.app._enrich_card(con, c, iso639, iso3166))

                if not top10_today:
                    top10_today = (
                        movie_cards("SELECT * FROM movies ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                        + series_cards("SELECT * FROM series ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                    )[:10]

                if not trending_today:
                    trending_today = (
                        movie_cards("SELECT * FROM movies ORDER BY COALESCE(popularity,0) DESC LIMIT 30")
                        + series_cards("SELECT * FROM series ORDER BY COALESCE(popularity,0) DESC LIMIT 30")
                    )[:30]
            else:
                slider = (
                    movie_cards_with_desc("SELECT * FROM movies ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                    + series_cards_with_desc("SELECT * FROM series ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                )[:10]
                top10_today = (
                    movie_cards("SELECT * FROM movies ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                    + series_cards("SELECT * FROM series ORDER BY COALESCE(popularity,0) DESC LIMIT 10")
                )
                top10_today = top10_today[:10]
                trending_today = (
                    movie_cards("SELECT * FROM movies ORDER BY COALESCE(popularity,0) DESC LIMIT 30")
                    + series_cards("SELECT * FROM series ORDER BY COALESCE(popularity,0) DESC LIMIT 30")
                )
                trending_today = trending_today[:30]

            series_on = {}
            if "networks" in self.app.series_cols:
                for p in PROVIDERS:
                    needles = PROVIDER_NEEDLES.get(p) or (p,)
                    where = " OR ".join("COALESCE(networks,'') LIKE ?" for _ in needles)
                    params = tuple(f"%{n}%" for n in needles)
                    series_on[p] = series_cards(
                        f"SELECT * FROM series WHERE {where} ORDER BY COALESCE(popularity,0) DESC LIMIT 18",
                        params,
                    )
            else:
                for p in PROVIDERS:
                    series_on[p] = []

            top_rated = {
                "movies": movie_cards(
                    """
                    SELECT * FROM (
                      SELECT * FROM movies ORDER BY COALESCE(vote_average,0) DESC LIMIT 48
                    ) ORDER BY COALESCE(vote_count,0) DESC LIMIT 12
                    """.strip()
                ),
                "series": series_cards(
                    """
                    SELECT * FROM (
                      SELECT * FROM series ORDER BY COALESCE(vote_average,0) DESC LIMIT 48
                    ) ORDER BY COALESCE(vote_count,0) DESC LIMIT 12
                    """.strip()
                ),
            }

            genres = {}
            if self.app.has_genres and self.app.has_title_genres:
                for k, needles in HOME_GENRES.items():
                    names = tuple(needles) if isinstance(needles, (tuple, list)) else (str(needles),)
                    if not names:
                        genres[k] = []
                        continue
                    ph = ",".join("?" for _ in names)
                    sql = f"""
                    SELECT DISTINCT id,kind,name,dt,rating,pop,poster,backdrop,logos FROM (
                      SELECT m.id id,'movie' kind,m.title name,m.release_date dt,m.vote_average rating,COALESCE(m.popularity,0) pop,m.poster_path poster,m.backdrop_path backdrop,m.logos_json logos
                      FROM movies m
                      JOIN title_genres tg ON tg.media_type='movie' AND tg.tmdb_id=m.id
                      JOIN genres g ON g.media_type='movie' AND g.genre_id=tg.genre_id
                      WHERE g.name IN ({ph})
                      UNION ALL
                      SELECT s.id id,'series' kind,s.name name,s.first_air_date dt,s.vote_average rating,COALESCE(s.popularity,0) pop,s.poster_path poster,s.backdrop_path backdrop,s.logos_json logos
                      FROM series s
                      JOIN title_genres tg ON tg.media_type='tv' AND tg.tmdb_id=s.id
                      JOIN genres g ON g.media_type='tv' AND g.genre_id=tg.genre_id
                      WHERE g.name IN ({ph})
                    )
                    ORDER BY COALESCE(pop,0) DESC
                    LIMIT 18
                    """.strip()
                    rows = [dict(r) for r in con.execute(sql, (*names, *names)).fetchall()]
                    out = []
                    for r in rows:
                        kind = r["kind"]
                        tid = int(r["id"])
                        media_type = "movie" if kind == "movie" else "tv"
                        t_title, _ = self.app._translated(con, media_type, tid, iso639, iso3166)
                        out.append(
                            {
                                "id": tid,
                                "kind": kind,
                                "name": (t_title or r.get("name") or "").strip(),
                                "year": _year(r.get("dt")),
                                "rating": r.get("rating"),
                                "poster": r.get("poster"),
                                "logo": _pick_logo(r.get("logos"), iso639) or r.get("poster"),
                                "backdrop": r.get("backdrop"),
                            }
                        )
                    genres[k] = out
            elif "genres" in self.app.movies_cols or "genres" in self.app.series_cols:
                for k, needles in HOME_GENRES.items():
                    names = tuple(needles) if isinstance(needles, (tuple, list)) else (str(needles),)
                    mv_gen = "genres" if "genres" in self.app.movies_cols else "''"
                    tv_gen = "genres" if "genres" in self.app.series_cols else "''"
                    where = " OR ".join("COALESCE(gen,'') LIKE ?" for _ in names) if names else "1=0"
                    sql = f"""
                    SELECT id,kind,name,dt,rating,pop,poster,backdrop,logos FROM (
                      SELECT id,'movie' kind,title name,release_date dt,vote_average rating,COALESCE(popularity,0) pop,poster_path poster,backdrop_path backdrop,logos_json logos,{mv_gen} gen
                      FROM movies
                      UNION ALL
                      SELECT id,'series' kind,name name,first_air_date dt,vote_average rating,COALESCE(popularity,0) pop,poster_path poster,backdrop_path backdrop,logos_json logos,{tv_gen} gen
                      FROM series
                    )
                    WHERE {where}
                    ORDER BY COALESCE(pop,0) DESC
                    LIMIT 18
                    """.strip()
                    params = tuple(f"%{n}%" for n in names)
                    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
                    out = []
                    for r in rows:
                        kind = r["kind"]
                        tid = int(r["id"])
                        media_type = "movie" if kind == "movie" else "tv"
                        t_title, _ = self.app._translated(con, media_type, tid, iso639, iso3166)
                        out.append(
                            {
                                "id": tid,
                                "kind": kind,
                                "name": (t_title or r.get("name") or "").strip(),
                                "year": _year(r.get("dt")),
                                "rating": r.get("rating"),
                                "poster": r.get("poster"),
                                "logo": _pick_logo(r.get("logos"), iso639) or r.get("poster"),
                                "backdrop": r.get("backdrop"),
                            }
                        )
                    genres[k] = out
            else:
                for k in HOME_GENRES.keys():
                    genres[k] = []

            out = {
                "as_of": int(time.time()),
                "providers": list(PROVIDERS),
                "slider": slider,
                "top10_today": top10_today,
                "trending_today": trending_today,
                "series_on": series_on,
                "top_rated": top_rated,
                "genres": genres,
            }
            with self.app.home_lock:
                self.app.home_cache[lang_tag] = (time.time(), out)
            return out
        finally:
            con.close()

    def app_home_bytes(self, iso639: str, iso3166: str | None, accept_encoding: str | None):
        lang_tag = _lang_tag(iso639, iso3166)
        now = time.time()
        with self.app.home_lock:
            cur = self.app.home_cache.get(lang_tag)
            if cur and (now - cur[0]) < self.app.home_ttl_s and len(cur) >= 5:
                _ts, _obj, raw, gz, br = cur
            else:
                cur = None

        if cur is None:
            obj = self.app_home(iso639, iso3166)
            raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            gz = gzip.compress(raw, compresslevel=5)
            br = _br_compress(raw)
            with self.app.home_lock:
                cur = self.app.home_cache.get(lang_tag)
                if cur and len(cur) >= 2:
                    ts, obj0 = cur[0], cur[1]
                    self.app.home_cache[lang_tag] = (ts, obj0, raw, gz, br)
                else:
                    self.app.home_cache[lang_tag] = (time.time(), obj, raw, gz, br)

        ae = (accept_encoding or "").lower()
        if "br" in ae and br:
            return (br, "br")
        if "gzip" in ae:
            return (gz, "gzip")
        return (raw, None)

    def app_title(self, tid: int, iso639: str, iso3166: str | None):
        con = self.app._con()
        try:
            lang_tag = _lang_tag(iso639, iso3166)
            m0 = con.execute("SELECT * FROM movies WHERE id=? LIMIT 1", (tid,)).fetchone()
            if m0:
                if self.app._missing_parts(con, "movie", tid, iso639, iso3166, full=True):
                    self.app._schedule_backfill("movie", tid, iso639, iso3166, full=True)
                m = dict(m0)
                media_type = "movie"
                kind = "movie"
                base_name = m.get("title")
                base_over = m.get("overview")
                date_val = m.get("release_date")
                poster = m.get("poster_path")
                backdrop = m.get("backdrop_path")
                rating = m.get("vote_average")
                genres = m.get("genres") or ""
                logos_json = m.get("logos_json")
            else:
                s0 = con.execute("SELECT * FROM series WHERE id=? LIMIT 1", (tid,)).fetchone()
                if s0:
                    if self.app._missing_parts(con, "tv", tid, iso639, iso3166, full=True):
                        self.app._schedule_backfill("tv", tid, iso639, iso3166, full=True)
                    s = dict(s0)
                    media_type = "tv"
                    kind = "series"
                    base_name = s.get("name")
                    base_over = s.get("overview")
                    date_val = s.get("first_air_date")
                    poster = s.get("poster_path")
                    backdrop = s.get("backdrop_path")
                    rating = s.get("vote_average")
                    genres = s.get("genres") or ""
                    logos_json = s.get("logos_json")
                else:
                    mt, st, data = self.app._tmdb_fetch_title_any(tid, lang_tag)
                    if st != 200 or not data or mt not in ("movie", "tv"):
                        return None
                    self.app._upsert_tmdb_base(con, mt, tid, data)
                    con.commit()
                    if self.app._missing_parts(con, mt, tid, iso639, iso3166, full=True):
                        self.app._schedule_backfill(mt, tid, iso639, iso3166, full=True)
                    if mt == "movie":
                        m0 = con.execute("SELECT * FROM movies WHERE id=? LIMIT 1", (tid,)).fetchone()
                        m = dict(m0) if m0 else {}
                        media_type = "movie"
                        kind = "movie"
                        base_name = m.get("title") or data.get("title")
                        base_over = m.get("overview") or data.get("overview")
                        date_val = m.get("release_date") or data.get("release_date")
                        poster = m.get("poster_path") or data.get("poster_path")
                        backdrop = m.get("backdrop_path") or data.get("backdrop_path")
                        rating = m.get("vote_average") if m.get("vote_average") is not None else data.get("vote_average")
                        genres = m.get("genres") or ", ".join(
                            g.get("name") for g in (data.get("genres") or []) if isinstance(g, dict) and g.get("name")
                        )
                        logos_json = m.get("logos_json")
                    else:
                        s0 = con.execute("SELECT * FROM series WHERE id=? LIMIT 1", (tid,)).fetchone()
                        s = dict(s0) if s0 else {}
                        media_type = "tv"
                        kind = "series"
                        base_name = s.get("name") or data.get("name")
                        base_over = s.get("overview") or data.get("overview")
                        date_val = s.get("first_air_date") or data.get("first_air_date")
                        poster = s.get("poster_path") or data.get("poster_path")
                        backdrop = s.get("backdrop_path") or data.get("backdrop_path")
                        rating = s.get("vote_average") if s.get("vote_average") is not None else data.get("vote_average")
                        genres = s.get("genres") or ", ".join(
                            g.get("name") for g in (data.get("genres") or []) if isinstance(g, dict) and g.get("name")
                        )
                        logos_json = s.get("logos_json")

            t_title, t_over = self.app._translated(con, media_type, tid, iso639, iso3166)
            name = (t_title or base_name or "").strip()
            description = (t_over or base_over or "").strip()

            trailer = None
            if self.app.has_videos:
                v0 = con.execute(
                    """
                    SELECT key, site
                    FROM title_videos
                    WHERE media_type=? AND tmdb_id=?
                    LIMIT 1
                    """.strip(),
                    (media_type, tid),
                ).fetchone()
                v = dict(v0) if v0 else None
                if v and (v.get("site") or "").lower() == "youtube" and v.get("key"):
                    k = v.get("key")
                    trailer = {"key": k, "url": f"https://www.youtube.com/watch?v={k}"}

            seasons = []
            prefetch_season = None
            prefetch_episodes = []
            if kind == "series" and self.app.has_seasons:
                seasons = [
                    {
                        "season": int(r["season_number"]),
                        "episode_count": int(r["episode_count"] or 0),
                    }
                    for r in con.execute(
                        "SELECT season_number, episode_count FROM tv_seasons WHERE series_id=? ORDER BY season_number ASC",
                        (tid,),
                    )
                ]
            if kind == "series" and self.app.has_episodes:
                sn = None
                for se in seasons:
                    if int(se.get("season") or 0) > 0:
                        sn = int(se["season"])
                        break
                if sn is None:
                    r0 = con.execute(
                        "SELECT MIN(season_number) sn FROM tv_episodes WHERE series_id=? AND season_number>0",
                        (tid,),
                    ).fetchone()
                    sn = int((dict(r0).get("sn") if r0 else 0) or 0) or None
                if sn is not None:
                    rows = con.execute(
                        """
                        SELECT episode_number, name, runtime, still_path
                        FROM tv_episodes
                        WHERE series_id=? AND season_number=?
                        ORDER BY episode_number ASC
                        """.strip(),
                        (tid, sn),
                    ).fetchall()
                    prefetch_season = sn
                    for r0 in rows:
                        r = dict(r0)
                        prefetch_episodes.append(
                            {
                                "episode": int(r["episode_number"]),
                                "name": (r.get("name") or "").strip(),
                                "runtime_min": r.get("runtime"),
                                "still": r.get("still_path"),
                            }
                        )

            cast = []
            if self.app.has_cast:
                for r in con.execute(
                    """
                    SELECT name, character, ord, profile_path
                    FROM title_cast
                    WHERE media_type=? AND tmdb_id=?
                    ORDER BY COALESCE(ord,9999) ASC
                    LIMIT 24
                    """.strip(),
                    (media_type, tid),
                ):
                    r = dict(r)
                    cast.append(
                        {
                            "name": (r.get("name") or "").strip(),
                            "role": (r.get("character") or "").strip(),
                            "order": int(r.get("ord") or 0),
                            "profile": r.get("profile_path"),
                        }
                    )

            tags = [t.strip() for t in genres.split(",") if t.strip()] if genres else []

            return {
                "id": tid,
                "kind": kind,
                "name": name,
                "description": description,
                "tags": tags,
                "year": _year(date_val),
                "runtime_min": None,
                "rating": rating,
                "poster": poster,
                "logo": _pick_logo(logos_json, iso639),
                "backdrop": backdrop,
                "trailer_youtube": trailer,
                "seasons": seasons,
                "prefetch_season": prefetch_season,
                "prefetch_episodes": prefetch_episodes,
                "cast": cast,
                "similar": self.app._tmdb_similar(kind, tid, iso639, iso3166),
            }
        finally:
            con.close()

    def app_browse(self, tab: str, page: int, iso639: str, iso3166: str | None):
        if page < 1:
            return None
        spec = BROWSE_TABS.get(tab)
        if not spec:
            return None
        mode, arg = spec
        limit = self.app.page_size + 1
        offset = (page - 1) * self.app.page_size

        def union_sql(where_movies: str, where_series: str, order_by: str, params: tuple):
            mv_gen = "genres" if "genres" in self.app.movies_cols else "''"
            tv_gen = "genres" if "genres" in self.app.series_cols else "''"
            return (
                f"""
                SELECT id,kind,name,dt,rating,pop,poster,backdrop,logos,gen FROM (
                  SELECT id,'movie' kind,title name,release_date dt,vote_average rating,popularity pop,poster_path poster,backdrop_path backdrop,logos_json logos,{mv_gen} gen
                  FROM movies
                  {where_movies}
                  UNION ALL
                  SELECT id,'series' kind,name name,first_air_date dt,vote_average rating,popularity pop,poster_path poster,backdrop_path backdrop,logos_json logos,{tv_gen} gen
                  FROM series
                  {where_series}
                )
                ORDER BY {order_by}
                LIMIT ? OFFSET ?
                """.strip(),
                (*params, limit, offset),
            )

        where_m = ""
        where_s = ""
        params: tuple = ()
        if mode == "genre":
            names = self._genre_needles(arg or "")
            if self.app.has_genres and self.app.has_title_genres and names:
                ph = ",".join("?" for _ in names)
                sql = f"""
                SELECT DISTINCT id,kind,name,dt,rating,pop,poster,backdrop,logos,gen FROM (
                  SELECT m.id id,'movie' kind,m.title name,m.release_date dt,m.vote_average rating,m.popularity pop,m.poster_path poster,m.backdrop_path backdrop,m.logos_json logos,'' gen
                  FROM movies m
                  JOIN title_genres tg ON tg.media_type='movie' AND tg.tmdb_id=m.id
                  JOIN genres g ON g.media_type='movie' AND g.genre_id=tg.genre_id
                  WHERE g.name IN ({ph})
                  UNION ALL
                  SELECT s.id id,'series' kind,s.name name,s.first_air_date dt,s.vote_average rating,s.popularity pop,s.poster_path poster,s.backdrop_path backdrop,s.logos_json logos,'' gen
                  FROM series s
                  JOIN title_genres tg ON tg.media_type='tv' AND tg.tmdb_id=s.id
                  JOIN genres g ON g.media_type='tv' AND g.genre_id=tg.genre_id
                  WHERE g.name IN ({ph})
                )
                ORDER BY COALESCE(pop,0) DESC
                LIMIT ? OFFSET ?
                """.strip()
                sql_params = (*names, *names, limit, offset)
                con = self.app._con()
                try:
                    rows = con.execute(sql, sql_params).fetchall()
                    has_more = len(rows) > self.app.page_size
                    rows = rows[: self.app.page_size]
                    items = []
                    for r in rows:
                        r = dict(r)
                        kind = r["kind"]
                        tid = int(r["id"])
                        media_type = "movie" if kind == "movie" else "tv"
                        t_title, _t_over = self.app._translated(con, media_type, tid, iso639, iso3166)
                        name = (t_title or r["name"] or "").strip()
                        items.append(
                            {
                                "id": tid,
                                "kind": kind,
                                "name": name,
                                "year": _year(r.get("dt")),
                                "rating": r.get("rating"),
                                "poster": r.get("poster"),
                                "logo": _pick_logo(r.get("logos"), iso639) or r.get("poster"),
                                "backdrop": r.get("backdrop"),
                            }
                        )
                    return {
                        "tab": tab,
                        "page": page,
                        "page_size": self.app.page_size,
                        "has_more": has_more,
                        "items": items,
                    }
                finally:
                    con.close()

            needle = arg or ""
            if "genres" in self.app.movies_cols:
                where_m = "WHERE COALESCE(genres,'') LIKE ?"
                params += (f"%{needle}%",)
            else:
                where_m = "WHERE 1=0"
            if "genres" in self.app.series_cols:
                where_s = "WHERE COALESCE(genres,'') LIKE ?"
                params += (f"%{needle}%",)
            else:
                where_s = "WHERE 1=0"

        order = {
            "popular": "COALESCE(pop,0) DESC",
            "rating": "COALESCE(rating,0) DESC, COALESCE(pop,0) DESC",
            "recent": "COALESCE(dt,'') DESC, COALESCE(pop,0) DESC",
            "genre": "COALESCE(pop,0) DESC",
        }[mode]

        sql, sql_params = union_sql(where_m, where_s, order, params)
        con = self.app._con()
        try:
            rows = con.execute(sql, sql_params).fetchall()
            has_more = len(rows) > self.app.page_size
            rows = rows[: self.app.page_size]
            items = []
            for r in rows:
                r = dict(r)
                kind = r["kind"]
                tid = int(r["id"])
                media_type = "movie" if kind == "movie" else "tv"
                t_title, _t_over = self.app._translated(con, media_type, tid, iso639, iso3166)
                name = (t_title or r["name"] or "").strip()
                items.append(
                    {
                        "id": tid,
                        "kind": kind,
                        "name": name,
                        "year": _year(r.get("dt")),
                        "rating": r.get("rating"),
                        "poster": r.get("poster"),
                        "logo": _pick_logo(r.get("logos"), iso639) or r.get("poster"),
                        "backdrop": r.get("backdrop"),
                    }
                )
            return {
                "tab": tab,
                "page": page,
                "page_size": self.app.page_size,
                "has_more": has_more,
                "items": items,
            }
        finally:
            con.close()

    def app_search_page(self, iso639: str, iso3166: str | None):
        home = self.app_home(iso639, iso3166)
        return {"trending_today": home["trending_today"], "query": "", "results": []}

    def app_search(self, query: str, iso639: str, iso3166: str | None):
        q = (query or "").strip()
        if not q:
            return {"query": "", "results": []}
        like = f"%{q}%"
        limit = 12
        con = self.app._con()
        try:
            sql = """
            SELECT id,kind,name,dt,rating,pop,poster,backdrop,logos FROM (
              SELECT m.id id,'movie' kind,COALESCE(tt.title,m.title) name,m.release_date dt,m.vote_average rating,m.popularity pop,m.poster_path poster,m.backdrop_path backdrop,m.logos_json logos,
                     COALESCE(tt.overview,m.overview) over
              FROM movies m
              LEFT JOIN title_translations tt
                ON tt.media_type='movie' AND tt.tmdb_id=m.id AND tt.iso_639_1=?
              UNION ALL
              SELECT s.id id,'series' kind,COALESCE(tt.title,s.name) name,s.first_air_date dt,s.vote_average rating,s.popularity pop,s.poster_path poster,s.backdrop_path backdrop,s.logos_json logos,
                     COALESCE(tt.overview,s.overview) over
              FROM series s
              LEFT JOIN title_translations tt
                ON tt.media_type='tv' AND tt.tmdb_id=s.id AND tt.iso_639_1=?
            )
            WHERE COALESCE(name,'') LIKE ? OR COALESCE(over,'') LIKE ?
            ORDER BY COALESCE(pop,0) DESC
            LIMIT ?
            """.strip()
            rows = con.execute(sql, (iso639, iso639, like, like, limit)).fetchall()
            out = []
            for r0 in rows:
                r = dict(r0)
                out.append(
                    {
                        "id": int(r["id"]),
                        "kind": r["kind"],
                        "name": (r.get("name") or "").strip(),
                        "year": _year(r.get("dt")),
                        "rating": r.get("rating"),
                        "poster": r.get("poster"),
                        "logo": _pick_logo(r.get("logos"), iso639) or r.get("poster"),
                        "backdrop": r.get("backdrop"),
                    }
                )
            return {"query": q, "results": out}
        finally:
            con.close()


class DualStackAPIServer(APIServer):
    address_family = socket.AF_INET6

    def server_bind(self):
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except OSError:
            pass
        super().server_bind()


def make_server(addr: str, port: int, db_path: str | None = None):
    db_path = db_path or (os.environ.get("CATALOG_DB") or os.path.join(os.getcwd(), "catalog.sqlite"))
    app = App(db_path)
    cls = DualStackAPIServer if ":" in addr else APIServer
    return cls((addr, port), H, app)


def serve(addr: str, port: int, tls_cert: str | None, tls_key: str | None, db_path: str | None = None):
    httpd = make_server(addr, port, db_path)
    if tls_cert and tls_key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(tls_cert, tls_key)
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
        httpd.is_tls = True
    httpd.serve_forever(poll_interval=0.5)


def main() -> int:
    addr = os.environ.get("BIND_ADDR") or "::"
    http_port = int(os.environ.get("HTTP_PORT") or "80")
    https_port = int(os.environ.get("HTTPS_PORT") or "443")
    tls_cert = os.environ.get("TLS_CERT") or ""
    tls_key = os.environ.get("TLS_KEY") or ""
    db_path = os.environ.get("CATALOG_DB") or os.path.join(os.getcwd(), "catalog.sqlite")

    t1 = Thread(target=serve, args=(addr, http_port, None, None, db_path), daemon=False)
    t1.start()

    if tls_cert and tls_key and os.path.isfile(tls_cert) and os.path.isfile(tls_key):
        serve(addr, https_port, tls_cert, tls_key, db_path)
        return 0

    t1.join()
    return 0
