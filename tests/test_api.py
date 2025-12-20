import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import date, timedelta
from http.client import HTTPConnection
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _req(port: int, path: str, headers: dict | None = None):
    c = HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        c.request("GET", path, headers=headers or {})
        r = c.getresponse()
        body = r.read()
        return r.status, dict(r.getheaders()), body
    finally:
        c.close()


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.tmp.name, "catalog.sqlite")
        con = sqlite3.connect(cls.db_path)
        today = date.today()
        d30 = (today - timedelta(days=30)).isoformat()
        d20 = (today - timedelta(days=20)).isoformat()
        con.executescript(
            """
            CREATE TABLE movies(
              id INTEGER PRIMARY KEY,
              title TEXT,
              vote_average REAL,
              vote_count INTEGER,
              release_date TEXT,
              overview TEXT,
              popularity REAL,
              poster_path TEXT,
              backdrop_path TEXT,
              logos_json TEXT,
              genres TEXT
            );
            CREATE TABLE series(
              id INTEGER PRIMARY KEY,
              name TEXT,
              vote_average REAL,
              vote_count INTEGER,
              first_air_date TEXT,
              overview TEXT,
              popularity REAL,
              poster_path TEXT,
              backdrop_path TEXT,
              logos_json TEXT,
              genres TEXT,
              networks TEXT,
              number_of_seasons INTEGER,
              number_of_episodes INTEGER
            );
            CREATE TABLE title_translations(
              media_type TEXT NOT NULL,
              tmdb_id INTEGER NOT NULL,
              iso_639_1 TEXT NOT NULL,
              iso_3166_1 TEXT NOT NULL,
              title TEXT,
              overview TEXT,
              tagline TEXT,
              homepage TEXT,
              PRIMARY KEY(media_type, tmdb_id, iso_639_1, iso_3166_1)
            );
            CREATE TABLE title_videos(
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
            );
            CREATE TABLE tv_seasons(
              series_id INTEGER NOT NULL,
              season_number INTEGER NOT NULL,
              season_id INTEGER,
              name TEXT,
              overview TEXT,
              air_date TEXT,
              poster_path TEXT,
              episode_count INTEGER,
              PRIMARY KEY(series_id, season_number)
            );
            CREATE TABLE tv_episodes(
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
            );
            CREATE TABLE title_cast(
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
            );
            """
        )

        con.execute(
            "INSERT INTO movies(id,title,vote_average,vote_count,release_date,overview,popularity,poster_path,backdrop_path,logos_json,genres) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                1,
                "English Movie",
                8.7,
                900,
                d30,
                "English overview",
                500.0,
                "/m1_poster.jpg",
                "/m1_backdrop.jpg",
                '{"en":"/m1_logo_en.png","de":"/m1_logo_de.png"}',
                "Action,Comedy",
            ),
        )
        con.execute(
            "INSERT INTO title_translations(media_type,tmdb_id,iso_639_1,iso_3166_1,title,overview,tagline,homepage) VALUES(?,?,?,?,?,?,?,?)",
            ("movie", 1, "de", "DE", "Deutscher Film", "Deutsche Übersicht", "", ""),
        )
        con.execute(
            "INSERT INTO title_videos(media_type,tmdb_id,key,site,type) VALUES(?,?,?,?,?)",
            ("movie", 1, "abc123", "YouTube", "Trailer"),
        )

        con.execute(
            "INSERT INTO series(id,name,vote_average,vote_count,first_air_date,overview,popularity,poster_path,backdrop_path,logos_json,genres,networks,number_of_seasons,number_of_episodes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                100,
                "English Series",
                9.1,
                1200,
                d20,
                "Series overview",
                800.0,
                "/s1_poster.jpg",
                "/s1_backdrop.jpg",
                '{"en":"/s1_logo_en.png","de":"/s1_logo_de.png"}',
                "Drama,Action",
                "Netflix",
                1,
                8,
            ),
        )
        con.execute(
            "INSERT INTO title_translations(media_type,tmdb_id,iso_639_1,iso_3166_1,title,overview,tagline,homepage) VALUES(?,?,?,?,?,?,?,?)",
            ("tv", 100, "de", "DE", "Deutsche Serie", "Serien Übersicht", "", ""),
        )
        con.execute(
            "INSERT INTO series(id,name,vote_average,vote_count,first_air_date,overview,popularity,poster_path,backdrop_path,logos_json,genres,networks,number_of_seasons,number_of_episodes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                101,
                "Game of Thrones",
                9.2,
                5000,
                d30,
                "A story of thrones.",
                600.0,
                "/got_poster.jpg",
                "/got_backdrop.jpg",
                '{"en":"/got_logo_en.png","de":"/got_logo_de.png"}',
                "Drama",
                "HBO",
                8,
                73,
            ),
        )
        con.execute(
            "INSERT INTO title_translations(media_type,tmdb_id,iso_639_1,iso_3166_1,title,overview,tagline,homepage) VALUES(?,?,?,?,?,?,?,?)",
            ("tv", 101, "de", "DE", "Spiel der Throne", "Eine Geschichte.", "", ""),
        )
        con.execute(
            "INSERT INTO tv_seasons(series_id,season_number,name,episode_count) VALUES(?,?,?,?)",
            (100, 1, "Season 1", 8),
        )
        con.execute(
            "INSERT INTO tv_episodes(series_id,season_number,episode_number,name,overview,runtime,still_path) VALUES(?,?,?,?,?,?,?)",
            (100, 1, 1, "Pilot", "Pilot overview", 55, "/ep1.jpg"),
        )
        con.execute(
            "INSERT INTO title_cast(media_type,tmdb_id,person_id,credit_id,name,character,ord,profile_path) VALUES(?,?,?,?,?,?,?,?)",
            ("tv", 100, 501, "cred1", "Actor One", "Hero", 0, "/p1.jpg"),
        )
        con.commit()
        con.close()

        os.environ["CATALOG_DB"] = cls.db_path
        os.environ.pop("TMDB_API_KEY", None)
        os.environ.pop("TMDB_BEARER_TOKEN", None)
        os.environ["DISABLE_DOTENV"] = "1"

        import api_server

        cls.httpd = api_server.make_server("127.0.0.1", 0, cls.db_path)
        cls.port = cls.httpd.server_address[1]
        cls.th = threading.Thread(target=cls.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        cls.th.start()
        t0 = time.time()
        while time.time() - t0 < 2:
            s, _, _ = _req(cls.port, "/ping")
            if s == 200:
                break
            time.sleep(0.02)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.tmp.cleanup()

    def test_lang_priority_query_param(self):
        s, h, b = _req(self.port, "/v1/home?lang=de", {"Accept-Language": "en-US,en;q=0.9"})
        self.assertEqual(s, 200)
        self.assertIn("application/json", h.get("Content-Type", ""))
        data = json.loads(b)
        self.assertIn("Deutscher Film", {it["name"] for it in data["slider"]})

    def test_lang_fallback_accept_language_then_en(self):
        s, _, b = _req(self.port, "/v1/home", {"Accept-Language": "de-DE,de;q=0.9"})
        self.assertIn("Deutscher Film", {it["name"] for it in json.loads(b)["slider"]})

        s, _, b = _req(self.port, "/v1/home")
        self.assertIn("English Movie", {it["name"] for it in json.loads(b)["slider"]})

    def test_home_shape(self):
        s, _, b = _req(self.port, "/v1/home?lang=en")
        self.assertEqual(s, 200)
        data = json.loads(b)
        for k in ("as_of", "providers", "slider", "top10_today", "trending_today", "series_on", "top_rated", "genres"):
            self.assertIn(k, data)
        self.assertIn("backdrop", data["slider"][0])
        self.assertIn("backdrop", data["top10_today"][0])
        self.assertEqual(
            data["providers"],
            ["Netflix", "Prime", "Max", "Disney+", "AppleTV", "Paramount"],
        )
        for p in data["providers"]:
            self.assertIn(p, data["series_on"])

    def test_title_movie_page(self):
        s, _, b = _req(self.port, "/v1/titles/1?lang=de")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertEqual(d["kind"], "movie")
        self.assertEqual(d["name"], "Deutscher Film")
        self.assertEqual(d["trailer_youtube"]["key"], "abc123")
        self.assertIn("youtube.com", d["trailer_youtube"]["url"])
        self.assertIn("similar", d)
        self.assertIsInstance(d["similar"], list)

    def test_title_series_page_prefetch_episode_and_cast(self):
        s, _, b = _req(self.port, "/v1/titles/100?lang=de")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertEqual(d["kind"], "series")
        self.assertEqual(d["name"], "Deutsche Serie")
        self.assertTrue(d["seasons"])
        self.assertEqual(d["prefetch_season"], 1)
        self.assertTrue(d["prefetch_episodes"])
        self.assertEqual(d["prefetch_episodes"][0]["episode"], 1)
        self.assertTrue(d["cast"])
        self.assertEqual(d["cast"][0]["name"], "Actor One")

    def test_browse_tab_and_search(self):
        s, _, b = _req(self.port, "/v1/browse/recent/1?lang=en")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertEqual(d["page"], 1)
        self.assertIn("items", d)

        s, _, b = _req(self.port, "/v1/browse/action/1?lang=en")
        self.assertEqual(s, 200)
        d = json.loads(b)
        ids = {it["id"] for it in d["items"]}
        self.assertIn(1, ids)

        s, _, b = _req(self.port, "/v1/search?lang=en")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertIn("trending_today", d)
        self.assertEqual(d["query"], "")

        q = quote("Deutscher")
        s, _, b = _req(self.port, f"/v1/search/{q}?lang=de")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertEqual(d["query"], "Deutscher")
        self.assertTrue(d["results"])

        q = quote("thrones")
        s, _, b = _req(self.port, f"/v1/search/{q}?lang=de")
        self.assertEqual(s, 200)
        d = json.loads(b)
        ids = {it["id"] for it in d["results"]}
        self.assertIn(101, ids)

        q = quote("thrones")
        s, _, b = _req(self.port, f"/v1/search/{q}", {"Accept-Language": "de-DE,de;q=0.9"})
        self.assertEqual(s, 200)
        d = json.loads(b)
        ids = {it["id"] for it in d["results"]}
        self.assertIn(101, ids)

    def test_title_tmdb_fallback_when_missing_locally(self):
        app = self.httpd.app
        app.tmdb_key = "x"
        app._tmdb_similar = lambda *_a, **_k: []

        def fake_tmdb(url: str, _timeout_s: float):
            if "/movie/211089" in url and "/videos" not in url and "/credits" not in url:
                return (None, None)
            if "/tv/211089?" in url:
                return (
                    200,
                    {
                        "name": "Remote Series",
                        "overview": "Remote overview",
                        "first_air_date": "2020-01-01",
                        "poster_path": "/p.jpg",
                        "backdrop_path": "/b.jpg",
                        "vote_average": 7.7,
                        "genres": [{"name": "Drama"}],
                        "seasons": [{"season_number": 1, "episode_count": 2}],
                    },
                )
            if "/tv/211089/season/1" in url:
                return (
                    200,
                    {
                        "episodes": [
                            {"episode_number": 1, "name": "E1", "runtime": 50, "still_path": "/s1.jpg"},
                            {"episode_number": 2, "name": "E2", "runtime": 49, "still_path": "/s2.jpg"},
                        ]
                    },
                )
            if "/tv/211089/videos" in url:
                return (200, {"results": [{"site": "YouTube", "key": "k1"}]})
            if "/tv/211089/credits" in url:
                return (200, {"cast": [{"id": 1, "credit_id": "c1", "name": "A", "character": "C", "order": 0, "profile_path": "/x.jpg"}]})
            return (404, None)

        app._tmdb_get_json = lambda url, timeout_s: fake_tmdb(url, timeout_s)

        s, _, b = _req(self.port, "/v1/titles/211089?lang=en")
        self.assertEqual(s, 200)
        d = json.loads(b)
        self.assertEqual(d["kind"], "series")
        self.assertEqual(d["name"], "Remote Series")
        self.assertEqual(d["prefetch_season"], 1)
        self.assertEqual(d["prefetch_episodes"][0]["episode"], 1)


if __name__ == "__main__":
    unittest.main()

