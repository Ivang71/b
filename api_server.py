#!/usr/bin/env python3
from src.catalog_api.app import App
from src.catalog_api.server import main, make_server, serve


__all__ = ["App", "make_server", "serve", "main"]


if __name__ == "__main__":
    raise SystemExit(main())


