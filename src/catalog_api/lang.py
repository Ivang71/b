def _split_lang(s: str) -> tuple[str, str | None]:
    s = (s or "").strip()
    if not s:
        return ("en", None)
    s = s.replace("_", "-")
    if "-" in s:
        a, b = s.split("-", 1)
        a = a.strip().lower() or "en"
        b = b.strip().upper() or None
        return (a, b)
    return (s.lower(), None)


def _accept_lang(header_val: str | None) -> tuple[str, str | None]:
    if not header_val:
        return ("en", None)
    first = header_val.split(",", 1)[0].strip()
    tag = first.split(";", 1)[0].strip()
    return _split_lang(tag)


def _pick_lang(qs: dict, accept_language: str | None) -> tuple[str, str | None]:
    v = (qs.get("lang") or [""])[0].strip()
    if v:
        return _split_lang(v)
    return _accept_lang(accept_language)


def _lang_tag(iso639: str, iso3166: str | None):
    return f"{iso639}-{iso3166}" if iso3166 else iso639
