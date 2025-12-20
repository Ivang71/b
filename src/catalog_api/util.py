import json


def _year(d: str | None):
    if not d or len(d) < 4:
        return None
    try:
        return int(d[:4])
    except Exception:
        return None


def _json_loads_best_effort(s: str | None):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _pick_logo(logos_json: str | None, lang: str):
    d = _json_loads_best_effort(logos_json)
    if not isinstance(d, dict):
        return None
    if lang in d and d.get(lang):
        return d.get(lang)
    if "en" in d and d.get("en"):
        return d.get("en")
    if "und" in d and d.get("und"):
        return d.get("und")
    for v in d.values():
        if v:
            return v
    return None


def _load_dotenv(path: str):
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
        return {}
    return out
