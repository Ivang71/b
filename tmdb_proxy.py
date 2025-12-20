#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, unquote, urlparse


def _load_env(path: str):
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


def _find_str(obj, pred):
    if isinstance(obj, str):
        return obj if pred(obj) else None
    if isinstance(obj, dict):
        for v in obj.values():
            r = _find_str(v, pred)
            if r:
                return r
    if isinstance(obj, list):
        for v in obj:
            r = _find_str(v, pred)
            if r:
                return r
    return None


def _maybe_json(s: str):
    s = (s or "").strip()
    if not s or not s.startswith("{") or "outbounds" not in s or "inbounds" not in s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _find_xray_config(obj):
    if isinstance(obj, dict):
        if isinstance(obj.get("outbounds"), list) and isinstance(obj.get("inbounds"), list):
            return obj
        for v in obj.values():
            if isinstance(v, str):
                j = _maybe_json(v)
                if isinstance(j, dict):
                    r = _find_xray_config(j)
                    if r:
                        return r
        for v in obj.values():
            r = _find_xray_config(v)
            if r:
                return r
    if isinstance(obj, list):
        for v in obj:
            r = _find_xray_config(v)
            if r:
                return r
    return None


def _parse_vless(vless_url: str):
    u = urlparse(vless_url)
    if u.scheme != "vless":
        return None
    if not u.hostname or not u.port or not u.username:
        return None
    qs = parse_qs(u.query or "")
    q = {k: (v[0] if v else "") for k, v in qs.items()}
    security = (q.get("security") or "").strip().lower()
    net = (q.get("type") or "tcp").strip().lower() or "tcp"
    flow = (q.get("flow") or "").strip()
    sni = (q.get("sni") or q.get("serverName") or "").strip()
    fp = (q.get("fp") or "").strip()
    pbk = (q.get("pbk") or q.get("publicKey") or "").strip()
    sid = (q.get("sid") or q.get("shortId") or "").strip()
    spx = (q.get("spx") or q.get("spiderX") or "").strip()
    alpn = (q.get("alpn") or "").strip()
    path = (q.get("path") or u.path or "").strip()
    if path and not path.startswith("/"):
        path = "/" + path
    host_hdr = (q.get("host") or "").strip()

    stream = {"network": net}
    if security in ("tls", "reality"):
        stream["security"] = security
        if security == "tls":
            ts = {}
            if sni:
                ts["serverName"] = sni
            if alpn:
                ts["alpn"] = [x for x in alpn.split(",") if x.strip()]
            stream["tlsSettings"] = ts
        else:
            rs = {}
            if sni:
                rs["serverName"] = sni
            if fp:
                rs["fingerprint"] = fp
            if pbk:
                rs["publicKey"] = pbk
            if sid:
                rs["shortId"] = sid
            if spx:
                rs["spiderX"] = spx
            stream["realitySettings"] = rs

    if net == "ws":
        ws = {}
        if path:
            ws["path"] = unquote(path)
        if host_hdr:
            ws["headers"] = {"Host": host_hdr}
        stream["wsSettings"] = ws
    if net == "grpc":
        svc = (q.get("serviceName") or "").strip()
        if svc:
            stream["grpcSettings"] = {"serviceName": svc}

    user = {"id": u.username, "encryption": "none"}
    if flow:
        user["flow"] = flow

    return {
        "log": {"loglevel": "warning"},
        "inbounds": [],
        "outbounds": [
            {
                "tag": "proxy",
                "protocol": "vless",
                "settings": {"vnext": [{"address": u.hostname, "port": int(u.port), "users": [user]}]},
                "streamSettings": stream,
            },
            {"tag": "direct", "protocol": "freedom", "settings": {}},
            {"tag": "block", "protocol": "blackhole", "settings": {}},
        ],
    }


def _pick_outbound_tag(outbounds: list[dict]):
    for ob in outbounds:
        proto = (ob.get("protocol") or "").lower()
        if proto in ("freedom", "direct", "blackhole", "dns"):
            continue
        tag = (ob.get("tag") or "").strip()
        if tag:
            return tag
    for ob in outbounds:
        tag = (ob.get("tag") or "").strip()
        if tag:
            return tag
    return "proxy"


def _normalize_outbounds(outbounds: list[dict]):
    out = []
    seen = set()
    proxy_set = False
    for ob in outbounds:
        if not isinstance(ob, dict):
            continue
        tag = (ob.get("tag") or "").strip()
        proto = (ob.get("protocol") or "").lower()
        if not tag:
            if not proxy_set and proto not in ("freedom", "blackhole", "dns"):
                tag = "proxy"
                proxy_set = True
            else:
                i = 1
                while True:
                    cand = f"out{i}"
                    if cand not in seen:
                        tag = cand
                        break
                    i += 1
        if tag in seen:
            i = 1
            while True:
                cand = f"{tag}_{i}"
                if cand not in seen:
                    tag = cand
                    break
                i += 1
        ob = dict(ob)
        ob["tag"] = tag
        seen.add(tag)
        out.append(ob)

    if "proxy" not in seen and out:
        out[0]["tag"] = "proxy"
        seen.add("proxy")

    if "direct" not in seen:
        out.append({"tag": "direct", "protocol": "freedom", "settings": {}})
    if "block" not in seen:
        out.append({"tag": "block", "protocol": "blackhole", "settings": {}})
    return out


def _decode_amnezia(vpn_uri: str):
    decoder = os.environ.get("AMNEZIA_DECODER") or os.path.join(os.getcwd(), "amnezia_decode.py")
    if not os.path.isfile(decoder):
        return None
    try:
        p = subprocess.run(
            [sys.executable, decoder, vpn_uri],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
    except Exception:
        return None
    if not p.stdout:
        return None
    try:
        return json.loads(p.stdout.decode("utf-8", errors="replace"))
    except Exception:
        return None


def main() -> int:
    env = {**_load_env(os.path.join(os.getcwd(), ".env")), **os.environ}
    listen = (env.get("TMDB_PROXY_LISTEN") or "127.0.0.1").strip() or "127.0.0.1"
    port = int((env.get("TMDB_PROXY_PORT") or "3128").strip() or "3128")
    link = (env.get("AMNEZIA_VPN") or env.get("TMDB_VLESS") or "").strip()
    if not link:
        return 2

    cfg = None
    if link.startswith("vpn://"):
        cfg = _decode_amnezia(link)
    elif link.startswith("{"):
        try:
            cfg = json.loads(link)
        except Exception:
            cfg = None

    if not isinstance(cfg, dict):
        return 3

    x = _find_xray_config(cfg)
    if not isinstance(x, dict):
        vless = _find_str(cfg, lambda s: s.startswith("vless://"))
        if vless:
            x = _parse_vless(vless)
    if not isinstance(x, dict):
        return 4

    outbounds = x.get("outbounds") or []
    if not isinstance(outbounds, list) or not outbounds:
        return 5

    outbounds = _normalize_outbounds([ob for ob in outbounds if isinstance(ob, dict)])
    tag = "proxy"
    xray_cfg = {
        "log": {"loglevel": "warning"},
        "inbounds": [{"listen": listen, "port": port, "protocol": "http", "settings": {}}],
        "outbounds": outbounds,
        "routing": {"domainStrategy": "AsIs", "rules": [{"type": "field", "network": "tcp,udp", "outboundTag": tag}]},
    }

    xray_path = env.get("XRAY_BIN") or "/usr/local/bin/xray"
    if not os.path.isfile(xray_path):
        xray_path = "/usr/bin/xray"
    if not os.path.isfile(xray_path):
        return 6

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".json") as f:
        json.dump(xray_cfg, f, ensure_ascii=False, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
        cfg_path = f.name

    os.execv(xray_path, [xray_path, "run", "-c", cfg_path])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

