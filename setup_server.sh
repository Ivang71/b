#!/usr/bin/env bash
set -euo pipefail

origin_crt="${1:-}"
origin_key="${2:-}"

if [ -z "$origin_crt" ] || [ -z "$origin_key" ]; then
  echo "usage: ./setup_server.sh origin.crt origin.key"
  exit 2
fi
[ -s "$origin_crt" ] && [ -s "$origin_key" ] || { echo "missing cert/key"; exit 2; }

svc_name="origin-api"
svc_user="originapi"
opt_dir="/opt/$svc_name"
etc_dir="/etc/$svc_name"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ipset curl unzip python3-brotli sqlite3

if systemctl is-enabled --quiet legacy-api.service 2>/dev/null || systemctl is-active --quiet legacy-api.service 2>/dev/null; then
  systemctl disable --now legacy-api.service || true
fi
rm -f /etc/systemd/system/legacy-api.service

id -u "$svc_user" >/dev/null 2>&1 || useradd --system --home /nonexistent --shell /usr/sbin/nologin "$svc_user"

install -d -m 0755 "$opt_dir"
install -m 0755 /root/b/api_server.py "$opt_dir/api_server.py"
install -m 0755 /root/b/tmdb_proxy.py "$opt_dir/tmdb_proxy.py"
install -m 0750 /root/b/backup_db.sh "$opt_dir/backup_db.sh"
rm -rf "$opt_dir/src"
cp -a /root/b/src "$opt_dir/src"
install -d -m 0750 -o "$svc_user" -g "$svc_user" "$opt_dir/backups"

if [ -s /root/b/catalog.sqlite ] && [ ! -s "$opt_dir/catalog.sqlite" ]; then
  install -m 0640 /root/b/catalog.sqlite "$opt_dir/catalog.sqlite"
  chown "$svc_user":"$svc_user" "$opt_dir/catalog.sqlite" || true
fi

if [ -s /root/b/.env ] && [ ! -s "$opt_dir/.env" ]; then
  install -m 0640 /root/b/.env "$opt_dir/.env"
  chown "$svc_user":"$svc_user" "$opt_dir/.env" || true
fi

install -d -m 0755 "$etc_dir"
src_crt="$(readlink -f "$origin_crt" || true)"
dst_crt="$(readlink -f "$etc_dir/tls.crt" 2>/dev/null || true)"
if [ "$src_crt" != "$dst_crt" ]; then
  install -m 0644 "$origin_crt" "$etc_dir/tls.crt"
fi
if curl -fsS https://developers.cloudflare.com/ssl/static/origin_ca_rsa_root.pem -o /tmp/origin_ca_rsa_root.pem; then
  fp="$(sha256sum /tmp/origin_ca_rsa_root.pem | awk '{print $1}')"
  if ! grep -q "$fp" "$etc_dir/tls.crt" 2>/dev/null; then
    printf "\n# origin_ca_rsa_root.sha256=%s\n" "$fp" >>"$etc_dir/tls.crt"
    cat /tmp/origin_ca_rsa_root.pem >>"$etc_dir/tls.crt"
  fi
fi
src_key="$(readlink -f "$origin_key" || true)"
dst_key="$(readlink -f "$etc_dir/tls.key" 2>/dev/null || true)"
if [ "$src_key" != "$dst_key" ]; then
  install -m 0640 "$origin_key" "$etc_dir/tls.key"
fi
chown root:"$svc_user" "$etc_dir/tls.crt" "$etc_dir/tls.key"

if [ ! -x /usr/local/bin/xray ]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  curl -fsSL -o "$tmp/xray.zip" https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip
  unzip -q "$tmp/xray.zip" -d "$tmp/xray"
  install -m 0755 "$tmp/xray/xray" /usr/local/bin/xray
  rm -rf "$tmp"
  trap - EXIT
fi

if [ ! -f "$opt_dir/amnezia_decode.py" ]; then
  curl -fsSL -o "$opt_dir/amnezia_decode.py" https://raw.githubusercontent.com/andr13/amnezia-config-decoder/main/amnezia-config-decoder.py
  chmod 0644 "$opt_dir/amnezia_decode.py"
  chown "$svc_user":"$svc_user" "$opt_dir/amnezia_decode.py" || true
fi

cat >/etc/systemd/system/tmdb-proxy.service <<EOF
[Unit]
Description=Local TMDB proxy (xray)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$svc_user
Group=$svc_user
WorkingDirectory=$opt_dir
EnvironmentFile=-$opt_dir/.env
ExecStart=/usr/bin/python3 $opt_dir/tmdb_proxy.py
Restart=on-failure
RestartSec=2
RestartPreventExitStatus=2 3 4 5 6
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$opt_dir

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/${svc_name}.service <<EOF
[Unit]
Description=Origin API (ping)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$svc_user
Group=$svc_user
WorkingDirectory=$opt_dir
EnvironmentFile=-$opt_dir/.env
Environment=BIND_ADDR=::
Environment=HTTP_PORT=80
Environment=HTTPS_PORT=443
Environment=TLS_CERT=$etc_dir/tls.crt
Environment=TLS_KEY=$etc_dir/tls.key
ExecStart=/usr/bin/python3 $opt_dir/api_server.py
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$opt_dir

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/${svc_name}-db-backup.service <<EOF
[Unit]
Description=Origin API sqlite backup

[Service]
Type=oneshot
User=$svc_user
Group=$svc_user
WorkingDirectory=$opt_dir
EnvironmentFile=-$opt_dir/.env
ExecStart=$opt_dir/backup_db.sh
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$opt_dir
EOF

cat >/etc/systemd/system/${svc_name}-db-backup.timer <<'EOF'
[Unit]
Description=Daily Origin API sqlite backup

[Timer]
OnCalendar=daily
RandomizedDelaySec=1h
Persistent=true

[Install]
WantedBy=timers.target
EOF

cat >/usr/local/sbin/cf_firewall_apply.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

cf4_url="https://www.cloudflare.com/ips-v4"
cf6_url="https://www.cloudflare.com/ips-v6"

have() { command -v "$1" >/dev/null 2>&1; }

have curl || exit 1
have ipset || exit 1
have iptables || exit 1
have ip6tables || exit 1

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsS "$cf4_url" -o "$tmp/cf4.txt"
curl -fsS "$cf6_url" -o "$tmp/cf6.txt"

ipset create cloudflare4 hash:net family inet -exist
ipset create cloudflare6 hash:net family inet6 -exist
ipset flush cloudflare4
ipset flush cloudflare6

while IFS= read -r cidr; do
  [ -n "$cidr" ] || continue
  ipset add cloudflare4 "$cidr" -exist
done <"$tmp/cf4.txt"

while IFS= read -r cidr; do
  [ -n "$cidr" ] || continue
  ipset add cloudflare6 "$cidr" -exist
done <"$tmp/cf6.txt"

ipt() { iptables -w "$@"; }
ip6t() { ip6tables -w "$@"; }

ensure_rule() {
  local tool="$1"; shift
  if ! "$tool" -C INPUT "$@" 2>/dev/null; then
    "$tool" -I INPUT 1 "$@"
  fi
}

ensure_rule ipt -i lo -j ACCEPT
ensure_rule ipt -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ensure_rule ipt -p tcp --dport 22 -j ACCEPT

ensure_rule ipt -p tcp -m set --match-set cloudflare4 src --dport 80 -j ACCEPT
ensure_rule ipt -p tcp -m set --match-set cloudflare4 src --dport 443 -j ACCEPT

iptables -w -P INPUT DROP

ensure_rule ip6t -i lo -j ACCEPT
ensure_rule ip6t -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ensure_rule ip6t -p tcp --dport 22 -j ACCEPT

ensure_rule ip6t -p tcp -m set --match-set cloudflare6 src --dport 80 -j ACCEPT
ensure_rule ip6t -p tcp -m set --match-set cloudflare6 src --dport 443 -j ACCEPT

ip6tables -w -P INPUT DROP
EOF
chmod 0755 /usr/local/sbin/cf_firewall_apply.sh

cat >/etc/systemd/system/cloudflare-firewall.service <<'EOF'
[Unit]
Description=Apply Cloudflare-only firewall rules (80/443)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/cf_firewall_apply.sh
EOF

cat >/etc/systemd/system/cloudflare-firewall.timer <<'EOF'
[Unit]
Description=Refresh Cloudflare IP ranges and re-apply firewall

[Timer]
OnBootSec=1m
OnUnitActiveSec=12h
RandomizedDelaySec=10m
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
if [ -s "$opt_dir/.env" ] && grep -Eq '^(AMNEZIA_VPN|TMDB_VLESS)=.+' "$opt_dir/.env"; then
  systemctl enable --now tmdb-proxy.service || true
else
  systemctl disable --now tmdb-proxy.service 2>/dev/null || true
fi
systemctl enable --now ${svc_name}.service cloudflare-firewall.timer ${svc_name}-db-backup.timer
systemctl start cloudflare-firewall.service


