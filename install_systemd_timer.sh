#!/usr/bin/env bash
set -euo pipefail

wd="$(pwd)"
py="$(command -v python3 || true)"
[ -n "$py" ] || { echo "python3 not found"; exit 1; }

mkdir -p "$HOME/.config/systemd/user"

svc="$HOME/.config/systemd/user/tmdb-sync.service"
tmr="$HOME/.config/systemd/user/tmdb-sync.timer"

cat >"$svc" <<EOF
[Unit]
Description=TMDB catalog rolling sync

[Service]
Type=oneshot
WorkingDirectory=$wd
ExecStart=$py $wd/sync_recent.py
EOF

cat >"$tmr" <<'EOF'
[Unit]
Description=Run TMDB sync every 2 hours

[Timer]
OnBootSec=2m
OnUnitActiveSec=2h
RandomizedDelaySec=30s
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now tmdb-sync.timer
systemctl --user status tmdb-sync.timer --no-pager


