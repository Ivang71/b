movies:
python3 fetch_movies.py
python3 enrich_logos.py

series:
python3 fetch_tv_series_csv.py
python3 enrich_tv_logos.py

merged:
python3 merge_movie_tv_dbs.py
python3 enrich_translations.py
python3 enrich_credits.py
python3 enrich_videos.py
python3 enrich_genres.py
python3 migrate_drop_series_genres.py
<!-- python3 enrich_episodes.py -->


```bash
sudo apt update && sudo apt install sqlite3 -y
```

```bash
scp -c aes128-ctr catalog.sqlite root@host:/opt/origin-api/
```

ping:
curl -fsS http://127.0.0.1/ping
curl -kfsS https://127.0.0.1/ping

setup (service + cloudflare-only firewall):
create an Origin Certificate in cloudflare, then:
./setup_server.sh origin.crt origin.key
./setup_server.sh /etc/origin-api/tls.crt /etc/origin-api/tls.key # reinstall

service control:
systemctl status origin-api.service --no-pager
systemctl restart origin-api.service                # restart
journalctl -u origin-api.service -n 200 --no-pager

firewall control:
systemctl status cloudflare-firewall.timer --no-pager
systemctl start cloudflare-firewall.service
journalctl -u cloudflare-firewall.service -n 200 --no-pager


## after code change:
./setup_server.sh /etc/origin-api/tls.crt /etc/origin-api/tls.key && systemctl restart origin-api.service


# Setup

```bash
# Install dependencies
sudo apt update && sudo apt install -y python3 python3-pip sqlite3 ipset curl unzip
pip3 install requests

# Clone project
git clone <repo> /root/b && cd /root/b

# Create .env file
echo "AMNEZIA_VPN=
TMDB_API_KEY=
TMDB_PROXY=http://127.0.0.1:3128
TMDB_PROXY_LISTEN=127.0.0.1
TMDB_PROXY_PORT=3128
CORS_ALLOW_LOCALHOST=
" > /opt/origin-api/.env

# Copy database (or build from scratch)
scp catalog.sqlite /root/b/

# Setup periodic sync
./install_systemd_timer.sh

# Production server setup (requires Cloudflare origin cert)
./setup_server.sh origin.crt origin.key
```
