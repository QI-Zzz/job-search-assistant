#!/usr/bin/env bash
# One-time setup of a fresh Hetzner Cloud Ubuntu 24.04 server for the job agent.
# Run on the server as root (Hetzner's default login):
#     bash setup_vm.sh
# Installs Docker from Ubuntu's own packages, clones the repo and builds the images.
# It does not start the pipeline: copy .env and profile/ first (see README.md, step 4).
set -euo pipefail

REPO_URL="https://github.com/QI-Zzz/job-search-assistant.git"
APP_DIR="$HOME/job-search-assistant"
AIRFLOW_UID=50000  # the "airflow" user inside the containers

echo "== Installing Docker, Docker Compose and git"
apt-get update
apt-get install -y docker.io docker-compose-v2 docker-buildx git
systemctl enable --now docker

echo "== Setting the time zone to Amsterdam (only affects log timestamps; the DAG has its own time zone)"
timedatectl set-timezone Europe/Amsterdam

# Hetzner images have no swap; a little protects a 4 GB server from running out of memory
if ! swapon --show | grep -q .; then
    echo "== Adding 2 GB of swap"
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "== Cloning the repo"
if [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

# On Linux the mounted folder keeps its owner, and dbt (running as airflow) writes to these
echo "== Letting the containers write dbt's working files"
mkdir -p dbt_project/target dbt_project/logs
chown "$AIRFLOW_UID" dbt_project
chown -R "$AIRFLOW_UID" dbt_project/target dbt_project/logs

echo "== Building the images (a few minutes on first run)"
docker compose build airflow-init airflow-webserver airflow-scheduler

echo
echo "Done. Next (README.md, step 4): copy .env and profile/ from your Mac into $APP_DIR."
