#!/usr/bin/env bash
# One-time setup of a fresh Oracle Cloud Ubuntu VM (Arm, Always Free) for the job agent.
# Run on the VM as the default "ubuntu" user:
#     bash setup_vm.sh
# Installs Docker from Ubuntu's own packages, clones the repo and builds the images.
# It does not start the pipeline: copy .env and profile/ first (see README.md, step 4).
set -euo pipefail

REPO_URL="https://github.com/QI-Zzz/job-search-assistant.git"
APP_DIR="$HOME/job-search-assistant"

echo "== Installing Docker, Docker Compose and git"
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 docker-buildx git
sudo systemctl enable --now docker
# let the ubuntu user run docker without sudo (takes effect at the next login)
sudo usermod -aG docker "$USER"

echo "== Setting the time zone to Amsterdam (only affects log timestamps; the DAG has its own time zone)"
sudo timedatectl set-timezone Europe/Amsterdam

echo "== Cloning the repo"
if [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$APP_DIR"
fi

echo "== Building the images (a few minutes on first run)"
cd "$APP_DIR"
sudo docker compose build airflow-init airflow-webserver airflow-scheduler

echo
echo "Done. Next (README.md, step 4): copy .env and profile/ from your Mac into $APP_DIR,"
echo "then log out and back in so docker works without sudo."
