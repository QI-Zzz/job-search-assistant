# Moving the job agent to Hetzner Cloud

The same Docker setup that runs on the Mac runs on a small rented Linux server, so the
pipeline no longer needs the laptop to be awake. Nothing in the pipeline changes.

| What | Where it comes from |
| --- | --- |
| Code | cloned from GitHub (so commit and push first) |
| `.env` (API keys, passwords) | copied from the Mac with `scp`; never in git |
| `profile/` (CV profile, job matching framework) | copied from the Mac with `scp`; never in git |
| Data: fetched jobs, extractions, scores, sent jobs | `export_data.sh` on the Mac, `import_data.sh` on the server |

Commands marked **Mac** run in a terminal on the Mac, from the repo folder. Commands marked
**VM** run on the server after logging in with `ssh`. Replace `PUBLIC_IP` with the server's address.

---

## Step 0: before you start

1. **Commit and push** all changes on the Mac, so the server clones the current code.
2. **Create an SSH key** on the Mac and copy its public half:

   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/job-agent
   pbcopy < ~/.ssh/job-agent.pub
   ```

3. **Sign up** at <https://www.hetzner.com/cloud>. Use real details and no VPN; it may ask for
   a card check or ID.

## Step 1: create the server (Hetzner console)

Create a project, then **Add Server**:

- **Location:** Falkenstein or Nuremberg.
- **Image:** Ubuntu 24.04.
- **Type:** shared vCPU, x86, **8 GB memory** to leave room for other projects (4 GB is the minimum).
- **Networking:** keep public IPv4 on.
- **SSH keys:** paste the public key.
- **Firewall:** create one allowing only inbound TCP port 22.
- **Name:** `job-agent`.

Write down the **public IP address**.

## Step 2: log in

**Mac:**

```bash
ssh -i ~/.ssh/job-agent root@PUBLIC_IP
```

Answer `yes` to the fingerprint question the first time.

## Step 3: set up the server

**VM:**

```bash
apt-get update && apt-get install -y git
git clone https://github.com/QI-Zzz/job-search-assistant.git
bash job-search-assistant/deploy/hetzner/setup_vm.sh
exit
```

This installs Docker, sets the clock to Amsterdam time, and builds the images (a few minutes).

## Step 4: copy the secrets and profile

**Mac:**

```bash
scp -i ~/.ssh/job-agent .env root@PUBLIC_IP:~/job-search-assistant/
scp -i ~/.ssh/job-agent -r profile root@PUBLIC_IP:~/job-search-assistant/
```

## Step 5: move the data (when no run is in progress on the Mac)

Do this outside the 07:30 run, so no job is lost or sent twice.

1. **Mac:** stop the schedule on the Mac, then export the data.

   ```bash
   docker compose exec airflow-scheduler airflow dags pause daily_job_agent
   bash deploy/hetzner/export_data.sh
   scp -i ~/.ssh/job-agent jobs_raw.dump root@PUBLIC_IP:~/job-search-assistant/
   ```

   Keep the row counts it prints.

2. **VM:** start the database and Airflow, then import.

   ```bash
   cd ~/job-search-assistant
   docker compose up -d postgres airflow-init airflow-webserver airflow-scheduler
   docker compose ps
   bash deploy/hetzner/import_data.sh
   ```

   Wait until `docker compose ps` shows the scheduler and webserver as running before the
   import. The row counts printed by the import should match the Mac's.

## Step 6: switch the schedule on and test

**VM:**

```bash
docker compose exec airflow-scheduler airflow dags unpause daily_job_agent
```

Airflow then starts **one run straight away** for the most recent schedule slot. That is
the test run: fetch, pass 1, scoring and the email (about 40 minutes, about $1.40). When the
email arrives, the cloud setup works. Check it with:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d daily_job_agent
```

If no run starts within a few minutes, start one by hand:
`docker compose exec airflow-scheduler airflow dags trigger daily_job_agent`.

From then on it runs Tuesday to Saturday at 07:30 Amsterdam time on the server.

## Step 7: retire the Mac setup

When the test email has arrived:

**Mac:**

```bash
docker compose stop
```

`stop` keeps the Mac's containers and data (a backup until the cloud has run for a while).
Never use `docker compose down -v`, which deletes the data.

---

## Day to day

**Open the Airflow UI.** **Mac:**

```bash
ssh -i ~/.ssh/job-agent -L 8080:localhost:8080 root@PUBLIC_IP
```

Then open <http://localhost:8080> while that terminal stays open. The server's ports are not
open to the internet; the tunnel is the way in.

**Deploy a code change** (after pushing to GitHub). **VM:**

```bash
cd ~/job-search-assistant && git pull
```

DAG, dbt and prompt changes take effect at the next run. If `airflow/Dockerfile` or a
`requirements` file changed, also run `docker compose up -d --build`.

**Change the profile or the job matching framework.** Edit it on the Mac, then copy it.
**Mac:**

```bash
scp -i ~/.ssh/job-agent profile/*.md root@PUBLIC_IP:~/job-search-assistant/profile/
```

**After a VM reboot** Docker and the containers start again by themselves (`restart:
unless-stopped`). To check: `docker compose ps` on the server.

## Troubleshooting

| Problem | Fix |
| --- | --- |
| dbt fails with `Permission denied` | **VM:** `cd ~/job-search-assistant && chown 50000 dbt_project && chown -R 50000 dbt_project/target dbt_project/logs` |
| `ssh` asks for a password or says "Permission denied (publickey)" | Use `-i ~/.ssh/job-agent` and user `root` |
| Server unreachable | Check it is running in the Hetzner console, and that the firewall allows port 22 |
