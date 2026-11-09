# Moving the job agent to Oracle Cloud (Always Free)

The same Docker setup that runs on the Mac runs on a free Oracle Cloud Arm VM, so the
pipeline no longer needs the laptop to be awake. Nothing in the pipeline changes.

| What | Where it comes from |
| --- | --- |
| Code | cloned from GitHub (so commit and push first) |
| `.env` (API keys, passwords) | copied from the Mac with `scp`; never in git |
| `profile/` (CV profile, job matching framework) | copied from the Mac with `scp`; never in git |
| Data: fetched jobs, extractions, scores, sent jobs | `export_data.sh` on the Mac, `import_data.sh` on the VM |

Commands marked **Mac** run in a terminal on the Mac, from the repo folder. Commands marked
**VM** run on the cloud VM after logging in with `ssh`.

---

## Step 0: before the weekend

1. **Sign up** at <https://www.oracle.com/cloud/free/>. Approval can take a while.
   - **Home region:** pick *Netherlands Northwest (Amsterdam)* or *Germany Central (Frankfurt)*.
     The free Arm VM is only available in the home region, and it cannot be changed later.
   - A card is needed to verify the account. Always Free resources are not charged.
2. **Upgrade to Pay As You Go** (Billing & Cost Management > Upgrade and Manage Payment).
   Always Free resources stay free. It matters because Oracle reclaims *idle* Always Free
   VMs (low CPU for 7 days), and this pipeline is idle most of the day; upgraded accounts are
   not reclaimed, and they usually find free Arm capacity more easily.
   Optional but recommended: set a budget alert of €1 (Billing > Budgets) to be told if
   anything ever costs money.
3. **Commit and push** all changes on the Mac, so the VM clones the current code.

## Step 1: create the VM (Oracle console)

Compute > Instances > **Create instance**:

- **Name:** `job-agent`
- **Image:** Canonical Ubuntu 24.04 (click *Change image*).
- **Shape:** *Change shape* > Ampere > `VM.Standard.A1.Flex`, **2 OCPUs, 12 GB memory**.
  It is marked *Always Free-eligible*; the free limit is 4 OCPUs / 24 GB in total.
- **Networking:** create a new virtual cloud network and public subnet, and keep
  *Assign a public IPv4 address* on.
- **SSH keys:** *Generate a key pair for me* and **download the private key**. It is the
  only way to log in.
- **Boot volume:** keep the default (about 47 GB; up to 200 GB is free).

If it says **"Out of capacity"**, try another availability domain in the same form, or try
again later (early morning often works). Pay As You Go accounts hit this less often.

Write down the **public IP address** from the instance page.

## Step 2: log in

**Mac** (replace the key file name and the IP):

```bash
mv ~/Downloads/ssh-key-*.key ~/.ssh/job-agent.key
chmod 600 ~/.ssh/job-agent.key
ssh -i ~/.ssh/job-agent.key ubuntu@PUBLIC_IP
```

Answer `yes` to the fingerprint question the first time.

## Step 3: set up the VM

**VM:**

```bash
sudo apt-get update && sudo apt-get install -y git
git clone https://github.com/QI-Zzz/job-search-assistant.git
bash job-search-assistant/deploy/oracle/setup_vm.sh
exit
```

This installs Docker, sets the clock to Amsterdam time, and builds the images (a few
minutes). `exit` logs out, so that Docker works without `sudo` at the next login.

## Step 4: copy the secrets and profile

**Mac:**

```bash
scp -i ~/.ssh/job-agent.key .env ubuntu@PUBLIC_IP:~/job-search-assistant/
scp -i ~/.ssh/job-agent.key -r profile ubuntu@PUBLIC_IP:~/job-search-assistant/
```

## Step 5: move the data (Saturday, after the Mac's 07:30 run)

Do this after the Saturday email has arrived, so no job is lost or sent twice.

1. **Mac:** stop the schedule on the Mac, then export the data.

   ```bash
   docker compose exec airflow-scheduler airflow dags pause daily_job_agent
   bash deploy/oracle/export_data.sh
   scp -i ~/.ssh/job-agent.key jobs_raw.dump ubuntu@PUBLIC_IP:~/job-search-assistant/
   ```

   Keep the row counts it prints.

2. **VM:** start the database and Airflow, then import.

   ```bash
   cd ~/job-search-assistant
   docker compose up -d postgres airflow-init airflow-webserver airflow-scheduler
   docker compose ps
   bash deploy/oracle/import_data.sh
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

From then on it runs Tuesday to Saturday at 07:30 Amsterdam time on the VM.

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
ssh -i ~/.ssh/job-agent.key -L 8080:localhost:8080 ubuntu@PUBLIC_IP
```

Then open <http://localhost:8080> while that terminal stays open. The VM's ports are not
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
scp -i ~/.ssh/job-agent.key profile/*.md ubuntu@PUBLIC_IP:~/job-search-assistant/profile/
```

**After a VM reboot** Docker and the containers start again by themselves (`restart:
unless-stopped`). To check: `docker compose ps` on the VM.

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `permission denied ... docker.sock` | Log out and in again after `setup_vm.sh`, or prefix the command with `sudo` |
| "Out of capacity" when creating the VM | Another availability domain, or retry later; Pay As You Go helps |
| `ssh` asks for a password or says "Permission denied (publickey)" | Use `-i ~/.ssh/job-agent.key` and user `ubuntu`; check `chmod 600` on the key |
| The VM disappeared or was stopped | Always Free idle reclaim; upgrade to Pay As You Go (step 0) |
