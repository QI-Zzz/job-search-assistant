"""Daily email: send the scored jobs not sent before, best fit first, and record what was sent.

Sends through Gmail SMTP with an app password (GMAIL_ADDRESS, GMAIL_APP_PASSWORD in .env;
EMAIL_TO defaults to GMAIL_ADDRESS). Runs from airflow/dags:
    python -m job_agent.daily_email [--preview PATH]
--preview writes the email as HTML to PATH instead of sending it, and records nothing.
"""
import argparse
import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape
from zoneinfo import ZoneInfo

from job_agent import config
from job_agent.db import get_connection

DUTCH_LABELS = {"required": "Dutch required", "nice_to_have": "Dutch a plus", "not_mentioned": "no Dutch mentioned"}
VISA_LABELS = {"offered": "visa sponsorship offered", "not_mentioned": "visa not mentioned"}
def jobs_to_send(conn):
    """Scored jobs from mart.job_ranking that were never sent, in ranking order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.job_id, r.title, r.company_name, r.location, r.job_url, r.years_required_min,
                   r.dutch_requirement, r.visa_sponsorship, r.tech_stack, r.company_summary, r.main_mode,
                   r.day_to_day, r.what_it_builds, r.watch_outs, r.fit_score, r.fit_reason, r.gaps
            FROM mart.job_ranking AS r
            WHERE NOT EXISTS (SELECT 1 FROM raw.sent_jobs AS s WHERE s.job_id = r.job_id)
            ORDER BY r.rank
            """
        )
        columns = [c.name for c in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


# title words that say where or for whom, not what the job is ("- Netherlands", "(m/f/d)")
TITLE_NOISE = {"m", "f", "d", "w", "x", "v", "all", "genders", "netherlands", "nl", "amsterdam", "utrecht",
               "emea", "europe", "benelux", "remote", "hybrid"}


def repost_key(company, title):
    """Company without punctuation, and the set of title words without bracketed bits and noise."""
    title = re.sub(r"\(.*?\)", " ", title or "")
    words = frozenset(re.split(r"\W+", title.lower())) - TITLE_NOISE - {""}
    return re.sub(r"\W+", " ", (company or "").lower()).strip(), words


def same_job(words, other):
    """Same company assumed. The same job when one title holds all the words of the other plus at
    most two more, e.g. "Analytics Engineer – Client Reporting" and "Client Reporting Analytics
    Engineer (dbt/Looker)". More extra words usually mean another job ("Data Analyst" vs "Data
    Insights Analyst: Self-Serve Dashboards & Impact"); a one-word title only matches exactly."""
    short, long = sorted((words, other), key=len)
    return short == long or (len(short) >= 2 and short <= long and len(long - short) <= 2)


def drop_reposts(conn, jobs):
    """Leave out jobs already sent under another job id in the last REPOST_WINDOW_DAYS days,
    and repeats of the same job within today's list (the best-ranked one is kept)."""
    since = datetime.now(timezone.utc) - timedelta(days=config.REPOST_WINDOW_DAYS)
    with conn.cursor() as cur:
        cur.execute("SELECT company_name, title FROM raw.sent_jobs WHERE sent_at >= %s", (since,))
        rows = cur.fetchall()
    seen = {}  # company -> title word sets already sent or kept today
    for company, title in rows:
        company_key, words = repost_key(company, title)
        seen.setdefault(company_key, []).append(words)
    kept = []
    for job in jobs:
        company_key, words = repost_key(job["company_name"], job["title"])
        if any(same_job(words, other) for other in seen.get(company_key, [])):
            print(f"skipping repost: {job['title']} at {job['company_name']}")
            continue
        seen.setdefault(company_key, []).append(words)
        kept.append(job)
    return kept


def facts_line(job):
    facts = [job["location"]]
    if job["years_required_min"] is not None:
        facts.append(f"{job['years_required_min']}+ years")
    facts.append(DUTCH_LABELS.get(job["dutch_requirement"], job["dutch_requirement"]))
    facts.append(VISA_LABELS.get(job["visa_sponsorship"]))
    if job["tech_stack"]:
        facts.append(", ".join(job["tech_stack"][:8]))
    return " · ".join(f for f in facts if f)


def bullet_list_html(items):
    return "".join(f"<li>{escape(i)}</li>" for i in items or [])


def render_job_html(position, job):
    gaps = bullet_list_html(job["gaps"])
    watch_outs = bullet_list_html(job["watch_outs"])
    mode = f" (mostly {escape(job['main_mode'])})" if job["main_mode"] else ""
    return f"""
<div style="border:1px solid #ddd;border-radius:8px;padding:14px 16px;margin:0 0 14px">
  <div style="font-size:16px;font-weight:600">{position}. <a href="{escape(job['job_url'] or '')}" style="color:#1a56db;text-decoration:none">{escape(job['title'])}</a></div>
  <div style="color:#444;margin:2px 0 8px">{escape(job['company_name'] or '')}</div>
  <div style="color:#666;font-size:13px;margin-bottom:10px">{escape(facts_line(job))}</div>
  {f'<div style="margin-bottom:6px"><b>The company</b>: {escape(job["company_summary"])}</div>' if job["company_summary"] else ''}
  {f'<div style="margin-bottom:6px"><b>The job{mode}</b>: {escape(job["day_to_day"])}</div>' if job["day_to_day"] else ''}
  {f'<div style="margin-bottom:6px"><b>What it builds for you</b>: {escape(job["what_it_builds"])}</div>' if job["what_it_builds"] else ''}
  {f'<div style="margin-bottom:6px"><b>Watch-outs</b><ul style="margin:4px 0">{watch_outs}</ul></div>' if watch_outs else ''}
  <div style="margin:10px 0 6px"><b>Fit {job['fit_score']:.0f}/10</b>: {escape(job['fit_reason'] or '')}</div>
  {f'<div><b>Gaps</b><ul style="margin:4px 0">{gaps}</ul></div>' if gaps else ''}
</div>"""


def render_job_text(position, job):
    mode = f" (mostly {job['main_mode']})" if job["main_mode"] else ""
    lines = [f"{position}. {job['title']} at {job['company_name']}", f"   {job['job_url']}",
             f"   {facts_line(job)}",
             f"   The company: {job['company_summary']}",
             f"   The job{mode}: {job['day_to_day']}",
             f"   What it builds for you: {job['what_it_builds']}"]
    lines += [f"   - watch out: {w}" for w in job["watch_outs"] or []]
    lines.append(f"   Fit {job['fit_score']:.0f}/10: {job['fit_reason']}")
    lines += [f"   - gap: {g}" for g in job["gaps"] or []]
    return "\n".join(lines)


def build_message(jobs, today):
    day = today.strftime("%a %-d %b")
    message = EmailMessage()
    if not jobs:
        message["Subject"] = f"Job agent: no new jobs, {day}"
        message.set_content("The pipeline ran this morning; no new job passed all the filters today.")
        return message
    message["Subject"] = f"Job agent: {len(jobs)} new job{'s' if len(jobs) != 1 else ''}, {day}"
    intro = "Best fit first. Fit: could you get it? Watch-outs are judged against your job matching framework."
    message.set_content(intro + "\n\n" + "\n\n".join(render_job_text(i, j) for i, j in enumerate(jobs, 1)))
    cards = "".join(render_job_html(i, j) for i, j in enumerate(jobs, 1))
    message.add_alternative(
        f"""<html><body style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px;color:#222;max-width:720px">
<p style="color:#666">{intro}</p>{cards}</body></html>""",
        subtype="html",
    )
    return message


def send(message):
    sender = os.environ["GMAIL_ADDRESS"]
    message["From"] = sender
    message["To"] = os.getenv("EMAIL_TO") or sender
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        # Google shows app passwords in groups of four ("abcd efgh ..."); the spaces are not part of it
        smtp.login(sender, os.environ["GMAIL_APP_PASSWORD"].replace(" ", ""))
        smtp.send_message(message)


def record_sent(conn, jobs, today):
    sent_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for position, job in enumerate(jobs, 1):
            cur.execute(
                """
                INSERT INTO raw.sent_jobs (job_id, title, company_name, email_date, position, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
                """,
                (job["job_id"], job["title"], job["company_name"], today, position, sent_at),
            )
    conn.commit()


def main(preview=None):
    today = datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    conn = get_connection()
    try:
        jobs = drop_reposts(conn, jobs_to_send(conn))
        message = build_message(jobs, today)
        if preview:
            html = message.get_body(("html", "plain")).get_content()
            with open(preview, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"preview: {message['Subject']} -> {preview} (not sent, nothing recorded)")
            return
        send(message)
        # recorded only after a successful send, so a failed send is retried the next day
        record_sent(conn, jobs, today)
        print(f"sent: {message['Subject']}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", help="write the email as HTML to this path instead of sending it")
    main(parser.parse_args().preview)
