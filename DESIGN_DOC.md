# Personal Job Agent — Design Doc

Last updated: 2026-10-31

## Purpose

On each working morning, find the newly posted LinkedIn jobs that are worth Amy's time and email her a short list, with the details and reasons needed to decide whether to apply.

Each job answers three questions, in order:

1. **Can I apply?** Eligibility: hard rules, no judgement.
2. **Is it good for my career?** Career value: the rubric below.
3. **Can I get it?** Fit: Amy's background against the job's requirements.

**In scope:** daily fetch via Apify, filtering, LLM extraction and scoring, a daily email, and later a feedback loop.

**Out of scope:** job-market trend data. That is a separate project. The Chrome extension and the manual save endpoint were removed for this reason.

## Current phase: testing pass 2

Steps 1–4 (search, free rules, pass 1, eligibility) were refined first, because they decide both the cost and whether good jobs reach Amy at all. After the review of the Sat 2026-10-31 run, pass 2 was switched on (`RUN_PASS_2 = True` in `config.py`). No email is sent yet. See [Test plan](#test-plan).

## Design principles

- **Fixed pipeline, one smart step.** The flow never changes; the LLM only extracts and judges. Predictable, cheap, testable.
- **Cheap before expensive.** Free rules run before any LLM call; a cheap model runs before the expensive one.
- **Search wide, filter cheaply.** LinkedIn's keyword search is loose and only returns a sample per search, so the search is broad and relevance is decided later by free rules and pass 1, not by job titles.
- **The LLM judges, code decides.** The LLM returns facts, sub-scores and reasons. Weights, thresholds and selection are deterministic code.
- **Raw stays raw.** Apify output and LLM responses are stored exactly as received.
- **Every score is traceable.** Each score records the model, the prompt version and (for fit) the profile version.
- **Stateless calls.** One independent LLM call per job; no memory between jobs.
- **Fail one, not all.** A failed search or an invalid LLM response is recorded; the run continues.
- **Measure, then decide.** Keyword choices and rules are checked against real data and Amy's review, not guessed.

## Stack

| Part | Tool |
| --- | --- |
| Orchestration | Airflow 2.8.1 (Docker Compose), on Amy's Mac for now |
| Transformations | dbt 1.12, triggered from Airflow (own virtualenv) |
| Storage | PostgreSQL `jobs_db`: `raw`, `staging`, `mart` schemas |
| Job source | Apify actor `valig/linkedin-jobs-scraper`: public LinkedIn job search, no LinkedIn login, so Amy's account is never used |
| LLM | Anthropic: Haiku 5.5 for pass 1, Sonnet 5.5 (effort low) for pass 2; own virtualenv, because the SDK clashes with Airflow's package pins |
| LLM output validation | Pydantic (structured outputs) |
| Feedback endpoint (v2) | FastAPI |

## Daily pipeline

Runs **Tuesday to Saturday at 07:30** (Amsterdam). Sunday and Monday mornings only cover weekend postings, which are rarely worth checking.

```
Step  Task                       What it does                                        Tool                 Status
1     fetch_jobs                 one Apify search per keyword, last 24 hours         Python + Apify       on
2     dbt_run_prefilter          normalize + free rules on title/location/fields    dbt                  on
3     extract_job_facts          pass 1: read the description, extract facts        Python + Haiku       on
4     dbt_run_eligibility        rules on the extracted facts; keyword_stats        dbt                  on
5     score_jobs                 pass 2: career value + fit                          Python + Sonnet      on
6     dbt_run_ranking            weighted scores, selection                          dbt                  on
7     email                      send the selected jobs, record what was sent       Python               not built
```

### 1. Search

- **One Apify run per keyword**, location "Amsterdam, North Holland, Netherlands", posted in the past 24 hours (`r86400`), limit 1,000 per keyword, 15-minute timeout per run (`RUN_TIMEOUT_SECS`; the actor's default of 5 minutes cut off broad keywords). Runs go one at a time; each passes the job ids fetched in the last 3 days, including by earlier keywords that morning, as `skipJobId`, so Apify neither returns nor charges for jobs already found. One failed keyword does not lose the others, but the task is marked failed so the gap is visible.
- **Keyword list:** 13 keywords in `airflow/dags/job_agent/config.py` (since 2026-10-30), cut from the 135-keyword test list:
  - broad: python, data, analyst, developer, consultant, analytics
  - data and analytics (the cleanest keywords, 18–24% of results end up eligible): data engineer, analytics engineer, business intelligence, data analyst
  - business analysis and consulting: analytics consultant, implementation consultant, technical business analyst
  - no presales keyword: "presales engineer" was dropped after Amy's review, because the presales jobs only it found were hardware, clinical, food and security roles. The presales jobs worth considering (ICEYE, KPMG, Boomi, Dash0) are also found by the broad keywords.

  Together they found every eligible data, analytics and consulting job on Thu 29 and Fri 30. Estimated ~3,800 results and ~$1.70 per run, against $13 for 135 keywords.
- Every keyword's results land unchanged in `raw.apify_jobs` (one row per job per run, with the keyword, run ID and fetch time), so overlap between keywords can be measured and keywords merged later. `mart.keyword_stats` summarises what each keyword adds per day.

**What we learned about LinkedIn search (2026-10-28/29):**

- **Keywords match loosely**, not as exact titles: "data" finds "Supply Planner" and "NL Technology"; "presales engineer" mostly finds sales roles (159 jobs, none with fit ≥ 5).
- **Each search returns a sample, not everything.** A search with no keyword stopped by itself at 326 unique jobs, while a same-day "data analyst" search found 149 jobs it missed. So a no-keyword search cannot replace keywords.
- **LinkedIn stops around 500 results per search.** In the 30-keyword run the largest were "consultant" (493), "analyst" (486) and "insights" (410); most returned 60–200. The 1,000 limit is never reached.
- **Keywords overlap heavily.** 30 keywords found 1,325 unique jobs; 140 passed the free rules, and only 21 of those were found by a single keyword. Ten keywords covered all 140: python, analyst, technology consultant, presales, consultant, analytics, developer, technical consultant, backend engineer, data.
- **LinkedIn's job-function filter** was considered and rejected: functions are chosen by whoever posts the job, so they are inconsistent across companies and agencies.

**135-keyword test (Fri 2026-10-30):** 130 of 135 keywords finished (analytics, consultant, reporting analyst, planning analyst and automation engineer hit the 5-minute timeout, since raised). 28,612 results, 3,583 unique jobs, 245 passed the free rules. Cost **$13.03** and 68 minutes. The 31 earlier keywords found 190 of the 245; the 55 found only by new keywords were almost all off-target (retail client advisors, account executives, QA, logistics), with about 3 relevant. Ten keywords covered 218 of 245 using 14% of the results: python, technology consultant, sales consultant, technical business analyst, operations analyst, ELT developer, analyst, technical sales engineer, developer, solutions operations.

### 2. Free rules (dbt, `jobs_prefiltered`)

Rules on fields Apify already returns; no LLM, no cost. Each dropped job keeps a `drop_reason`; jobs with no reason go to pass 1. Patterns live under `vars.prefilter` in `dbt_project.yml`.

| Rule | Field |
| --- | --- |
| Talent pool, not a vacancy ("talent pool", "candidate pool", "talent program", "open application") | title |
| Excluded company: Coolblue, bol, MediaMarkt and the Big Four (Deloitte, PwC, EY, KPMG) | company |
| LinkedIn level Director, Executive or Internship ("Mid-Senior" is not used: most jobs say "Not Applicable") | `experience_level` |
| LinkedIn contract type Internship, Volunteer, Part-time, Contract or Temporary (fixed-term employee jobs are listed as Full-time and are kept) | `contract_type` |
| Lead, leader, head, manager, managing, principal, director, chief, VP, vice president or supervisor | title |
| Senior, Sr or Staff | title |
| Intern, trainee, graduate, afstudeer(stage), onderzoeksstage, or a year such as "2027" | title |
| Academic: PhD, postdoc, postdoctoral, lecturer, professor, docent, hoogleraar | title |
| Freelance, ZZP or interim | title |
| "Freelance" or "ZZP" in the description (not "freelancers", which agencies use in boilerplate); also catches AI-training gig work | description |
| Asks for another language: "German-speaking", "Dutch speaker", "(French)" or a trailing "- Polish"; Mandarin and Chinese are kept on purpose | title |
| Outside Amsterdam, Amstelveen, Hoofddorp, Schiphol, Utrecht (city and province) and the Randstad; country-only and remote locations are kept | location |
| Description written in Dutch (more Dutch than English function words) | description |

### 3. Pass 1: extract facts (Haiku, `job_agent/extract.py`)

- A cheap model reads the full description and only extracts facts: posting language, minimum years required, Dutch required / nice to have / not mentioned, whether the role leads a team, the tech stack, and short quotes as evidence.
- Results land in `raw.job_extractions` (one row per job per prompt version). Jobs already extracted for the current prompt version are never sent again; failed ones are retried on the next run.
- **Role family (since prompt v2; current prompt v4, 2026-10-30).** Pass 1 also labels each job with one role family, judged from the responsibilities rather than the title, plus a one-sentence reason. From v3 each family lists what does NOT belong in it (merchandising, planning and research are not data analytics; DevOps, SRE and full-stack are not data engineering; onboarding, audit and HR are not BA/consulting; presales only for software, data, cloud or AI products). v4 adds marketing campaign roles to `other`, GTM / revenue-operations engineering to data engineering, and falls back to the title when a posting has no responsibilities. Unwanted families are dropped in eligibility, before Sonnet. The keep list is `vars.eligibility.keep_role_families` in `dbt_project.yml`:

| Keep | Drop |
| --- | --- |
| `data_analytics` | `sales_account_management` |
| `data_ml_ai_engineering` | `finance_controlling` |
| `business_analysis_consulting` | `software_engineering` (dropped since 2026-10-30) |
| | `presales_solutions` (dropped since 2026-10-30) |
| | `operations_supply_chain` |
| | `it_infrastructure_security` (IT support, networks, security, field service) |
| | `other` (marketing execution, design, HR, legal, clinical, hardware, retail, AI-training gig work…) |

First run (Fri 2026-10-30): of 245 free-rule survivors, eligible jobs fell from 166 to **72**. Role family dropped 92: IT/infrastructure/security 28, sales 24, other 19, operations 11, finance 10. **v3/v4 results (2026-10-30):** eligible jobs fell from 78 to 65; about 15 of the 65 are still off-target (for example consumer research, demand planning, procurement and paid social labelled data analytics; hardware and security vendors labelled presales). Haiku does not follow the NOT lists reliably, and the same job can change family between runs. Further prompt tuning has diminishing returns; pass 2 is expected to filter the rest by career value and fit.

**Software engineering moved to the drop list (2026-10-30):** Amy reviewed the 25 eligible software jobs (mostly mobile, frontend and Java/PHP full stack) and decided her CV and experience don't fit software roles. Eligible jobs over Wed–Fri, by family: data/ML/AI engineering 38, data and analytics 24, BA and consulting 23, presales 14. Other edge cases to review: DevOps and SRE roles were labelled IT infrastructure (dropped) although the prompt lists them under software engineering; a Security Engineer was labelled once as IT and once as data/ML engineering; a Pricing Analyst (L'Oréal) and a Logistics Systems & Implementation Analyst were dropped as finance and operations.

### 4. Eligibility (dbt, `jobs_eligibility`)

Rules on the pass 1 facts; thresholds under `vars.eligibility` in `dbt_project.yml`. A job is dropped if:

- it was not extracted yet;
- the posting is not in English (this also catches German postings);
- Dutch is required ("nice to have" is kept);
- more than 5 years are required (5 is kept);
- the role leads a team;
- its role family is not on the keep list (`'role family: …'`); extractions from before v2 have none (`'no role family yet'`).

### 5. Scoring (pass 2)

Two separate scores, both returned as validated JSON.

#### Career value: "Is it good for my career?"

Four core dimensions, each 1–10:

| Dimension | Weight | Assesses |
| --- | --- | --- |
| Salary ceiling | 30% | Long-term earning potential, salary progression, access to high-paying employers and markets, paths into leadership or other lucrative tracks |
| Geographic mobility | 25% | International demand, cross-border skill recognition, remote work, feasibility of working in the Netherlands, UAE, Singapore or the US |
| Skill transferability | 25% | Whether the skills and experience transfer across industries, companies, functions and countries, including pivots into other careers |
| Low lock-in risk | 20% | Dependence on local regulations, niche industry knowledge, proprietary systems, country-specific qualifications or a narrow track. **Higher score = lower risk.** |

**Technical advantage** is a separate bonus, not a gatekeeper:

- Non-technical roles (strategy, commercial, operations, product, consulting, management) can score highly.
- A technical job is not automatically good if it has poor salary prospects, limited mobility or high lock-in.
- Technical depth, AI exposure, coding and data skills improve a rating when they create additional career options.
- A role is not favoured just because it involves more coding.

**Tiers**, based on actual responsibilities and likely career outcomes, never on the title alone:

| Tier | Meaning |
| --- | --- |
| 1 — Excellent | Strong earning potential, international mobility, transferable skills, low lock-in |
| 2 — Good | Attractive overall, with one meaningful compromise |
| 3 — Conditional | Limited upside in one or more important dimensions; only if the trade-offs are justified |
| 4 — Poor | Several serious constraints on earning potential, mobility or career freedom |

Career value does not use the CV, so it does not need recomputing when the profile changes.

#### Fit: "Can I get it?"

Fit means how well the job matches Amy's background. It compares the job with the candidate profile (`profile/candidate_profile.md`, built from her two CVs and kept out of git because the repo is public).

Since prompt v2 (2026-10-31) fit is **one overall score from 1 to 10**, judged the way a recruiter would read her CV against the posting: skills and tools, level of experience (about 2 years), how close the work is to what she has done, and any hard blocker (language, degree, certification) are weighed together. 2 = no interview; 5 = a stretch worth applying to; 9 = a strong match. It returns a one-sentence reason, 2–4 match reasons (written to Amy) and the main gaps.

v1 had four weighted dimensions (skills 35%, experience level 25%, role and domain 25%, other must-haves 15%); Amy found the split unhelpful, and a fixed weighting cannot say that one missing requirement outweighs everything else.

#### How pass 2 runs (`job_agent/score.py`)

- Two separate calls per job. Career value runs first and never sees the CV; fit then runs for every eligible job (before 2026-10-31 it was skipped for Tier 4).
- Only jobs first fetched in the last 2 days are scored (`SCORE_RECENT_DAYS` in `config.py`), so a new prompt version re-scores recent jobs only, not every job ever found. Jobs already scored with the current versions are skipped, so a normal day scores only that day's new eligible jobs (about 20).
- The LLM returns the sub-scores (each with a one-sentence reason), the tier and the technical-advantage bonus. Anchors for 2, 5 and 9 are in the prompts.
- The rubric and profile sit in a cached system prompt. Results land in `raw.job_scores`; the profile version is a hash of the profile file.
- `mart.job_ranking` computes the weighted scores and `overall = (career value + fit) / 2`, and orders the jobs by it (technical advantage only breaks ties). It lists only jobs first fetched in the last 2 days (`vars.ranking.recent_days`). There is no cut: every scored job goes in the email, with both scores, so Amy decides. The weights are vars in `dbt_project.yml`.

**First run (2026-10-29):** 73 eligible jobs, no failures; tiers 9 / 33 / 30 / 1; 15 selected. Sales and clinical roles scored Tier 2–3 on career value but fit 1–3, so none were selected. Known issues seen: a graduate programme ("SDE – 2027", now caught by the free rules), a talent pool (now caught), and one job posted by two agencies.

### 6. Email (not built)

- **Each job in the email shows:** title, company, location, link, years required, tech stack, tier, career value score, fit score and several reasons why it matches.
- What was sent, and when, is recorded; the feedback loop depends on it. Jobs already sent are never sent again.

### Candidate profile

- `profile/candidate_profile.md`, written by hand from the two CVs (Data Analyst and Analytics / Data Engineer versions). Amy edits it when her CV changes.
- Its content hash is the profile version; fit scores record which version they used.

## Test plan

### Test runs

| When | Run | Purpose |
| --- | --- | --- |
| Thu 2026-10-29 (done) | No-keyword search + same-day "data analyst" search | Real volume; whether a no-keyword search covers everything (it does not) |
| Thu 2026-10-29 (done) | 30 keywords, by hand | Limits, cost, overlap, free-rule survivors |
| Fri 2026-10-30, 07:30 (done) | Scheduled, all 135 keywords | Fetch failed on 5 timeouts; the other steps were run by hand. See the 135-keyword test under Search |
| Fri 2026-10-30 (done) | Pass 1 v2 on all 343 free-rule survivors, by hand | Role family labels and the new eligibility drop |
| Sat 2026-10-31, 07:30 (done) | Scheduled, the new 13-keyword list | 1,212 new jobs, 122 past the free rules, 26 eligible, no timeouts; FERO's Junior Data Analyst came through as data_analytics. About 6 of the 26 were off-target |
| Sat 2026-10-31 (done) | Pass 2 on every eligible job, by hand | 52 new jobs scored, 0 failures; 78 ranked, 29 meet the bar, 15 selected. The off-target jobs Haiku let through (secretarial, aviation finance, tender, SAP, research) all fall below the fit bar. Misses: a Michael Page procurement job was selected, and duplicate postings are ranked twice |
| **Tue 2026-11-03 onward** | Scheduled, Tuesday to Saturday, with pass 2 | Daily review of scores and selection |

The Mac must be awake with Docker running at 07:30 (`caffeinate -ims -t 36000` the evening before, plugged in, lid open).

### How each step is checked

| Step | Question | Check |
| --- | --- | --- |
| 1 Search | Are we finding the jobs Amy would want? | `keyword_stats` per day (jobs found, found only by this keyword, survivors, survivors only by this keyword) and a greedy coverage table (how many keywords are needed to cover all survivors). Amy notes interesting jobs she sees on LinkedIn herself; we check whether the pipeline found them and which keyword would have. |
| 2 Free rules | Are we dropping only jobs Amy would never want? | A sample of about 10 dropped jobs per rule; Amy marks each "right to drop" or "wrong". Rules with wrong drops are fixed. |
| 3 Pass 1 | Are the extracted facts correct? | Evidence quotes checked against the posting; Amy reviews about 30 jobs (role family, Dutch, years). Those become a test set that is re-run whenever the prompt changes. |
| 4 Eligibility | Do the rules match Amy's intent? | A sample per drop reason, plus borderline cases (exactly 5 years, Dutch "preferred"). |

**Keyword pruning:** after several days, keywords that add no surviving job are merged or dropped. Once the list is stable, the actor's "skip job IDs" input can stop paying for jobs already fetched by an earlier keyword.

**Daily monitoring (planned):** a `pipeline_daily_stats` model with one row per day: jobs after each step, drops per reason, cost per step.

## Data model

Convention: **Python tasks write to `raw`; dbt owns `staging` and `mart`.** Every Python output is a dbt source.

| Table | Schema | Written by | Grain: one row per… |
| --- | --- | --- | --- |
| `apify_jobs` | raw | Python | job as returned by one Apify run (JSON as received, with keyword) |
| `job_extractions` | raw | Python | job × extraction prompt version |
| `job_scores` | raw | Python | job × score type (career value / fit) × prompt version × profile version |
| `stg_jobs` | staging | dbt | unique job (latest payload, first/last seen, all keywords that found it) |
| `jobs_prefiltered` | staging | dbt | job, with a free-rule drop reason |
| `jobs_eligibility` | staging | dbt | job that passed the free rules, with pass 1 facts and a drop reason |
| `job_ranking` | mart | dbt | scored job, with weighted scores, rank and `selected` |
| `keyword_stats` | mart | dbt | fetch day × keyword |
| `sent_jobs` (planned) | raw | Python | job × email sent |
| `feedback` (v2) | raw | FastAPI | reaction submitted |
| `volume_test` (temporary) | sandbox | by hand | job from the no-keyword test; drop when done |

## Cost

Measured at Anthropic and Apify list prices, 2026-10:

| Step | Unit cost | Measured |
| --- | --- | --- |
| Apify search | $0.0004 per returned result + ~$0.001 per run | 30 keywords: $2.36 for ~6,000 results. 135 keywords: **$13.03** for 28,612 results (130 finished), ~$65 per 5-day week |
| Pass 1 (Haiku) | ~$0.0004 per job | 103 jobs (v1): ~$0.03; 343 jobs (v2): ~$0.15 |
| Pass 2 (Sonnet) | ~$0.016 per job (both calls) | 73 jobs: ~$1.20 |

Apify is the largest cost during the keyword test; it drops once the keyword list is pruned. Pass 2 cost depends on how many jobs reach it, which the role-family filter reduces. Options if pass 2 is too expensive: Haiku for career value, or the Batch API (50% off).

## Decision log

| Date | Decision | Why |
| --- | --- | --- |
| 2026-10-26 | Drop the Chrome extension and trend data; build a fixed daily pipeline with Airflow + dbt | Trends become a separate project; the extension was never used |
| 2026-10-26 | Two-pass LLM: cheap extraction, then scoring of survivors only | Most jobs fail eligibility; scoring every job is several times more expensive |
| 2026-10-26 | Two separate scores: career value (Amy's rubric) and fit | They answer different questions; career value does not depend on the CV |
| 2026-10-28 | Apify actor `valig/linkedin-jobs-scraper` | Full descriptions, stable job IDs, no LinkedIn login |
| 2026-10-28 | Location: Amsterdam region plus Utrecht (city and province) | Commutable from Amsterdam |
| 2026-10-28 | Drop Dutch-language postings; drop senior, lead/manager, intern and freelance titles | Amy has no Dutch; second year of career |
| 2026-10-29 | Anthropic as LLM provider (Haiku 5.5 for pass 1, Sonnet 5.5 for pass 2) | — |
| 2026-10-29 | Keep "managing" in the lead rule; keep Haarlem, Diemen and Almere | Amy kept the current rules over a pasted variant |
| 2026-10-29 | Fit = match with Amy's background; four dimensions and weights as above | Built from the two CVs; LinkedIn profile not readable without login |
| 2026-10-29 | Pass 2 defaults: two calls, LLM assigns the tier, average of the two scores, tier ≤ 3 and fit ≥ 5, max 15, technical advantage as tie-break | Simple v1; tune after feedback |
| 2026-10-29 | Role families: keep data/analytics, data/ML/AI engineering, software engineering, BA/consulting, technical presales; drop finance/controlling, operations/supply chain, pure sales, unrelated | Amy's scope: data and somewhat technical |
| 2026-10-29 | Search wide with keywords and filter by role family in pass 1; no title-topic filter | Titles are too varied; LinkedIn returns samples, so no single search covers everything |
| 2026-10-29 | Rejected: no-keyword search, one-week fetch, LinkedIn job-function filter | Samples only; a week window distorts overlap; functions are set inconsistently by posters |
| 2026-10-29 | Comprehensive keyword list (135), every keyword's results saved; prune later with data | Missing roles is Amy's main worry |
| 2026-10-29 | Drop Part-time, Internship and Volunteer contracts; keep Contract; keep "Randstad" locations | Amy's choice |
| 2026-10-29 | Location narrowed to Amsterdam, Amstelveen, Hoofddorp, Schiphol, Utrecht and the Randstad (Haarlem, Diemen, Zaandam, Almere and Hilversum removed) | Amy's edit; replaces the earlier choice to keep Haarlem, Diemen and Almere |
| 2026-10-29 | Schedule Tuesday to Saturday 07:30, first run Friday 2026-10-30; pass 2 off during refinement | Weekend postings are rarely worth checking; refine steps 1–4 first |
| 2026-10-31 | Pass 2 scores, and the ranking lists, only jobs first fetched in the last 2 days | Changing the fit prompt to v2 re-scored all 78 eligible jobs ever found; over months that would be thousands of jobs per prompt change. The email only needs recent jobs |
| 2026-10-31 | Keywords run one after another; each run passes the jobs fetched in the last 3 days (and by earlier keywords today) as `skipJobId` | 64% of Saturday's results were jobs another keyword had already found, and Apify charges per returned result. Tested: with Saturday's 1,268 job ids skipped, "data analyst" returned 1 job ($0.001) instead of 240 ($0.10). Expected ~$0.55 per run instead of ~$1.60; the fetch takes longer (one run at a time). `keyword_stats` now shows what each keyword adds after the ones before it |
| 2026-10-31 | Fit prompt v2: one overall score instead of four weighted dimensions | Amy's choice: the four-way split was not useful to her |
| 2026-10-31 | No selection cut: every eligible job is scored (career value and fit) and sent with both scores | Amy's choice: only about 20 jobs a day reach pass 2, so a tier / fit bar and a top-15 cap hide jobs she may want to judge herself. Replaces the tier ≤ 3, fit ≥ 5, top-15 selection |
| 2026-10-31 | Switch pass 2 on in the daily run | Review of steps 1–4 on Saturday's run: 26 eligible, about 6 off-target. Haiku label noise is left for pass 2 to judge (about $0.40 a day) instead of more prompt tuning |
| 2026-10-31 | Free rules for academic titles (lecturer, professor, postdoctoral, …) and "talent program" | Saturday's eligible jobs included a lecturer, a postdoc and three Mploy Associates traineeship listings. Drops 14 academic and 17 talent-pool jobs fetched so far |
| 2026-10-30 | Apify run timeout raised from 5 to 15 minutes | Five broad keywords timed out in the first scheduled run |
| 2026-10-30 | Keyword list cut from 135 to 14 | A typical eligible job was found by 31 keywords; only 1 of 130 keywords found an eligible job no other keyword found; 6–9 keywords covered every eligible job each day. Cuts Apify from ~$13 to ~$1.80 per run |
| 2026-10-30 | Drop the presales role family | Amy's choice: few presales jobs she would apply to. Loses ICEYE's Sales Engineer (data products); 57 eligible jobs remain |
| 2026-10-30 | Role family prompt v3 then v4: each family lists what does not belong; presales only for software, data, cloud or AI products | Amy's review found about half the eligible jobs off-target. Eligible 78 → 65; about 15 still off-target because Haiku labels are noisy. Stop tuning here and let pass 2 judge the rest |
| 2026-10-30 | Drop LinkedIn "Contract" and "Temporary" jobs; keep fixed-term employee jobs | Amy's choice: Contract jobs were mostly freelance or day-rate agency work (15 of 93 eligible). Replaces the 2026-10-29 choice to keep Contract. 78 eligible jobs remain |
| 2026-10-30 | Free-rule gaps closed: leader, supervisor, vice president; "… speaker", "(French)", trailing "- Polish"; freelance in the description | Seen in Friday's survivors. Dropped 33 more survivors (16 of them Alignerr gig postings); 93 eligible jobs remain |
| 2026-10-30 | Exclude Coolblue, bol, MediaMarkt and the Big Four (Deloitte, PwC, EY, KPMG) as a free rule | Amy's choice. Dropped 62 jobs fetched so far, 3 of them eligible (including the KPMG AI pre-sales architect) |
| 2026-10-30 | Drop the "presales engineer" keyword (13 keywords left); keep the presales role family | Amy's review of the 14 eligible presales jobs: only ICEYE and KPMG are worth applying to, and the broad keywords find them; the 7 found only by this keyword were hardware, clinical, food and security roles |
| 2026-10-30 | Drop the software engineering role family | Amy's review: her CV and experience don't fit software roles (mostly mobile, frontend, Java/PHP full stack) |
| 2026-10-30 | Role family added to pass 1 (prompt v2) with ten families; keep list in dbt vars; IT infrastructure/security dropped | Eligible jobs still included sales, field service and retail roles; a keep list means any new or unexpected family is dropped by default |

## Open decisions

| Blocks | Decision | Notes |
| --- | --- | --- |
| Refinement | Is the 13-keyword list enough? | Chosen from two days only; each search is a sample, so recheck coverage after a week. Amy notes good jobs she sees on LinkedIn herself, to test for misses |
| Refinement | Review format for samples | CSV to mark in Excel / Sheets, or a small private review page with ✓ / ✗ buttons |
| Refinement | Haiku label noise | If pass 2 does not filter the remaining off-target jobs, options: Haiku with thinking on for the role family, or move the role family into pass 2 (Sonnet) |
| Refinement | Role family edge cases | Whether DevOps/SRE and security engineering count as software engineering; pricing and procurement analysts; solution architects in delivery (not presales) |
| Scoring | Anchors for every score | Draft anchors for 2 / 5 / 9 are in the prompts; review against real results |
| Scoring | Skill transferability vs lock-in risk | They overlap; state the difference in one sentence, or merge them |
| Scoring | Management roles | The rubric allows them to score well; eligibility drops team leads |
| Selection | How fit and career value combine | v1: average of the two, tier ≤ 3 and fit ≥ 5; tune after feedback |
| Selection | Tier rules | v1: the LLM assigns the tier from the rubric; move to code rules if it drifts |
| Email | Repeat and reposted jobs | Proposal: each job is emailed at most once (`sent_jobs` by LinkedIn job ID); the email only takes jobs first fetched in today's run; a repost (same company and similar title within ~30 days, new job ID) is skipped or marked "reposted". Today `job_ranking` ranks every scored job ever, so this must be built with the email |
| Later | Same job posted by several agencies | Needs fuzzy matching on company + title |
| Later | Where the pipeline runs | Mac with `caffeinate` for now; an always-on machine for daily use |
| Later | Search beyond Amsterdam | UAE, Singapore and US jobs would need visa or sponsorship rules |
| Later | Zero-match days | Send nothing, or a "nothing today" email |
| Later | Profile changes | Rescore recent jobs for fit, or apply to new jobs only |

## Phasing

**v1: working daily feed**

- [x] Apify fetch into `raw.apify_jobs`
- [x] Normalize into `stg_jobs` (one row per job)
- [x] Free rules in dbt (`jobs_prefiltered`), with a drop reason per job
- [x] Pass 1 extraction and eligibility rules
- [x] Candidate profile, versioned by content hash
- [x] Career value and fit scoring, with validation and raw-response storage
- [x] Weighted score and selection in dbt (`job_ranking`)
- [x] Keyword list (135) and per-keyword stats (`keyword_stats`)
- [x] Airflow DAG scheduled Tuesday to Saturday at 07:30 (steps 1–4)
- [ ] **Refine steps 1–4** (current): keyword test and pruning, free-rule review, role family in pass 1, eligibility review, daily stats
- [ ] Switch pass 2 back on
- [ ] Email delivery and a record of what was sent

**v2: feedback**

- [ ] FastAPI feedback endpoint and links in the email
- [ ] Feedback labels: applied / interested / not relevant (with an optional reason)
- [ ] Precision metric: the share of sent jobs that were relevant
- [ ] Tune weights, cut-offs and selection

**v3: learning**

- [ ] Good and bad examples fed back into the prompts
- [ ] Occasionally include a lower-scored job, to check for missed good ones (selection bias)
- [ ] Possibly: look up company information to judge lock-in risk better

## Setup notes

- Init scripts in `db/` run only on an empty Postgres volume, connected to `POSTGRES_DB` (`jobs_db`). New table files start with `\c jobs_db`; existing databases get new tables by running the script with `psql` (all statements use `IF NOT EXISTS`).
- dbt is mounted in Airflow at `/opt/airflow/dbt_project`; `DBT_PROFILES_DIR` points there. `macros/generate_schema_name.sql` makes models land in `staging` and `mart`, not `staging_mart`.
- The profile is mounted read-only at `/opt/airflow/profile`.
- `.env` holds `APIFY_TOKEN` and `ANTHROPIC_API_KEY` (gitignored). Restart Airflow after changing it.
- `airflow/dags/.airflowignore` keeps the `job_agent` helper package out of DAG parsing.
- Run steps by hand from `/opt/airflow/dags` in the scheduler container: `python -m job_agent.fetch`; `/home/airflow/llm_venv/bin/python -m job_agent.extract [--limit N]`; `/home/airflow/llm_venv/bin/python -m job_agent.score [--limit N]`.
