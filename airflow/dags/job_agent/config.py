# Search settings for the daily fetch. Credentials come from .env, not here.

APIFY_ACTOR_ID = "valig/linkedin-jobs-scraper"

# One Apify run per keyword. LinkedIn matches keywords loosely and only returns a sample
# per search, so a few broad keywords plus a few clean specific ones find nearly every
# eligible job. Chosen 2026-10-30 from the 135-keyword test: these 13 found every eligible
# data, analytics and consulting job on Thu 29 and Fri 30. mart.keyword_stats tracks what each one adds.
SEARCH_KEYWORDS = [
    # broad: most eligible jobs come from these
    "python",
    "data",
    "analyst",
    "developer",
    "consultant",
    "analytics",
    # data and analytics: cleanest keywords (18-24% of results end up eligible)
    "data engineer",
    "analytics engineer",
    "business intelligence",
    "data analyst",
    # business analysis and consulting
    "analytics consultant",
    "implementation consultant",
    "technical business analyst",
    # no presales keyword (dropped 2026-10-30): it mostly found sales and hardware roles;
    # presales jobs in data / AI still come in through the broad keywords
]
SEARCH_LOCATION = "Amsterdam, North Holland, Netherlands"

# LinkedIn time filter: r86400 = posted in the past 24 hours
DATE_POSTED = "r86400"

# safety cap per keyword (actor max is 1000); LinkedIn usually stops well below it
LIMIT_PER_SEARCH = 1000

# Keywords run one after another; each run skips the jobs already fetched in the last few days
# (including earlier keywords today), because Apify charges per returned job and about 64% of
# results were jobs another keyword had already found (2026-10-31: 3,549 results, 1,268 unique)
SKIP_SEEN_DAYS = 3

# Apify run timeout per keyword; the actor's default of 300s cut off broad keywords
# like "analytics" and "consultant" (about 500 results each)
RUN_TIMEOUT_SECS = 900

# Pass 2 only scores jobs first fetched in the last N days, so a prompt change does not rescore
# every job ever found; keep in step with vars.ranking.recent_days in dbt_project.yml
SCORE_RECENT_DAYS = 2

# Pass 2 (Sonnet scoring) runs after eligibility; switched on 2026-10-31 after the review of steps 1-4
RUN_PASS_2 = True
