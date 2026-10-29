# Search settings for the daily fetch. Credentials come from .env, not here.

APIFY_ACTOR_ID = "valig/linkedin-jobs-scraper"

# One Apify run per keyword. LinkedIn matches keywords loosely and only returns a sample
# per search, so broad and specific keywords both add jobs. Every keyword's results are
# kept in raw.apify_jobs; mart.keyword_stats shows what each one adds, to prune later.
SEARCH_KEYWORDS = [
    # data and analytics
    "data",
    "analytics",
    "analyst",
    "data analyst",
    "product analyst",
    "business intelligence",
    "BI developer",
    "insights",
    "analytics engineer",
    # data and analytics — additions
    "analytics analyst",
    "BI analyst",
    "business data analyst",
    "commercial analyst",
    "operations analyst",
    "decision scientist",
    "decision analyst",
    "quantitative analyst",
    "reporting analyst",
    "performance analyst",
    "revenue analyst",
    "pricing analyst",
    "marketing analyst",
    "customer insights analyst",
    "growth analyst",
    "experimentation analyst",
    "forecasting analyst",
    "planning analyst",
    "data visualization",
    "analytics specialist",
    "business intelligence analyst",
    "BI engineer",
    # data, ML and AI engineering
    "data engineer",
    "data platform",
    "AI engineer",
    # data, ML and AI engineering — additions
    "analytics developer",
    "data modeller",
    "data modeler",
    "data architect",
    "data warehouse engineer",
    "ETL developer",
    "ELT developer",
    "data integration engineer",
    "data infrastructure engineer",
    "data reliability engineer",
    "AI solutions engineer",
    "AI consultant",
    "AI implementation consultant",
    "generative AI engineer",
    "AI automation engineer",
    "platform engineer",
    "developer advocate",
    "technical solutions engineer",
    # software engineering
    "software engineer",
    "developer",
    "python",
    "backend engineer",
    # software engineering — additions
    "software developer",
    "application developer",
    "systems engineer",
    "solutions developer",
    "API engineer",
    "automation engineer",
    "DevOps engineer",
    "site reliability engineer",
    "platform developer",
    "cloud solutions engineer",
    "cloud solutions architect",
    "technical architect",
    "systems analyst",
    "business systems analyst",
    "enterprise applications consultant",
    # business analysis and consulting
    "business analyst",
    "consultant",
    "data consultant",
    "technology consultant",
    "implementation consultant",
    # business analysis and consulting — additions
    "strategy analyst",
    "business intelligence consultant",
    "analytics consultant",
    "data strategy consultant",
    "technology analyst",
    "IT consultant",
    "management consultant",
    "digital transformation consultant",
    "business transformation consultant",
    "process improvement analyst",
    "business process analyst",
    "decision support analyst",
    "operations research analyst",
    "technical business analyst",
    "functional consultant",
    "solution delivery consultant",
    "transformation analyst",
    # presales and solutions
    "presales",
    "solutions engineer",
    "solutions consultant",
    "solutions architect",
    "sales engineer",
    "technical consultant",
    # presales and solutions — additions
    "pre-sales engineer",
    "pre sales engineer",
    "solutions specialist",
    "solution engineer",
    "technical sales engineer",
    "sales consultant",
    "solution architect",
    "customer engineer",
    "customer success engineer",
    "technical account manager",
    "solutions delivery engineer",
    "sales solutions consultant",
    "cloud consultant",
    "data solutions architect",
    "data solutions consultant",
    "partner solutions architect",
    "technical product specialist",
    "field applications engineer",
    "implementation engineer",
    # hybrid business and technology — additions
    "revenue operations analyst",
    "RevOps analyst",
    "GTM engineer",
    "growth operations analyst",
    "business operations analyst",
    "strategy and operations analyst",
    "sales operations analyst",
    "marketing operations analyst",
    "product operations analyst",
    "business systems consultant",
    "solutions operations",
    "automation consultant",
    "workflow automation engineer",
    "technical program analyst",
    "product operations",
    "business technology consultant",
    "commercial operations analyst",
]
SEARCH_LOCATION = "Amsterdam, North Holland, Netherlands"

# LinkedIn time filter: r86400 = posted in the past 24 hours
DATE_POSTED = "r86400"

# safety cap per keyword (actor max is 1000); LinkedIn usually stops well below it
LIMIT_PER_SEARCH = 1000

# Apify runs started at the same time
MAX_PARALLEL_SEARCHES = 4

# Pass 2 (Sonnet scoring) is off while steps 1-4 are being refined; the daily run stops after eligibility
RUN_PASS_2 = False
