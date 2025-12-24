from apify import Actor
from jobspy import scrape_jobs
import pandas as pd
import os
import time
import random

# ==============================================================
# CONFIGURATION
# ==============================================================

SEARCH_TERMS = [
    "AI", "ML", "Machine Learning", "Artificial Intelligence",
    "Deep Learning", "NLP", "Computer Vision",
    "AI Engineer", "ML Engineer", "Machine Learning Engineer",
    "AI Scientist", "AI Intern", "ML Intern",
    "Machine Learning Intern", "AI Fresher",
    "Machine Learning Fresher", "Entry Level AI",
    "Entry Level Machine Learning", "Junior AI Engineer",
    "Junior Machine Learning Engineer", "Senior AI Engineer",
    "Senior ML Engineer", "Lead Machine Learning", "Principal AI"
]

CITIES = [
    "California", "New York", "Texas", "Washington",
    "Massachusetts", "Illinois", "Florida"
    # (you can keep full list; trimmed here for sanity)
]

STRICT_WORDS = [
    "ai", "ml", "machine learning", "deep learning",
    "llm", "nlp", "computer vision", "genai"
]

MAX_JOBS_PER_RUN = 1000
COUNTRY = "usa"
RESULTS_PER_QUERY = 100

DATA_DIR = "/apify_storage/data"
os.makedirs(DATA_DIR, exist_ok=True)

MASTER_FILE = os.path.join(DATA_DIR, "indeed_ai_ml_master.csv")
BATCH_FILE = os.path.join(DATA_DIR, "indeed_ai_ml_batch.csv")

# ==============================================================
# SCRAPE FUNCTION
# ==============================================================

def scrape_one(term, city):
    print(f"→ Scraping: {term} | {city}")
    time.sleep(random.uniform(1.5, 3.5))  # throttle (IMPORTANT)

    try:
        df = scrape_jobs(
            site_name=["indeed"],
            search_term=term,
            location=city,
            country=COUNTRY,
            results_wanted=RESULTS_PER_QUERY,
            sort_by="date",
            timeout=60
        )

        if df.empty:
            return pd.DataFrame()

        df = df.dropna(subset=["job_url"])
        df["job_url"] = df["job_url"].astype(str)
        df["title_clean"] = df["title"].str.lower()

        df = df[df["title_clean"].str.contains("|".join(STRICT_WORDS))]

        if "date_posted" in df.columns:
            df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")

        return df

    except Exception as e:
        print(f"Error for {term} - {city}: {e}")
        return pd.DataFrame()

# ==============================================================
# MAIN ACTOR ENTRYPOINT
# ==============================================================

async def main():
    async with Actor:

        # 🔹 CREATE APIFY PROXY
        proxy_url = await Actor.create_proxy_configuration(
            groups=["DATACENTER"],  # use RESIDENTIAL if available
            country_code="US"
        ).new_url()

        # 🔹 MAKE JOBSPY USE THE PROXY
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url

        Actor.log.info("Apify proxy enabled")

        # ======================================================
        # LOAD MASTER
        # ======================================================

        job_count = 0
        session_urls = set()

        if os.path.exists(MASTER_FILE):
            master_df = pd.read_csv(MASTER_FILE)
            existing_urls = set(master_df["job_url"].astype(str))
            Actor.log.info(f"Loaded master with {len(existing_urls)} URLs.")
        else:
            master_df = pd.DataFrame()
            existing_urls = set()
            Actor.log.info("Master not found → starting fresh.")

        open(BATCH_FILE, "w").close()

        combos = [(t, c) for t in SEARCH_TERMS for c in CITIES]
        random.shuffle(combos)

        # ======================================================
        # SCRAPING LOOP
        # ======================================================

        for term, city in combos:
            if job_count >= MAX_JOBS_PER_RUN:
                Actor.log.info("Reached job limit. Stopping.")
                break

            df = scrape_one(term, city)
            if df.empty:
                continue

            df = df[~df["job_url"].isin(existing_urls)]
            df = df[~df["job_url"].isin(session_urls)]

            if df.empty:
                continue

            remaining = MAX_JOBS_PER_RUN - job_count
            df = df.head(remaining)

            job_count += len(df)
            session_urls.update(df["job_url"])

            df.to_csv(
                BATCH_FILE,
                mode="a",
                header=(os.path.getsize(BATCH_FILE) == 0),
                index=False
            )

            Actor.log.info(f"Saved {len(df)} jobs (total: {job_count})")

        # ======================================================
        # MERGE
        # ======================================================

        if os.path.getsize(BATCH_FILE) > 0:
            batch_df = pd.read_csv(BATCH_FILE)
            final_df = pd.concat([master_df, batch_df], ignore_index=True)
            final_df.drop_duplicates(subset=["job_url"], inplace=True)
            final_df.to_csv(MASTER_FILE, index=False)

        Actor.log.info(f"Run finished. Total jobs scraped: {job_count}")
