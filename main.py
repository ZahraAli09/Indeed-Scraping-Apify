from apify import Actor
from jobspy import scrape_jobs
import pandas as pd
import os
import time
import random
import asyncio

# ============================================================== #
# CONFIGURATION
# ============================================================== #

SEARCH_TERMS = [
    "AI", "ML", "Machine Learning", "Artificial Intelligence",
    "Deep Learning", "NLP", "Computer Vision",
    "AI Engineer", "ML Engineer", "Machine Learning Engineer",
    "AI Scientist",
    "AI Intern", "ML Intern", "Machine Learning Intern",
    "AI Fresher", "Machine Learning Fresher",
    "Entry Level AI", "Entry Level Machine Learning",
    "Junior AI Engineer", "Junior Machine Learning Engineer",
    "Senior AI Engineer", "Senior ML Engineer",
    "Lead Machine Learning", "Principal AI"
]

CITIES = [ "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming","New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis",
    "Seattle", "Denver", "Washington", "Boston", "El Paso", "Nashville",
    "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis",
    "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson",
    "Fresno", "Sacramento", "Mesa", "Kansas City", "Atlanta", "Omaha",
    "Colorado Springs", "Raleigh", "Long Beach", "Miami", "Virginia Beach",
    "Oakland", "Minneapolis", "Tulsa", "Tampa", "Arlington", "New Orleans",
    "Wichita", "Cleveland", "Bakersfield", "Aurora", "Anaheim", "Honolulu",
    "Santa Ana", "Riverside", "Corpus Christi", "Lexington", "Stockton",
    "Henderson", "Saint Paul", "St. Louis", "Cincinnati", "Pittsburgh",
    "Greensboro", "Anchorage", "Plano", "Lincoln", "Orlando", "Irvine",
    "Newark", "Durham", "Chula Vista", "Toledo", "Fort Wayne", "St. Petersburg",
    "Laredo", "Jersey City", "Chandler", "Madison", "Lubbock", "Scottsdale",
    "Reno", "Buffalo", "Gilbert", "Glendale", "Winston–Salem", "North Las Vegas",
    "Norfolk", "Chesapeake", "Garland", "Irving", "Hialeah", "Fremont",
    "Boise", "Richmond", "Baton Rouge", "Spokane", "Des Moines", "Tacoma",
    "San Bernardino", "Modesto", "Fontana", "Moreno Valley", "Santa Clarita",
    "Fayetteville", "Birmingham", "Oxnard", "Rochester", "Port St. Lucie",
    "Grand Rapids", "Huntsville", "Salt Lake City", "Frisco", "Yonkers",
    "Amarillo", "Glendale", "Huntington Beach", "McKinney", "Montgomery",
    "Augusta", "Aurora", "Akron", "Little Rock", "Tempe", "Overland Park",
    "Grand Prairie", "Tallahassee", "Cape Coral", "Mobile", "Knoxville",
    "Shreveport", "Worcester", "Ontario", "Vancouver", "Sioux Falls",
    "Chattanooga", "Brownsville", "Fort Lauderdale", "Providence",
    "Newport News", "Rancho Cucamonga", "Santa Rosa", "Peoria", "Oceanside",
    "Elk Grove", "Salem", "Pembroke Pines", "Eugene", "Garden Grove",
    "Cary", "Fort Collins", "Corona", "Springfield", "Jackson", "Alexandria",
    "Hayward", "Clarksville", "Lakewood", "Lancaster", "Salinas", "Palmdale",
    "Hollywood", "Springfield", "Macon", "Kansas City", "Sunnyvale", "Pomona",
    "Escondido", "Pasadena", "Savannah", "Bellevue", "Murfreesboro",
    "Dayton", "Visalia", "Gainesville", "Boulder", "Thornton", "Roseville",
    "Denton", "West Valley City", "Midland", "Carrollton", "Waco", "Charleston",
    "Sterling Heights", "Surprise", "Greeley", "Santa Clara", "Simi Valley",
    "East Los Angeles", "Evansville", "Olathe", "Hartford", "Allentown",
    "Beaumont", "Independence", "Ann Arbor", "Abilene", "Vallejo", "Berkeley",
    "Round Rock", "Columbia", "Pearland", "Thornton", "Lafayette", "Hampton",
    "Gainesville", "Flint", "Kennewick", "Erie", "Clearwater", "Arvada",
    "Fairfield", "Billings", "West Palm Beach", "Richardson"]

STRICT_WORDS = [
    "ai", "ml", "machine learning", "deep learning",
    "llm", "nlp", "computer vision", "genai"
]

MAX_JOBS_PER_RUN = 200
RESULTS_PER_QUERY = 10
COUNTRY = "usa"

DATA_DIR = "apify_storage/data"
os.makedirs(DATA_DIR, exist_ok=True)

MASTER_FILE = os.path.join(DATA_DIR, "indeed_ai_ml_master.csv")
BATCH_FILE = os.path.join(DATA_DIR, "indeed_ai_ml_batch.csv")

# ============================================================== #
# SCRAPER
# ============================================================== #

def scrape_one(term, city):
    time.sleep(random.uniform(0.5, 2.0))

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

        return df

    except Exception as e:
        print(f"Error for {term} - {city}: {e}")
        return pd.DataFrame()

# ============================================================== #
# ACTOR MAIN
# ============================================================== #

async def main():
    async with Actor:

        # 🔹 CREATE APIFY PROXY (FREE PLAN SAFE)
        proxy_config = await Actor.create_proxy_configuration(
            groups=["DATACENTER"],
            country_code="US"
        )
        

        proxy_url=await proxy_config.new_url()
        # 🔹 FORCE REQUESTS / JOBSPY TO USE PROXY
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url

        Actor.log.info("Apify proxy enabled")

        # LOAD MASTER
        if os.path.exists(MASTER_FILE):
            master_df = pd.read_csv(MASTER_FILE)
            existing_urls = set(master_df["job_url"].astype(str))
        else:
            master_df = pd.DataFrame()
            existing_urls = set()

        open(BATCH_FILE, "w").close()
        session_urls = set()
        job_count = 0

        combos = [(t, c) for t in SEARCH_TERMS for c in CITIES]
        random.shuffle(combos)

        for term, city in combos:
            if job_count >= MAX_JOBS_PER_RUN:
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

        # MERGE
        if os.path.getsize(BATCH_FILE) > 0:
            batch_df = pd.read_csv(BATCH_FILE)
            final_df = pd.concat([master_df, batch_df], ignore_index=True)
            final_df.drop_duplicates(subset=["job_url"], inplace=True)
            final_df.to_csv(MASTER_FILE, index=False)

        Actor.log.info(f"Run finished. Total jobs saved: {job_count}")

# ============================================================== #

if __name__ == "__main__":
    asyncio.run(main())
