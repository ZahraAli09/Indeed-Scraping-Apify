#WITHOUT THREADPOOL EXECUTOR
from jobspy import scrape_jobs
import pandas as pd
import os
import time
import random

# ==============================================================
# CONFIGURATION
# ==============================================================

SEARCH_TERMS =SEARCH_TERMS = [
    "AI",
    "ML",
    "Machine Learning",
    "Artificial Intelligence",
    "Deep Learning",
    "NLP",
    "Computer Vision",
    "AI Engineer",
    "ML Engineer",
    "Machine Learning Engineer",
    "AI Scientist",

    # Experience based (INCREASE RESULTS)
    "AI Intern",
    "ML Intern",
    "Machine Learning Intern",
    "AI Fresher",
    "Machine Learning Fresher",
    "Entry Level AI",
    "Entry Level Machine Learning",
    "Junior AI Engineer",
    "Junior Machine Learning Engineer",
    "Senior AI Engineer",
    "Senior ML Engineer",
    "Lead Machine Learning",
    "Principal AI"
]


CITIES = [  "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
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
job_count = 0


COUNTRY = "usa"
RESULTS_PER_QUERY = 10 # realistic maximum from Indeed

DATA_DIR="data"

MASTER_FILE =os.path.join(DATA_DIR, "indeed_ai_ml_master.csv")
BATCH_FILE =os.path.join(DATA_DIR, "indeed_ai_ml_batch.csv")


# ==============================================================
# LOAD MASTER FILE
# ==============================================================

if os.path.exists(MASTER_FILE):
    master_df = pd.read_csv(MASTER_FILE)
    existing_urls = set(master_df["job_url"].astype(str))
    print(f"Loaded master with {len(existing_urls)} URLs.")
else:
    master_df = pd.DataFrame()
    existing_urls = set()
    print("Master not found → starting fresh.")

# reset batch
open(BATCH_FILE, "w").close()
session_urls = set()


# ==============================================================
# SCRAPE FUNCTION
# ==============================================================

def scrape_one(term, city):
    print(f"→ Scraping: {term} | {city}")
    time.sleep(random.uniform(0.5,2.5))

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

        # Remove rows without URLs
        df = df.dropna(subset=["job_url"])
        df["job_url"] = df["job_url"].astype(str)

        # # convert title to lower
        df["title_clean"] = df["title"].str.lower()

        # # strict filter — keep only real AI/ML jobs
        # df = df[df["title_clean"].str.contains("|".join(TITLE_FILTER))]
        df = df[df["title_clean"].str.contains("|".join(STRICT_WORDS))]

        if df.empty:
            return pd.DataFrame()

        # Keep only recent jobs
        if "date_posted" in df.columns:
            df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")


        return df

    except Exception as e:
        print(f"Error for {term} - {city}: {e}")
        return pd.DataFrame()


# ==============================================================
# CREATE COMBINATIONS
# ==============================================================

combos = [(t, c) for t in SEARCH_TERMS for c in CITIES]
random.shuffle(combos)
print(f"Total queries: {len(combos)}\nStarting  scraping...\n")



for term, city in combos:
  if job_count>=MAX_JOBS_PER_RUN:
    print(f"\nReached job limit ({MAX_JOBS_PER_RUN}). Stopping run.")
    break

  df = scrape_one(term, city)
  if df.empty:
    continue
        # remove duplicates globally
  df = df[~df["job_url"].isin(existing_urls)]
  df = df[~df["job_url"].isin(session_urls)]

  if df.empty:
      continue


  remaining = MAX_JOBS_PER_RUN - job_count
  if len(df) > remaining:
    df = df.head(remaining)

  job_count+=len(df)


  # update session
  new_urls = set(df["job_url"])
  session_urls.update(new_urls)

  # append batch
  df.to_csv(
      BATCH_FILE,
      mode="a",
      header=(os.path.getsize(BATCH_FILE) == 0),
      index=False
  )

  print(f"Saved {len(df)} rows to batch.")


# ==============================================================
# MERGE INTO MASTER
# ==============================================================

print("\nScraping finished. Merging batch → master...")

if os.path.getsize(BATCH_FILE) == 0:
    print("Batch empty → nothing to merge.")
else:
    batch_df = pd.read_csv(BATCH_FILE)
    print(f"Batch rows: {len(batch_df)}")

    final_df = pd.concat([master_df, batch_df], ignore_index=True)
    final_df.drop_duplicates(subset=["job_url"], inplace=True)

    final_df.to_csv(MASTER_FILE, index=False)
    print(f"Master updated → {len(final_df)} total rows")
    print("Done!")