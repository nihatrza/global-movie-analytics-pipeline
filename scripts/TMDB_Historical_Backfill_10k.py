import os
import time
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Path to your Service Account JSON Key in Colab
JSON_KEY_PATH = "your-service-account-key.json"
credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)

# TMDB & GCP Configuration
TMDB_BEARER_TOKEN = "YOUR_TMDB_BEARER_TOKEN_HERE"
PROJECT_ID = "YOUR_GCP_PROJECT_ID_HERE"
DATASET_ID = "YOUR_BIGQUERY_DATASET_ID_HERE"
LOCATION = "US"

HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_BEARER_TOKEN}"
}
BASE_URL = "https://api.themoviedb.org/3"

def run_10k_colab_backfill():
    print("🚀 Backfill process for 10,000 movies started...")
    raw_movies = []
    
    # Fetch 500 pages x 20 movies = 10,000 movies
    start_time = time.time()
    for page in range(1, 501):
        discover_url = f"{BASE_URL}/discover/movie?include_adult=false&include_video=false&language=en-US&page={page}&sort_by=vote_count.desc&vote_count.gte=50"
        resp = requests.get(discover_url, headers=HEADERS)
        if resp.status_code == 200:
            raw_movies.extend(resp.json().get("results", []))
        
        if page % 50 == 0:
            print(f"📦 Fetched list of {page * 20} movies from TMDB API...")
        time.sleep(0.03)

    print(f"✅ Total of {len(raw_movies)} movie IDs fetched. Extracting details for each movie...")

    fact_movies = []
    genres_set, cast_set, directors_set, companies_set = set(), set(), set(), set()
    bridge_genres, bridge_cast, bridge_directors, bridge_companies = [], [], [], []
    date_dict = {}

    processed_count = 0
    for d_raw in raw_movies:
        movie_id = d_raw["id"]
        detail_url = f"{BASE_URL}/movie/{movie_id}?language=en-US&append_to_response=credits"
        det_resp = requests.get(detail_url, headers=HEADERS)
        
        if det_resp.status_code != 200:
            continue
            
        d = det_resp.json()
        budget = d.get("budget", 0)
        revenue = d.get("revenue", 0)
        profit = revenue - budget
        roi = round((profit / budget * 100), 2) if budget > 0 else 0.0
        rel_date = d.get("release_date")
        date_key = int(rel_date.replace("-", "")) if rel_date and len(rel_date) == 10 else None
        
        if rel_date and date_key and date_key not in date_dict:
            try:
                dt = pd.to_datetime(rel_date)
                date_dict[date_key] = {
                    "date_key": date_key,
                    "full_date": rel_date,
                    "year": dt.year,
                    "quarter": dt.quarter,
                    "month": dt.month,
                    "month_name": dt.strftime("%B"),
                    "day": dt.day,
                    "day_of_week": dt.strftime("%A")
                }
            except Exception:
                pass

        fact_movies.append({
            "movie_id": movie_id,
            "tconst": d.get("imdb_id"),
            "title": d.get("title"),
            "release_date": rel_date,
            "date_key": date_key,
            "runtime": d.get("runtime", 0),
            "budget": budget,
            "revenue": revenue,
            "profit": profit,
            "roi": roi,
            "popularity": round(d.get("popularity", 0.0), 2),
            "vote_average": round(d.get("vote_average", 0.0), 1),
            "vote_count": d.get("vote_count", 0),
            "original_language": d.get("original_language")
        })

        for g in d.get("genres", []):
            genres_set.add((g["id"], g["name"]))
            bridge_genres.append({"movie_id": movie_id, "genre_id": g["id"]})

        cast = d.get("credits", {}).get("cast", [])[:5]
        for order, c in enumerate(cast):
            cast_set.add((c["id"], c["name"]))
            bridge_cast.append({
                "movie_id": movie_id,
                "actor_id": c["id"],
                "character_name": c.get("character"),
                "cast_order": order + 1
            })

        crew = d.get("credits", {}).get("crew", [])
        for member in crew:
            if member.get("job") == "Director":
                directors_set.add((member["id"], member["name"]))
                bridge_directors.append({"movie_id": movie_id, "director_id": member["id"]})

        for comp in d.get("production_companies", []):
            companies_set.add((comp["id"], comp["name"], comp.get("origin_country")))
            bridge_companies.append({"movie_id": movie_id, "company_id": comp["id"]})

        processed_count += 1
        if processed_count % 1000 == 0:
            print(f"⏳ Processed details and Star Schema for {processed_count} movies...")
        
        time.sleep(0.01)

    print("📊 Converting data to Pandas DataFrames...")
    dfs = {
        "fact_movies": pd.DataFrame(fact_movies),
        "dim_date": pd.DataFrame(list(date_dict.values())),
        "dim_genres": pd.DataFrame([{"genre_id": g[0], "genre_name": g[1]} for g in genres_set]),
        "bridge_movie_genres": pd.DataFrame(bridge_genres),
        "dim_cast": pd.DataFrame([{"actor_id": c[0], "actor_name": c[1]} for c in cast_set]),
        "bridge_movie_cast": pd.DataFrame(bridge_cast),
        "dim_directors": pd.DataFrame([{"director_id": d[0], "director_name": d[1]} for d in directors_set]),
        "bridge_movie_directors": pd.DataFrame(bridge_directors),
        "dim_companies": pd.DataFrame([{"company_id": c[0], "company_name": c[1], "origin_country": c[2]} for c in companies_set]),
        "bridge_movie_companies": pd.DataFrame(bridge_companies)
    }

    print("☁️ Uploading to Google BigQuery (WRITE_TRUNCATE mode)...")
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID, location=LOCATION)
    for table_name, df in dfs.items():
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"🎉 Successfully written to BigQuery: {table_ref} ({len(df)} rows)")

    end_time = time.time()
    print(f"✨ 10,000 movies backfill completed in {round((end_time - start_time)/60, 2)} minutes!")

run_10k_colab_backfill()