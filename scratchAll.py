import pandas as pd
import careerLink
import careerViet
import itViec
import topCV
import vietnamWorks

print("Starting all job scrapers...")

all_data = []
for scraper in [careerLink, careerViet, itViec, topCV, vietnamWorks]:
    print(f"Scraping jobs from {scraper.__name__}...")
    data = scraper.get_all_jobs()
    all_data.extend(data)  # Combine data from all sources

df = pd.DataFrame(all_data)
print(df)