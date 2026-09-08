"""Downloads the real UCI Online Retail dataset and converts it to CSV.
Run this once before running the quality pipeline."""
import urllib.request
import pandas as pd
import os

URL = "https://raw.githubusercontent.com/eaintkyawthmu/UCI_Online_Retail_Dataset_Cleaned_Version/master/Online%20Retail.xlsx"
XLSX_PATH = "data/online_retail.xlsx"
CSV_PATH = "data/online_retail_raw.csv"

os.makedirs("data", exist_ok=True)
if not os.path.exists(XLSX_PATH):
    print("Downloading UCI Online Retail dataset...")
    urllib.request.urlretrieve(URL, XLSX_PATH)

df = pd.read_excel(XLSX_PATH)
df.to_csv(CSV_PATH, index=False)
print(f"Wrote {len(df)} rows to {CSV_PATH}")
