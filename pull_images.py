import os
import pandas as pd
import requests
from urllib.parse import urlparse

df = pd.read_csv("/home/tommy/cdw-retail-ai-assistant/shared/data/Product_Dataset_wImages.csv", encoding="latin-1")

# Choose the column
image_column = "ImageUrl"  # change this to your column name
name_column = "ProductName"

# Get unique values
unique_df = df.drop_duplicates(subset=[image_column])[[name_column, image_column]]

output_dir = "./shared/images"

os.makedirs(output_dir, exist_ok=True)

for i, row in unique_df.iterrows():
    url = row[image_column]
    name = str(row[name_column]).strip().replace("/", "_").replace("\\", "_").replace(" ", "_")
    # image_name = str(row[name_column]).strip().replace(" ", "_")

    if not isinstance(url, str) or not url.startswith("http"):
        print(f"Skipping invalid URL: {url}")
        continue

    try:
        # Get filename from name or URL
        parsed = urlparse(url)
        ext = os.path.splitext(parsed.path)[1] or ".jpg"
        filename = f"{name}{ext}" if name else os.path.basename(parsed.path)
        if not filename:
            filename = f"{name}.jpg"

        save_path = os.path.join(output_dir, filename)

        # Skip if already downloaded
        if os.path.exists(save_path):
            print(f"⚠️ Already exists: {filename}")
            continue

        # Download
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Saved: {filename}")

    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")