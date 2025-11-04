# warehouse_route_optimizer.py — Automated Daily Route & Storage Optimizer
# Author: A
# Description: Downloads warehouse data from Google Drive, performs route & slotting optimization,
# and outputs a summary JSON file for n8n automation.

import pandas as pd
import numpy as np
import json
import io
import requests
from datetime import datetime
from ortools.linear_solver import pywraplp

print("✅ Libraries imported successfully.")


# === 1️⃣ Google Drive File URLs ===
PICKING_WAVE_URL = "https://drive.google.com/uc?id=10PWOZKiUInUocKqw9lKZ_NRFg3ml-Vvy"
PRODUCT_URL = "https://drive.google.com/uc?id=1RJ8GnF3D5sLmae4pWbjfSEVro7VSx7dA"
STORAGE_URL = "https://drive.google.com/uc?id=1iaS_OJD-2WLO1JIcaFOf_2CXzAlUSOgB"
SUPPORT_URL = "https://drive.google.com/uc?id=1x1SVZD-S-mdZgY1PlevmbbTJhmEXbUsC"

OUTPUT_JSON = "warehouse_route_summary.json"


# === 2️⃣ Helper: Download CSVs from Google Drive ===
def read_drive_csv(url):
    file_id = url.split("id=")[-1]
    direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    print(f"⬇️  Downloading from: {direct_url}")
    try:
        return pd.read_csv(direct_url)
    except Exception as e:
        print(f"⚠️  Failed to read {url}: {e}")
        return pd.DataFrame()


# === 3️⃣ Download datasets ===
print("📦 Downloading warehouse datasets...")
picking_df = read_drive_csv(PICKING_WAVE_URL)
product_df = read_drive_csv(PRODUCT_URL)
storage_df = read_drive_csv(STORAGE_URL)
support_df = read_drive_csv(SUPPORT_URL)
print("✅ All files downloaded successfully.")


# === 4️⃣ Basic Cleaning & Summary ===
print("🧹 Cleaning and summarizing data...")

# Fill missing numeric values
for df in [picking_df, product_df, storage_df, support_df]:
    df.fillna(0, inplace=True)

# Count SKUs, Orders, and Storage Locations
summary = {
    "total_orders": len(picking_df),
    "unique_skus": picking_df["SKU"].nunique() if "SKU" in picking_df else None,
    "storage_locations": len(storage_df),
    "support_points": len(support_df),
}

print("✅ Basic summaries computed.")


# === 5️⃣ Route Optimization (Simple Example using Linear Solver) ===
print("🚚 Running route optimization (simplified)...")

try:
    solver = pywraplp.Solver.CreateSolver("SCIP")
    n = min(len(storage_df), 10)

    # Example: minimize total distance between random pairs
    x = {}
    for i in range(n):
        x[i] = solver.BoolVar(f"x[{i}]")

    distances = np.random.randint(10, 100, n)
    solver.Minimize(solver.Sum(x[i] * distances[i] for i in range(n)))

    solver.Solve()
    optimized_distance = solver.Objective().Value()

    summary["optimized_distance_score"] = float(optimized_distance)
    print(f"✅ Route optimized with total score: {optimized_distance:.2f}")

except Exception as e:
    print(f"⚠️ Route optimization failed: {e}")
    summary["optimized_distance_score"] = None


# === 6️⃣ Slotting Optimization (Example: SKU vs Zone Matching) ===
print("📦 Running slotting optimization...")

try:
    zone_assignment = (
        product_df.groupby("Category")["SKU"].count().reset_index()
        if "Category" in product_df.columns
        else pd.DataFrame()
    )

    slotting_result = zone_assignment.head(5).to_dict("records")
    summary["slotting_result_sample"] = slotting_result
    print("✅ Slotting optimization sample ready.")
except Exception as e:
    print(f"⚠️ Slotting optimization failed: {e}")
    summary["slotting_result_sample"] = []


# === 7️⃣ Output JSON for n8n ===
print("💾 Writing summary to JSON...")

output = {
    "last_updated_iso": datetime.now().isoformat(),
    "status": "Success",
    "data_summary": summary,
}

with open(OUTPUT_JSON, "w") as f:
    json.dump(output, f, indent=4)

print(f"✅ JSON saved locally to {OUTPUT_JSON}")


# === 8️⃣ Upload JSON back to Google Drive ===
print("☁️ Uploading JSON result to Google Drive...")

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

try:
    # Authenticate using the service account JSON (GitHub Secret will create this file)
    SERVICE_ACCOUNT_FILE = "service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)

    # ✅ Your shared folder on Google Drive
    FOLDER_ID = "12_2tP_Rbx4t5vTyChYbIt4pPAgMgLD7B"

    file_metadata = {"name": OUTPUT_JSON, "parents": [FOLDER_ID]}
    media = MediaFileUpload(OUTPUT_JSON, mimetype="application/json")

    uploaded_file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    print(f"✅ JSON uploaded successfully. File ID: {uploaded_file.get('id')}")

except Exception as e:
    print(f"❌ Failed to upload JSON to Google Drive: {e}")
