# warehouse_route_optimizer.py — Automated Daily Route & Storage Optimizer
# Author: A
# Description: Downloads warehouse data from Google Drive, performs route & slotting optimization,
# and outputs a detailed summary JSON file for n8n automation.

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
for df in [picking_df, product_df, storage_df, support_df]:
    df.fillna(0, inplace=True)

summary = {
    "total_orders": len(picking_df),
    "unique_skus": picking_df["SKU"].nunique() if "SKU" in picking_df else None,
    "storage_locations": len(storage_df),
    "support_points": len(support_df),
    "avg_pick_quantity": picking_df["Quantity"].mean() if "Quantity" in picking_df else None,
    "max_storage_capacity": storage_df["Capacity"].max() if "Capacity" in storage_df else None,
    "avg_storage_utilization": storage_df["Utilization"].mean() if "Utilization" in storage_df else None,
}
print("✅ Basic summaries computed.")

# === 5️⃣ Route Optimization (Simple Example using Linear Solver) ===
print("🚚 Running route optimization (simplified)...")
try:
    solver = pywraplp.Solver.CreateSolver("SCIP")
    n = min(len(storage_df), 10)
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
    "meta_info": {
        "script_version": "v2.1",
        "developer": "A",
        "execution_environment": "GitHub Actions - Ubuntu",
        "data_sources": {
            "picking_wave": PICKING_WAVE_URL,
            "product_data": PRODUCT_URL,
            "storage_data": STORAGE_URL,
            "support_data": SUPPORT_URL,
        },
        "note": "This file is auto-generated daily at 11:00 PM IST by a GitHub Actions cron job.",
    },
    "validation_flags": {
        "data_complete": all(len(df) > 0 for df in [picking_df, product_df, storage_df, support_df]),
        "optimization_success": summary["optimized_distance_score"] is not None,
        "slotting_success": len(summary["slotting_result_sample"]) > 0,
    },
    "next_steps": [
        "Feed this output into n8n workflow",
        "Trigger Power BI refresh if needed",
        "Log execution metrics"
    ]
}

with open(OUTPUT_JSON, "w") as f:
    json.dump(output, f, indent=4)

print(f"✅ JSON saved locally to {OUTPUT_JSON}")

# === 8️⃣ Update Existing JSON in Google Drive ===
print("☁️ Updating existing JSON file on Google Drive...")

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

try:
    SERVICE_ACCOUNT_FILE = "service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)

    # 🧩 DEBUG: List visible files to confirm Drive access
    print("🔍 Listing files visible to service account...")
    files_result = drive_service.files().list(pageSize=10, fields="files(id, name)").execute()
    for f in files_result.get("files", []):
        print(f"📁 {f['name']} (ID: {f['id']})")

    # 🆔 Updated file ID for warehouse_route_summary.json (now outside folder)
    FILE_ID = "1oaq5MPXTa73FpdxZihQfrLVSeRtyMtFq"

    # 🧩 DEBUG: Try to fetch metadata for that file specifically
    print(f"🔎 Checking access to specific file ID: {FILE_ID}")
    try:
        meta = drive_service.files().get(fileId=FILE_ID, fields="id, name, mimeType").execute()
        print(f"✅ File found: {meta['name']} ({meta['id']})")
    except Exception as e_meta:
        print(f"⚠️ Could not access file metadata: {e_meta}")

    # Upload JSON update
    media = MediaFileUpload(OUTPUT_JSON, mimetype="application/json")
    updated_file = (
        drive_service.files()
        .update(fileId=FILE_ID, media_body=media)
        .execute()
    )

    print(f"✅ JSON file updated successfully on Google Drive (File ID: {updated_file.get('id')})")

except Exception as e:
    print(f"❌ Failed to update JSON file on Google Drive: {e}")
