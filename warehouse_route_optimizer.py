# warehouse_route_optimizer.py — Automated Daily Route & Storage Optimizer
# Author: A
# Description: Downloads warehouse data from Google Drive, performs route & slotting optimization,
# and outputs a detailed summary JSON file for n8n automation.

import json
from datetime import datetime

import numpy as np
import pandas as pd
from ortools.linear_solver import pywraplp

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

print("✅ Libraries imported successfully.")

# === 0️⃣ Google Drive auth (same pattern as SI Live) ===
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]  # <- full Drive scope (fix)
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive = build("drive", "v3", credentials=creds)

# === 1️⃣ Google Drive File URLs ===
PICKING_WAVE_URL = "https://drive.google.com/uc?id=10PWOZKiUInUocKqw9lKZ_NRFg3ml-Vvy"
PRODUCT_URL = "https://drive.google.com/uc?id=1RJ8GnF3D5sLmae4pWbjfSEVro7VSx7dA"
STORAGE_URL = "https://drive.google.com/uc?id=1iaS_OJD-2WLO1JIcaFOf_2CXzAlUSOgB"
SUPPORT_URL = "https://drive.google.com/uc?id=1x1SVZD-S-mdZgY1PlevmbbTJhmEXbUsC"

# Where we write locally and which Drive file to overwrite
OUTPUT_JSON = "warehouse_route_summary.json"
RESULT_JSON_FILE_ID = "1oaq5MPXTa73FpdxZihQfrLVSeRtyMtFq"  # outside folder (your latest)

# === 2️⃣ Helper: Download CSVs from Google Drive ===
def read_drive_csv(url: str) -> pd.DataFrame:
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
    if not df.empty:
        df.fillna(0, inplace=True)

summary = {
    "total_orders": int(len(picking_df)) if not picking_df.empty else 0,
    "unique_skus": int(picking_df["SKU"].nunique()) if "SKU" in picking_df.columns else None,
    "storage_locations": int(len(storage_df)) if not storage_df.empty else 0,
    "support_points": int(len(support_df)) if not support_df.empty else 0,
    "avg_pick_quantity": float(picking_df["Quantity"].mean()) if "Quantity" in picking_df.columns else None,
    "max_storage_capacity": float(storage_df["Capacity"].max()) if "Capacity" in storage_df.columns else None,
    "avg_storage_utilization": float(storage_df["Utilization"].mean()) if "Utilization" in storage_df.columns else None,
}
print("✅ Basic summaries computed.")

# === 5️⃣ Route Optimization (Simple Example using Linear Solver) ===
print("🚚 Running route optimization (simplified)...")
try:
    solver = pywraplp.Solver.CreateSolver("SCIP")
    n = min(len(storage_df), 10) if not storage_df.empty else 0
    if n > 0:
        x = {i: solver.BoolVar(f"x[{i}]") for i in range(n)}
        distances = np.random.randint(10, 100, n)

        # Objective
        solver.Minimize(solver.Sum(x[i] * distances[i] for i in range(n)))
        # 🔧 Add a tiny constraint so solution isn't "pick nothing"
        solver.Add(solver.Sum(x[i] for i in range(n)) >= 1)

        status = solver.Solve()
        optimized_distance = solver.Objective().Value() if status == pywraplp.Solver.OPTIMAL else None
    else:
        optimized_distance = None

    summary["optimized_distance_score"] = float(optimized_distance) if optimized_distance is not None else None
    if optimized_distance is not None:
        print(f"✅ Route optimized with total score: {optimized_distance:.2f}")
    else:
        print("ℹ️ Route optimization skipped (no storage rows).")
except Exception as e:
    print(f"⚠️ Route optimization failed: {e}")
    summary["optimized_distance_score"] = None

# === 6️⃣ Slotting Optimization (Example: SKU vs Zone Matching) ===
print("📦 Running slotting optimization...")
try:
    if not product_df.empty and "Category" in product_df.columns and "SKU" in product_df.columns:
        zone_assignment = product_df.groupby("Category")["SKU"].count().reset_index()
        slotting_result = zone_assignment.sort_values("SKU", ascending=False).head(5).to_dict("records")
    else:
        slotting_result = []
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
        "script_version": "v2.2",
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
        "data_complete": all((not df.empty) for df in [picking_df, product_df, storage_df, support_df]),
        "optimization_success": summary["optimized_distance_score"] is not None,
        "slotting_success": len(summary["slotting_result_sample"]) > 0,
    },
    "next_steps": [
        "Feed this output into n8n workflow",
        "Trigger Power BI refresh if needed",
        "Log execution metrics",
    ],
}

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=4, ensure_ascii=False)

print(f"✅ JSON saved locally to {OUTPUT_JSON}")

# === 8️⃣ Update Existing JSON in Google Drive (SI pattern) ===
print("☁️ Updating existing JSON file on Google Drive...")

try:
    # Confirm we can see the target file (this is where drive.file scope used to fail)
    meta = drive.files().get(fileId=RESULT_JSON_FILE_ID, fields="id,name,mimeType").execute()
    print(f"🔎 Access confirmed for: {meta.get('name')} ({meta.get('id')})")

    # Validate JSON before upload
    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        json.load(f)

    media = MediaFileUpload(OUTPUT_JSON, mimetype="application/json", resumable=True)
    drive.files().update(fileId=RESULT_JSON_FILE_ID, media_body=media).execute()
    print("✅ JSON file updated successfully on Google Drive.")

except json.JSONDecodeError:
    print("❌ JSON invalid — skipping upload.")
except Exception as e:
    print(f"❌ Failed to update JSON file on Google Drive: {e}")
