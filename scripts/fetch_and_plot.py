import os
import sys
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
BUCKETS = {
    "EURO-AIFS": "ecmwf-forecasts",
    "EAGLE-GraphCast": "noaa-nws-graphcastgfs-pds",
    "FourCastNet": "noaa-nws-fourcastnetgfs-pds"
}

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def get_latest_prefix(bucket, model_type):
    """Finds the latest available date folder."""
    date = datetime.utcnow()
    # Check up to 5 days back (sometimes data is delayed)
    for i in range(5): 
        date_str = date.strftime('%Y%m%d')
        
        if "noaa" in bucket:
            # NOAA Naming: graphcastgfs.20240205/00/
            prefix = f"{model_type.lower().replace('-', '').replace('eagle', '')}.{date_str}/00/"
            # Correction for EAGLE/GraphCast naming differences if needed
            if "eagle" in model_type.lower():
                prefix = f"graphcastgfs.{date_str}/00/"
            if "fourcastnet" in model_type:
                prefix = f"fcngfs.{date_str}/00/"
        else:
            # ECMWF Naming: 20240205/00z/aifs/0p25/oper/
            prefix = f"{date_str}/00z/aifs/0p25/oper/"
            
        print(f"Checking {bucket}/{prefix}...")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' in resp:
            print(f"FOUND data at {prefix}")
            return prefix, date_str
        
        date -= timedelta(days=1)
    return None, None

def download_and_plot(model_name, bucket):
    print(f"--- Processing {model_name} ---")
    prefix, date_str = get_latest_prefix(bucket, model_name)
    
    if not prefix:
        print(f"❌ No data found for {model_name} in last 5 days.")
        return False

    # List files to find the forecast step
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    target_key = None
    candidates = []

    for obj in resp.get('Contents', []):
        key = obj['Key']
        candidates.append(key)
        # We look for forecast hour 6 or 12 (f006/f012)
        if "noaa" in bucket and ("f006" in key or "f012" in key):
            target_key = key
            break
        elif "ecmwf" in bucket and key.endswith("grib2"):
            target_key = key
            break
    
    if not target_key:
        print(f"❌ Found prefix but no matching GRIB file. First 5 files found:")
        for c in candidates[:5]: print(f" - {c}")
        return False

    # Download
    local_file = f"{model_name}.grib2"
    print(f"Downloading {target_key}...")
    s3.download_file(bucket, target_key, local_file)

    # --- Processing ---
    try:
        # Open with explicit backend
        ds = xr.open_dataset(local_file, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
        
        # Variable Mapping
        temp_var = next((v for v in ds.data_vars if v in ['t2m', 'tmp', '2t']), None)
        u_var = next((v for v in ds.data_vars if v in ['u10', 'ugrd', '10u']), None)
        v_var = next((v for v in ds.data_vars if v in ['v10', 'vgrd', '10v']), None)

        if not temp_var:
            print(f"⚠️ Could not find Temperature variable. Available: {list(ds.data_vars)}")

        # Plot Temp
        if temp_var:
            plt.figure(figsize=(10, 5))
            data = ds[temp_var] - 273.15
            data.plot(cmap='jet', vmin=-30, vmax=40, add_labels=False)
            plt.axis('off')
            plt.savefig(f"assets/{model_name}_temp.png", bbox_inches='tight', pad_inches=0)
            plt.close()
            print(f"✅ Generated {model_name}_temp.png")

        # Plot Wind
        if u_var and v_var:
            plt.figure(figsize=(10, 5))
            wind = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
            wind.plot(cmap='viridis', vmin=0, vmax=30, add_labels=False)
            plt.axis('off')
            plt.savefig(f"assets/{model_name}_wind.png", bbox_inches='tight', pad_inches=0)
            plt.close()
            print(f"✅ Generated {model_name}_wind.png")
            
        return True

    except Exception as e:
        print(f"❌ Error processing {model_name}: {e}")
        return False

# Main Execution
os.makedirs("assets", exist_ok=True)
success_count = 0
for name, bucket in BUCKETS.items():
    if download_and_plot(name, bucket):
        success_count += 1

# Fail the Action if NO images were generated
if success_count == 0:
    print("❌ No images were generated for any model. Exiting with error.")
    sys.exit(1)
