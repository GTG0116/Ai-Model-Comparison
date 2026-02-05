import os
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

# Setup anonymous S3 client
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def get_latest_prefix(bucket, model_type):
    """Finds the latest available date folder in the S3 buckets."""
    # Logic varies slightly by bucket, simplifying for the most recent date
    date = datetime.utcnow()
    for _ in range(3): # Check last 3 days
        date_str = date.strftime('%Y%m%d')
        
        # NOAA Naming Structure
        if "noaa" in bucket:
            # Check 00z run
            prefix = f"graphcastgfs.{date_str}/00/" if "graphcast" in bucket else f"fcngfs.{date_str}/00/"
        
        # ECMWF Naming Structure
        else:
            # e.g., 20240205/00z/aifs/0p25/oper/
            prefix = f"{date_str}/00z/aifs/0p25/oper/"
            
        # Check if exists
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' in resp:
            return prefix, date_str
        date -= timedelta(days=1)
    return None, None

def download_and_plot(model_name, bucket):
    print(f"Processing {model_name}...")
    prefix, date_str = get_latest_prefix(bucket, model_name)
    
    if not prefix:
        print(f"No data found for {model_name}")
        return

    # Find the file (Forecast hour 0 or 6 usually)
    # We look for a file ending in grib2
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    target_key = None
    
    for obj in resp.get('Contents', []):
        key = obj['Key']
        # We want a forecast file, usually f006 or f012 to show change
        if "noaa" in bucket and ("f012" in key or "f006" in key):
            target_key = key
            break
        elif "ecmwf" in bucket and key.endswith("grib2"):
            # ECMWF often splits by variable or step. 
            # For simplicity in this demo, we grab the first main forecast file
            target_key = key
            break
    
    if not target_key:
        return

    # Download
    local_file = f"{model_name}.grib2"
    print(f"Downloading {target_key}...")
    s3.download_file(bucket, target_key, local_file)

    # --- Processing with Xarray ---
    # Note: Requires cfgrib and eccodes installed in the runner
    try:
        ds = xr.open_dataset(local_file, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
        
        # Identify variables (names vary by model)
        # Standardize names: t2m (temp), u10/v10 (wind)
        # This is a simplified mapping logic
        temp_var = None
        u_var = None
        v_var = None

        for v in ds.data_vars:
            if v in ['t2m', 'tmp', '2t']: temp_var = v
            if v in ['u10', 'ugrd', '10u']: u_var = v
            if v in ['v10', 'vgrd', '10v']: v_var = v

        # Generate Temp Map
        if temp_var:
            plt.figure(figsize=(10, 5))
            data = ds[temp_var] - 273.15 # Kelvin to C
            data.plot(cmap='jet', vmin=-30, vmax=40)
            plt.title(f"{model_name} - Temperature (°C) - {date_str}")
            plt.axis('off')
            plt.savefig(f"assets/{model_name}_temp.png", bbox_inches='tight', pad_inches=0)
            plt.close()

        # Generate Wind Map
        if u_var and v_var:
            plt.figure(figsize=(10, 5))
            wind_speed = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
            wind_speed.plot(cmap='viridis', vmin=0, vmax=30)
            plt.title(f"{model_name} - Wind Speed (m/s) - {date_str}")
            plt.axis('off')
            plt.savefig(f"assets/{model_name}_wind.png", bbox_inches='tight', pad_inches=0)
            plt.close()

    except Exception as e:
        print(f"Error processing {model_name}: {e}")

# Run for all
os.makedirs("assets", exist_ok=True)
for name, bucket in BUCKETS.items():
    download_and_plot(name, bucket)
