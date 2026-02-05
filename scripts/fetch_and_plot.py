import os
import sys
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# --- 2026 Updated Bucket Configuration ---
MODELS = {
    "EURO-AIFS": {
        "bucket": "ecmwf-forecasts",
        "type": "ecmwf",
        "path_template": "{date}/{hour}z/aifs/0p25/oper/",
        "file_pattern": "0p25_oper.grib2"
    },
    "EAGLE-AIGFS": { # Formerly EAGLE-GraphCast
        "bucket": "noaa-nws-graphcastgfs-pds",
        "type": "noaa",
        "path_template": "graphcastgfs.{date}/{hour}/forecasts_13_levels/",
        "file_prefix": "graphcastgfs.t{hour}z.pgrb2.0p25.f012"
    },
    "FourCastNet": {
        "bucket": "noaa-nws-fourcastnetgfs-pds",
        "type": "noaa",
        "path_template": "fcngfs.{date}/{hour}/",
        "file_prefix": "fcngfs.t{hour}z.pgrb2.0p25.f012"
    }
}

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def get_latest_available_file(model_name, cfg):
    """Searches last 48 hours for the most recent valid GRIB2 file."""
    now = datetime.utcnow()
    # Check every 6-hour cycle for the last 2 days
    for delta_hours in range(0, 48, 6):
        test_time = now - timedelta(hours=delta_hours)
        date_str = test_time.strftime("%Y%m%d")
        # Ensure hour is 00, 06, 12, or 18
        hour_str = f"{(test_time.hour // 6) * 6:02d}"
        
        prefix = cfg["path_template"].format(date=date_str, hour=hour_str)
        print(f"[{model_name}] Checking s3://{cfg['bucket']}/{prefix}...")
        
        resp = s3.list_objects_v2(Bucket=cfg["bucket"], Prefix=prefix)
        if 'Contents' in resp:
            # Find a file matching our needs
            for obj in resp['Contents']:
                key = obj['Key']
                if cfg["type"] == "ecmwf" and key.endswith(".grib2"):
                    return cfg['bucket'], key, date_str, hour_str
                if cfg["type"] == "noaa" and cfg["file_prefix"] in key and key.endswith(".grib2"):
                    return cfg['bucket'], key, date_str, hour_str
    return None, None, None, None

def process_model(name, cfg):
    bucket, key, date, hour = get_latest_available_file(name, cfg)
    if not key:
        print(f"❌ No data found for {name} in the last 48 hours.")
        return False

    local_file = f"{name}.grib2"
    print(f"✅ Found! Downloading: {key}")
    s3.download_file(bucket, key, local_file)

    try:
        # Load surface variables only to save memory/time
        ds = xr.open_dataset(local_file, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
        
        # Mapping standard GRIB variable names
        t_var = next((v for v in ds.data_vars if v in ['2t', 't2m', 'tmp']), None)
        u_var = next((v for v in ds.data_vars if v in ['10u', 'u10', 'ugrd']), None)
        v_var = next((v for v in ds.data_vars if v in ['10v', 'v10', 'vgrd']), None)

        # Plot Temperature
        if t_var:
            plt.figure(figsize=(12, 6))
            (ds[t_var] - 273.15).plot(cmap='magma', vmin=-20, vmax=45, add_labels=False)
            plt.title(f"{name} 2m Temp - {date} {hour}Z")
            plt.axis('off')
            plt.savefig(f"assets/{name}_temp.png", bbox_inches='tight')
            plt.close()

        # Plot Wind Speed
        if u_var and v_var:
            plt.figure(figsize=(12, 6))
            wind = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
            wind.plot(cmap='viridis', vmin=0, vmax=40, add_labels=False)
            plt.title(f"{name} 10m Wind - {date} {hour}Z")
            plt.axis('off')
            plt.savefig(f"assets/{name}_wind.png", bbox_inches='tight')
            plt.close()
        
        return True
    except Exception as e:
        print(f"❌ Error processing {name}: {e}")
        return False

if __name__ == "__main__":
    os.makedirs("assets", exist_ok=True)
    results = [process_model(n, c) for n, c in MODELS.items()]
    if not any(results):
        print("CRITICAL: Failed to generate any images.")
        sys.exit(1)
