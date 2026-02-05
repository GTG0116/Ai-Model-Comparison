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
# specific naming prefixes for the NOAA buckets
BUCKET_CONFIG = {
    "EURO-AIFS": {
        "bucket": "ecmwf-forecasts",
        "source": "ecmwf"
    },
    "EAGLE-GraphCast": {
        "bucket": "noaa-nws-graphcastgfs-pds",
        "source": "noaa",
        "prefix_base": "graphcastgfs"
    },
    "FourCastNet": {
        "bucket": "noaa-nws-fourcastnetgfs-pds",
        "source": "noaa",
        "prefix_base": "fcngfs"
    }
}

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def find_latest_data(model_name, config):
    """
    Scans the last 5 days to find the correct S3 prefix and file key.
    Handles differences between NOAA (fcngfs/graphcast) and ECMWF structures.
    """
    bucket = config['bucket']
    source = config['source']
    
    date = datetime.utcnow()
    
    # Try last 5 days
    for i in range(5):
        date_str = date.strftime('%Y%m%d')
        
        # --- Construct Prefix Candidates ---
        prefixes_to_check = []
        
        if source == "noaa":
            base = config['prefix_base']
            # NOAA Format: [model].[date]/[hour]/forecasts/
            # We check 00z, then 12z, then 06z, 18z
            for hour in ["00", "12", "06", "18"]:
                prefixes_to_check.append(f"{base}.{date_str}/{hour}/forecasts/")
                prefixes_to_check.append(f"{base}.{date_str}/{hour}/") # Fallback without 'forecasts'
        else:
            # ECMWF Format: [date]/00z/aifs/0p25/oper/
            prefixes_to_check.append(f"{date_str}/00z/aifs/0p25/oper/")
            prefixes_to_check.append(f"{date_str}/12z/aifs/0p25/oper/")

        # --- Check S3 for each candidate ---
        for prefix in prefixes_to_check:
            print(f"[{model_name}] Checking: s3://{bucket}/{prefix}")
            
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in resp:
                # We found data! Now find the specific target file.
                for obj in resp['Contents']:
                    key = obj['Key']
                    
                    # File Filtering Logic
                    if source == "noaa":
                        # Look for forecast hour 12 (f012) or 6 (f006) to show movement
                        # NOAA files usually look like: ...f012.grib2
                        if key.endswith(".grib2") and ("f012" in key or "f006" in key):
                            print(f"[{model_name}] ✅ Found file: {key}")
                            return key, date_str
                            
                    elif source == "ecmwf":
                        # ECMWF files are often just "date-run-step.grib2"
                        # We take the first valid grib2
                        if key.endswith(".grib2"):
                            print(f"[{model_name}] ✅ Found file: {key}")
                            return key, date_str
                            
        # Move to previous day if nothing found today
        date -= timedelta(days=1)
        
    return None, None

def process_model(model_name):
    config = BUCKET_CONFIG[model_name]
    bucket = config['bucket']
    
    print(f"\n--- Starting {model_name} ---")
    
    # 1. Find Data
    key, date_str = find_latest_data(model_name, config)
    if not key:
        print(f"❌ Failed to find data for {model_name} after checking last 5 days.")
        return False

    # 2. Download
    local_filename = f"{model_name}.grib2"
    try:
        print(f"Downloading {key}...")
        s3.download_file(bucket, key, local_filename)
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

    # 3. Process Image
    try:
        # Load GRIB2
        # 'filter_by_keys' is critical for large GRIB files to pick just the surface level
        ds = xr.open_dataset(local_filename, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})
        
        # Normalize Variable Names
        # Different models call Temperature different things (t2m, tmp, 2t, etc.)
        data_vars = list(ds.data_vars)
        
        t_var = next((v for v in data_vars if v in ['t2m', 'tmp', '2t']), None)
        u_var = next((v for v in data_vars if v in ['u10', 'ugrd', '10u']), None)
        v_var = next((v for v in data_vars if v in ['v10', 'vgrd', '10v']), None)
        
        # Generate Temp Map
        if t_var:
            plt.figure(figsize=(10, 5))
            # Convert Kelvin to Celsius
            temps = ds[t_var] - 273.15 
            temps.plot(cmap='jet', vmin=-30, vmax=40, add_labels=False)
            plt.axis('off')
            plt.title(f"{model_name} Temp ({date_str})")
            plt.savefig(f"assets/{model_name}_temp.png", bbox_inches='tight', pad_inches=0)
            plt.close()
            print(f"✅ Saved {model_name}_temp.png")
        else:
            print(f"⚠️ {model_name}: No temperature variable found in {data_vars}")

        # Generate Wind Map
        if u_var and v_var:
            plt.figure(figsize=(10, 5))
            wind = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
            wind.plot(cmap='viridis', vmin=0, vmax=30, add_labels=False)
            plt.axis('off')
            plt.title(f"{model_name} Wind ({date_str})")
            plt.savefig(f"assets/{model_name}_wind.png", bbox_inches='tight', pad_inches=0)
            plt.close()
            print(f"✅ Saved {model_name}_wind.png")
        else:
             print(f"⚠️ {model_name}: No wind variables found in {data_vars}")

        return True

    except Exception as e:
        print(f"❌ Error processing GRIB data for {model_name}: {e}")
        return False

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs("assets", exist_ok=True)
    
    success_count = 0
    for model in BUCKET_CONFIG.keys():
        if process_model(model):
            success_count += 1
            
    if success_count == 0:
        print("\n❌ CRITICAL: No images generated for any model.")
        sys.exit(1)
    else:
        print(f"\n✅ Success! Generated data for {success_count} models.")
