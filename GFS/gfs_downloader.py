import requests
import os
import time
from datetime import datetime, timedelta

def get_latest_gfs_cycle():
    """Find the latest available GFS cycle by checking recent runs."""
    # GFS runs 4 times daily at 00, 06, 12, 18 UTC
    cycles = ["00", "06", "12", "18"]
    now = datetime.utcnow()
    
    # Try today and yesterday (in case we're right after midnight)
    for days_back in range(2):
        check_date = now - timedelta(days=days_back)
        date_str = check_date.strftime("%Y%m%d")
        
        # Check cycles in reverse order (most recent first)
        for cycle in sorted(cycles, reverse=True):
            # Skip future cycles
            cycle_time = datetime.strptime(f"{date_str}{cycle}", "%Y%m%d%H")
            if cycle_time > now:
                continue
                
            # Need to allow ~4-5 hours for GFS run to become available
            if (now - cycle_time).total_seconds() < 5 * 3600:
                continue
                
            test_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f000"
            try:
                response = requests.head(test_url, timeout=10)
                if response.status_code == 200:
                    return date_str, cycle
            except requests.RequestException:
                pass
    
    raise Exception("Could not find an available GFS cycle")

def download_gfs_wind_data(output_dir=None, max_retries=3, retry_delay=30):
    """Download the latest available GFS wind data."""
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = "."
        
    # Find latest available run
    date_str, cycle = get_latest_gfs_cycle()
    print(f"Found latest GFS run: {date_str} cycle {cycle}Z")
    
    # For wind data, we typically want forecasts up to 5 days (120 hours)
    # but let's try to get as many steps as available (up to 384 hours/16 days for 0.25° data)
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    
    # Define parameters for the filtered data request
    base_params = {
        "lev_10_m_above_ground": "on",
        "lev_surface": "on",
        "var_UGRD": "on",
        "var_VGRD": "on", 
        "var_GUST": "on",
        "subregion": "",
        "leftlon": 0,
        "rightlon": 360,
        "toplat": 90,
        "bottomlat": -90,
        "dir": f"/gfs.{date_str}/{cycle}/atmos"
    }
    
    downloaded_files = []
    step = 0
    consecutive_failures = 0
    max_consecutive_failures = 3  # Stop after this many consecutive failures

    while consecutive_failures < max_consecutive_failures:
        fxx = f"{step:03d}"
        file_name = f"gfs.t{cycle}z.pgrb2.0p25.f{fxx}"
        output_file = os.path.join(output_dir, f"gfs_{date_str}_{cycle}_f{fxx}.grib2")
        
        # Skip if already downloaded
        if os.path.exists(output_file):
            print(f"✅ File already exists: {output_file}")
            downloaded_files.append(output_file)
            step += 1
            consecutive_failures = 0
            continue
            
        params = base_params.copy()
        params["file"] = file_name
        
        # Try a few times in case of temporary failures
        for attempt in range(max_retries):
            try:
                print(f"Downloading forecast step {fxx} (attempt {attempt+1}/{max_retries})...")
                response = requests.get(base_url, params=params, timeout=120)
                
                if response.status_code == 200 and len(response.content) > 100000:  # Must be reasonably sized
                    with open(output_file, "wb") as f:
                        f.write(response.content)
                    print(f"✅ Successfully downloaded {output_file}")
                    downloaded_files.append(output_file)
                    consecutive_failures = 0
                    break
                else:
                    error_msg = f"Failed with status {response.status_code}" if response.status_code != 200 else "Response too small"
                    print(f"⚠️ Attempt {attempt+1}: {error_msg}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            except requests.RequestException as e:
                print(f"⚠️ Attempt {attempt+1}: Error - {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        else:
            # All attempts failed for this step
            consecutive_failures += 1
            print(f"❌ Failed to download forecast step {fxx} after {max_retries} attempts")
            
            # If we've already downloaded some files and start hitting failures,
            # it probably means we've reached the end of available forecast steps
            if downloaded_files:
                print("Reached the end of available forecast steps")
                break
                
        # Move to next step
        step += 1
        
        # Small pause to avoid hammering the server
        time.sleep(1)
    
    return downloaded_files

if __name__ == "__main__":
    output_directory = "gfs_wind_data"  # Change this if you want
    files = download_gfs_wind_data(output_directory)
    print(f"Downloaded {len(files)} GFS forecast files")