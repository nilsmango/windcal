import requests
import os
import time
import glob
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
            # This is a heuristic, sometimes it's faster, sometimes slower
            if (now - cycle_time).total_seconds() < 4.5 * 3600: # Adjusted slightly
                print(f"Skipping cycle {date_str}/{cycle} as it's too recent.")
                continue
                
            # Test if the initial (f000) file exists
            test_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f000"
            try:
                response = requests.head(test_url, timeout=10)
                if response.status_code == 200:
                    print(f"Found available GFS cycle: {date_str} cycle {cycle}Z")
                    return date_str, cycle
            except requests.RequestException:
                print(f"Could not access test URL for {date_str}/{cycle}: {test_url}")
                pass
    
    raise Exception("Could not find an available GFS cycle within the last 2 days")

def clean_output_directory(output_dir, current_date_str=None, current_cycle=None):
    """Remove old GFS data files from the output directory."""
    if not os.path.exists(output_dir):
        return
        
    # If we have current date and cycle information, only delete older files
    if current_date_str and current_cycle:
        print(f"Cleaning up old files (keeping files from run {current_date_str}_{current_cycle})...")
        pattern = os.path.join(output_dir, "gfs_*.grib2")
        current_prefix = f"gfs_{current_date_str}_{current_cycle}_"
        
        for file_path in glob.glob(pattern):
            file_name = os.path.basename(file_path)
            # Keep files from the current run
            if file_name.startswith(current_prefix):
                continue
            # Delete older files
            try:
                os.remove(file_path)
                # print(f"Deleted old file: {file_name}") # Uncomment for verbose cleaning
            except OSError as e:
                print(f"Error deleting {file_name}: {e}")
    else:
        # If no current run info provided, delete all files
        print("Cleaning up all existing files in the output directory...")
        pattern = os.path.join(output_dir, "gfs_*.grib2")
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                # print(f"Deleted: {os.path.basename(file_path)}") # Uncomment for verbose cleaning
            except OSError as e:
                print(f"Error deleting {os.path.basename(file_path)}: {e}")
    print("Cleaning complete.")


def download_gfs_wind_data(output_dir=None, max_retries=5, retry_delay=60, clean_old_files=True): # Increased retries and delay
    """Download the latest available GFS wind data."""
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = "."
        
    # Find latest available run
    date_str, cycle = get_latest_gfs_cycle()
    print(f"Using GFS run: {date_str} cycle {cycle}Z")
    
    # Clean up old files if requested
    if clean_old_files:
        clean_output_directory(output_dir, date_str, cycle)
    
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    
    # Define parameters for the filtered data request
    base_params = {
        "lev_10_m_above_ground": "on",
        "lev_surface": "on",
        "var_UGRD": "on",
        "var_VGRD": "on", 
        "var_GUST": "on", # Gust is also usually available
        "subregion": "",
        "leftlon": 0,
        "rightlon": 360,
        "toplat": 90,
        "bottomlat": -90,
        "dir": f"/gfs.{date_str}/{cycle}/atmos"
    }
    
    downloaded_files = []
    fxx_int = 0 # Start at forecast hour 0
    max_fxx = 384 # GFS 0.25 goes up to 384 hours (16 days)
    consecutive_failures = 0
    max_consecutive_failures = 5 # Allow a few more failures before stopping

    print(f"Starting download loop from f{fxx_int:03d} up to f{max_fxx:03d}...")

    while fxx_int <= max_fxx and consecutive_failures < max_consecutive_failures:
        fxx = f"{fxx_int:03d}"
        file_name = f"gfs.t{cycle}z.pgrb2.0p25.f{fxx}"
        output_file = os.path.join(output_dir, f"gfs_{date_str}_{cycle}_f{fxx}.grib2")
        
        # Skip if already downloaded
        if os.path.exists(output_file):
            print(f"✅ File already exists: {output_file}")
            downloaded_files.append(output_file)
            consecutive_failures = 0 # Reset consecutive failures on success
            # Determine the next forecast hour based on intervals
            if fxx_int < 120:
                fxx_int += 1
            else:
                fxx_int += 3
            continue # Move to the next iteration

        params = base_params.copy()
        params["file"] = file_name
        
        # Try a few times in case of temporary failures
        download_success = False
        for attempt in range(max_retries):
            try:
                print(f"Downloading forecast step {fxx} ({fxx_int} hours) (attempt {attempt+1}/{max_retries})...")
                response = requests.get(base_url, params=params, timeout=180) # Increased timeout

                if response.status_code == 200 and len(response.content) > 100000: # Must be reasonably sized
                    with open(output_file, "wb") as f:
                        f.write(response.content)
                    print(f"✅ Successfully downloaded {output_file}")
                    downloaded_files.append(output_file)
                    consecutive_failures = 0 # Reset consecutive failures on success
                    download_success = True
                    break # Exit retry loop on success
                else:
                    error_msg = f"Failed with status {response.status_code}" if response.status_code != 200 else "Response too small"
                    print(f"⚠️ Attempt {attempt+1} for f{fxx}: {error_msg}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            except requests.RequestException as e:
                print(f"⚠️ Attempt {attempt+1} for f{fxx}: Error - {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        # After attempting downloads for the current fxx_int:
        if download_success:
            # Determine the next forecast hour based on intervals
            if fxx_int < 120:
                fxx_int += 1
            else:
                fxx_int += 3
        else:
            # Download failed after max retries for this fxx_int
            consecutive_failures += 1
            print(f"❌ Failed to download forecast step {fxx} ({fxx_int} hours) after {max_retries} attempts. Consecutive failures: {consecutive_failures}")
            
            # Even if download failed, determine the next hour to *try* based on intervals.
            # This ensures we skip over expected missing files (like f121, f122)
            # and try the next available one (like f123).
            if fxx_int < 120:
                 fxx_int += 1
            else:
                 fxx_int += 3


        # Small pause to avoid hammering the server
        time.sleep(0.5) # Slightly reduced pause

    if consecutive_failures >= max_consecutive_failures:
         print(f"Reached maximum consecutive failures ({max_consecutive_failures}), stopping download. This likely means the full forecast run is not yet available or has ended.")
    else:
         print(f"Finished download loop. Last attempted forecast hour was {fxx_int - (1 if fxx_int <= 120 else 3)}.")


    return downloaded_files

if __name__ == "__main__":
    output_directory = "gfs_wind_data" # Change this if you want
    
    # Set clean_old_files=True to remove old files (default is True)
    # Increased max_retries and retry_delay for potentially large files
    files = download_gfs_wind_data(output_directory, max_retries=7, retry_delay=90, clean_old_files=True)
    print(f"Successfully downloaded {len(files)} GFS forecast files")