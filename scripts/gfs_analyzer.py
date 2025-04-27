import cfgrib
import numpy as np
import json
import glob
import os
import re
from datetime import datetime
import pandas as pd

def convert_gfs_to_json(spot_lat, spot_lon, input_directory="gfs_wind_data", output_file="forecast_combined.json"):
    """
    Extract wind data from GFS GRIB2 files and save as JSON.
    
    Args:
        spot_lat (float): Target latitude
        spot_lon (float): Target longitude
        input_directory (str): Directory containing GFS files
        output_file (str): Output JSON filename
    """
    print(f"Extracting wind data for coordinates: {spot_lat}°N, {spot_lon}°E")
    
    # Get all forecast files from the specified directory
    input_pattern = os.path.join(input_directory, "gfs_*_f*.grib2")
    files = sorted(glob.glob(input_pattern), 
                   key=lambda x: int(re.search(r'_f(\d+)\.grib2$', x).group(1)))
    
    if not files:
        print(f"❌ No files found matching pattern: {input_pattern}")
        return False
        
    print(f"Found {len(files)} forecast files in {input_directory}")
    
    data = []
    errors = []
    
    # Process each file
    for i, file in enumerate(files):
        try:
            # Extract forecast step from filename for logging
            step_match = re.search(r'_f(\d+)\.grib2$', file)
            step = step_match.group(1) if step_match else "unknown"
            
            print(f"Processing file {i+1}/{len(files)}: step f{step}")
            
            # Open the GRIB file
            ds = cfgrib.open_dataset(file)
            
            # Find nearest grid point
            latitudes = ds.latitude.values
            longitudes = ds.longitude.values
            
            # Handle longitude wrapping (0-360 vs -180 to 180)
            if spot_lon < 0 and np.all(longitudes >= 0):
                search_lon = spot_lon + 360
            else:
                search_lon = spot_lon
                
            nearest_lat_idx = np.abs(latitudes - spot_lat).argmin()
            nearest_lon_idx = np.abs(longitudes - search_lon).argmin()
            
            nearest_lat = latitudes[nearest_lat_idx]
            nearest_lon = longitudes[nearest_lon_idx]
            
            # Extract wind components
            u10 = float(ds['u10'].sel(latitude=nearest_lat, longitude=nearest_lon).values)
            v10 = float(ds['v10'].sel(latitude=nearest_lat, longitude=nearest_lon).values)
            
            # Try to get gusts if available
            try:
                gust = float(ds['gust'].sel(latitude=nearest_lat, longitude=nearest_lon).values)
                gust_kt = gust * 1.94384  # m/s to knots
            except (KeyError, ValueError):
                gust_kt = None
            
            # Calculate wind speed and direction
            wind_speed_ms = np.sqrt(u10**2 + v10**2)
            wind_speed_kt = wind_speed_ms * 1.94384  # m/s to knots
            
            # Direction FROM which the wind is blowing (meteorological)
            # Arctan2 gives direction wind is going TO, so we need to add 180 degrees
            wind_dir = (np.arctan2(-u10, -v10) * 180 / np.pi) % 360
            
            # Get valid time for this forecast
            try:
                # Try to parse the valid_time attribute directly
                forecast_time = pd.Timestamp(ds.valid_time.values).strftime('%Y-%m-%d %H:%M:%S')
            except:
                # If that fails, try to extract from filename and step
                match = re.search(r'gfs_(\d{8})_(\d{2})_f(\d+)', file)
                if match:
                    date_str, cycle, step = match.groups()
                    base_time = datetime.strptime(f"{date_str} {cycle}", "%Y%m%d %H")
                    hours = int(step)
                    forecast_time = (base_time + pd.Timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    forecast_time = f"Unknown (file: {os.path.basename(file)})"
            
            # Add to our data list
            data.append({
                "datetime": forecast_time,
                "wind_speed_kt": round(float(wind_speed_kt), 1),
                "wind_dir_deg": round(float(wind_dir), 1),
                "wind_gust_kt": round(float(gust_kt), 1) if gust_kt is not None else None,
                "source_file": os.path.basename(file),
                "grid_point": {
                    "latitude": float(nearest_lat),
                    "longitude": float(nearest_lon)
                }
            })
            
        except Exception as e:
            print(f"❌ Error processing {file}: {str(e)}")
            errors.append(f"{file}: {str(e)}")
    
    if not data:
        print("❌ No data was extracted from files")
        return False
    
    # Sort by datetime
    data = sorted(data, key=lambda x: x["datetime"])
    
    # Prepare final output
    output = {
        "location": {
            "latitude": spot_lat,
            "longitude": spot_lon
        },
        "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "forecast_count": len(data),
        "forecasts": data
    }
    
    # Save JSON
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
        
    print(f"✅ Successfully created {output_file} with {len(data)} forecast points")
    
    if errors:
        print(f"⚠️ Encountered {len(errors)} errors while processing files")
        
    return True

if __name__ == "__main__":
    # Configure your location
    spot_lat = 47.128  # latitude in decimal degrees
    spot_lon = 7.229   # longitude in decimal degrees
    
    # Run the converter
    convert_gfs_to_json(
        spot_lat=spot_lat, 
        spot_lon=spot_lon,
        input_directory="gfs_wind_data",  # Directory containing GFS files
        output_file="forecast_combined.json"
    )