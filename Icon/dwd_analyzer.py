import xarray as xr
import cfgrib
import numpy as np
import math
from datetime import datetime, timedelta
import os
import glob
import re
import json # Import json for exporting data
import argparse # Import argparse for command-line arguments
import traceback # Import traceback for detailed error printing
import bz2 # Import bz2 for decompression
import tempfile # Import tempfile for creating temporary files
import shutil # Import shutil for rmtree
from ics import Calendar, Event
from datetime import datetime, timedelta

def decompress_bz2_grib(bz2_filepath: str, output_dir: str):
    """
    Decompresses a .grib2.bz2 file to a .grib2 file in a specified output directory.

    Args:
        bz2_filepath: The path to the input .grib2.bz2 file.
        output_dir: The directory where the decompressed .grib2 file will be saved.

    Returns:
        The path to the decompressed .grib2 file, or None if decompression fails.
    """
    if not os.path.exists(bz2_filepath):
        # Print a more specific error here as this function is called per file
        print(f"Decompression Error: Input file not found: {bz2_filepath}")
        return None

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Define the output filename (remove .bz2 extension)
    output_filename = os.path.basename(bz2_filepath).replace('.bz2', '')
    output_filepath = os.path.join(output_dir, output_filename)

    try:
        with bz2.open(bz2_filepath, 'rb') as f_in:
            with open(output_filepath, 'wb') as f_out:
                for chunk in iter(lambda: f_in.read(4096), b''):
                    f_out.write(chunk)
        return output_filepath
    except Exception as e:
        print(f"Decompression Error: Failed to decompress {bz2_filepath}: {e}")
        return None

def get_full_wind_forecast_robust_json(base_icon_eu_dir: str, latitude: float, longitude: float):
    """
    Reads wind data for all available forecast steps for a specific
    initialization time from local DWD ICON-EU GRIB files compressed with bz2.
    Decompresses files to temporary files before reading.
    The initialization time and forecast hour are automatically determined
    from the filenames. Processes each forecast hour independently to skip
    problematic files. Calculates validity time manually. Exports the results
    as a JSON array.

    Args:
        base_icon_eu_dir: The base directory containing the variable subdirectories
                          (e.g., 'path/to/your/dwd/icon-eu').
        latitude: The latitude of the location.
        longitude: The longitude of the location.

    Returns:
        A JSON formatted string containing an array of dictionaries,
        each with time, average 10-min wind speed (knots), wind gusts (knots),
        and wind direction (degrees) for each successfully processed forecast step,
        sorted by time. Returns None if a critical error occurs or no files are found.
    """
    # --- Identify Variables and construct expected subdirectory paths ---
    # Variable names as they appear *inside* the GRIB files (from previous error)
    u_avg_component_var_internal = 'u10'
    v_avg_component_var_internal = 'v10'
    gust_var_internal = 'fg10' # Assuming fg10 is the gust variable

    # Variable names as they appear *in the filenames* and directory names
    u_avg_component_var_filename = 'U_10M'
    v_avg_component_var_filename = 'V_10M'
    gust_var_filename = 'VMAX_10M'

    # Expected variable subdirectory names (lowercase based on typical file paths)
    u_avg_subdir = os.path.join(base_icon_eu_dir, u_avg_component_var_filename.lower())
    v_avg_subdir = os.path.join(base_icon_eu_dir, v_avg_component_var_filename.lower())
    gust_subdir = os.path.join(base_icon_eu_dir, gust_var_filename.lower())

    # Check if variable subdirectories exist
    required_subdirs = [u_avg_subdir, v_avg_subdir]
    for subdir in required_subdirs:
        if not os.path.isdir(subdir):
            print(f"Error: Required variable directory not found: {subdir}")
            return None

    has_gust_subdir = os.path.isdir(gust_subdir)
    if not has_gust_subdir:
        print(f"Warning: Gust variable directory not found: {gust_subdir}. Gust data will not be included in the output JSON.")

    # --- Determine Initialization Time and available forecast hours ---
    # Scan one of the variable directories (e.g., u_10m) to find all filenames
    # and extract the initialization time and forecast hour from each.
    # Updated glob pattern for .grib2.bz2
    sample_files = glob.glob(os.path.join(u_avg_subdir, '*.grib2.bz2'))
    if not sample_files:
        print(f"Error: No GRIB files found in the sample directory: {u_avg_subdir}")
        return None

    # Use a regular expression to extract the YYYYMMDDHH initialization time
    # and the FFF forecast hour from the filename.
    # Assuming the format icon-eu_europe_regular-lat-lon_single-level_YYYYMMDDHH_FFF_VARIABLE.grib2.bz2
    filename_regex = re.compile(r'icon-eu_europe_regular-lat-lon_single-level_(\d{10})_(\d{3})_.*\.grib2\.bz2')

    initialization_time_str = None
    available_forecast_hours = set()

    # Process sample files to get initialization time and available forecast hours
    for sample_file in sample_files:
        match = filename_regex.search(os.path.basename(sample_file))
        if match:
            current_init_time_str = match.group(1)
            forecast_hour_str = match.group(2)

            # Assume all files in this run have the same initialization time
            if initialization_time_str is None:
                initialization_time_str = current_init_time_str
                print(f"Detected initialization time: {initialization_time_str}")
            elif initialization_time_str != current_init_time_str:
                 print(f"Warning: Found files with different initialization times in {u_avg_subdir}. Using the first one found: {initialization_time_str}")
                 # Decide how to handle this in a production system (e.g., error, process separately)

            available_forecast_hours.add(forecast_hour_str)

    if initialization_time_str is None or not available_forecast_hours:
        print("Error: Could not determine initialization time or find any forecast hours from filenames.")
        return None

    # Sort forecast hours numerically for chronological processing
    sorted_forecast_hours = sorted(list(available_forecast_hours), key=int)

    print(f"Found {len(sorted_forecast_hours)} available forecast hours for initialization {initialization_time_str}.")

    # Parse initialization time string into a datetime object
    try:
        init_datetime = datetime.strptime(initialization_time_str, '%Y%m%d%H')
    except ValueError:
        print(f"Error: Could not parse initialization time string: {initialization_time_str}. Cannot calculate validity times.")
        return None

    # --- Process Data for Each Forecast Hour ---
    predictions = []
    # Create a temporary directory for decompressed files
    temp_dir = None

    try:
        temp_dir = tempfile.mkdtemp()
        print(f"Using temporary directory for decompression: {temp_dir}")

        for forecast_hour_str in sorted_forecast_hours:
            # print(f"Processing forecast hour: {forecast_hour_str}") # Uncomment for detailed hour processing log

            # Construct the expected filenames for this forecast hour (.grib2.bz2)
            # Use the variable names as they appear in the filenames (U_10M, V_10M, VMAX_10M)
            filename_template = f'icon-eu_europe_regular-lat-lon_single-level_{initialization_time_str}_{forecast_hour_str}_'

            u_avg_bz2_filepath = os.path.join(u_avg_subdir, f'{filename_template}{u_avg_component_var_filename}.grib2.bz2')
            v_avg_bz2_filepath = os.path.join(v_avg_subdir, f'{filename_template}{v_avg_component_var_filename}.grib2.bz2')
            gust_bz2_filepath = os.path.join(gust_subdir, f'{filename_template}{gust_var_filename}.grib2.bz2')


            # Decompress files for this hour
            u_avg_grib_filepath = None
            v_avg_grib_filepath = None
            gust_grib_filepath = None
            decompressed_files = []

            # Decompress required files (U_10M and V_10M)
            u_avg_grib_filepath = decompress_bz2_grib(u_avg_bz2_filepath, temp_dir)
            if u_avg_grib_filepath:
                decompressed_files.append(u_avg_grib_filepath)
            else:
                print(f"Error decompressing {u_avg_component_var_filename} file for hour {forecast_hour_str}. Skipping this hour.")
                continue # Skip to the next forecast hour

            v_avg_grib_filepath = decompress_bz2_grib(v_avg_bz2_filepath, temp_dir)
            if v_avg_grib_filepath:
                decompressed_files.append(v_avg_grib_filepath)
            else:
                print(f"Error decompressing {v_avg_component_var_filename} file for hour {forecast_hour_str}. Skipping this hour.")
                # Clean up decompressed U file before continuing
                if u_avg_grib_filepath and os.path.exists(u_avg_grib_filepath):
                    os.remove(u_avg_grib_filepath)
                continue # Skip to the next forecast hour

            # Decompress optional gust file if it exists
            if has_gust_subdir and os.path.exists(gust_bz2_filepath):
                 gust_grib_filepath = decompress_bz2_grib(gust_bz2_filepath, temp_dir)
                 if gust_grib_filepath:
                      decompressed_files.append(gust_grib_filepath)
                 else:
                      print(f"Warning: Error decompressing {gust_var_filename} file for hour {forecast_hour_str}. Gust data will be missing for this step.")


            try:
                # Open decompressed files for this specific forecast hour
                # Use backend_kwargs={'indexpath': ''} to prevent indexing issues
                # combine='by_coords' is still useful here to combine variables for the same time step
                ds_hour = xr.open_mfdataset(decompressed_files, engine='cfgrib', combine='by_coords',
                                            backend_kwargs={'indexpath': ''},
                                            # preprocess=lambda d: d.rename({list(d.data_vars)[0]: 'variable_name'})) # Example preprocess
                                            )

                # --- Select Data for Location for this hour ---
                try:
                    # Use the variable names as found by cfgrib (u10, v10, fg10)
                    u_avg_data = ds_hour[u_avg_component_var_internal].sel(latitude=latitude, longitude=longitude, method='nearest')
                    v_avg_data = ds_hour[v_avg_component_var_internal].sel(latitude=latitude, longitude=longitude, method='nearest')
                    gust_data = ds_hour[gust_var_internal].sel(latitude=latitude, longitude=longitude, method='nearest') if gust_var_internal in ds_hour.data_vars else None

                except KeyError as e:
                     print(f"Error selecting data for location or variable in data for hour {forecast_hour_str}: {e}. Skipping this hour.")
                     print("Available variables in this file:", list(ds_hour.data_vars))
                     continue # Skip to the next forecast hour


                # --- Extract and Process Data for this hour ---
                # Calculate validity time manually
                try:
                    forecast_hour_int = int(forecast_hour_str)
                    validity_time_obj = init_datetime + timedelta(hours=forecast_hour_int)
                except ValueError:
                     print(f"Error: Could not convert forecast hour string '{forecast_hour_str}' to integer. Skipping this hour.")
                     continue # Skip to the next forecast hour


                u_avg = u_avg_data.values.item()
                v_avg = v_avg_data.values.item()
                gust = gust_data.values.item() if gust_data is not None else np.nan


                # Calculate average wind speed from U_AV and V_AV components (in m/s)
                wind_speed_avg_ms = math.sqrt(u_avg**2 + v_avg**2)

                # Calculate average wind direction (in degrees)
                if u_avg == 0 and v_avg == 0:
                     wind_direction_met_deg = 0 # Or NaN
                else:
                     wind_direction_math_rad = math.atan2(u_avg, v_avg)
                     wind_direction_math_deg = math.degrees(wind_direction_math_rad)
                     wind_direction_met_deg = (wind_direction_math_deg + 180) % 360 # Meteorological direction


                # Convert speeds from m/s to knots (1 m/s = 1.94384 knots)
                wind_speed_avg_knots = round(wind_speed_avg_ms * 1.94384, 2)
                # Only convert gust if it's not NaN
                wind_gusts_knots = round(gust * 1.94384, 2) if not np.isnan(gust) else np.nan

                predictions.append({
                    "date": validity_time_obj.isoformat(), # Use 'date' as requested, storing ISO format
                    "wind_speed_avg_knots": wind_speed_avg_knots,
                    "wind_gusts_knots": wind_gusts_knots,
                    "wind_direction_degrees": round(wind_direction_met_deg, 2)
                })

            except Exception as e:
                print(f"Error processing data for forecast hour {forecast_hour_str}: {e}. Skipping this hour.")
                traceback.print_exc() # Print detailed traceback for other errors
                continue # Skip to the next forecast hour

            finally:
                # --- Clean up decompressed files for this hour ---
                for f in decompressed_files:
                    if os.path.exists(f):
                        os.remove(f)
                # print(f"Cleaned up temporary files for hour {forecast_hour_str}") # Uncomment for debugging cleanup

    except Exception as e:
         print(f"An error occurred during the main processing loop: {e}")
         traceback.print_exc()
         return None

    finally:
        # --- Clean up the temporary directory ---
        if temp_dir and os.path.exists(temp_dir):
            try:
                # Use shutil.rmtree to remove the directory and its contents
                shutil.rmtree(temp_dir, ignore_errors=True)
                # print(f"Cleaned up temporary directory: {temp_dir}") # Uncomment for debugging cleanup
            except Exception as e:
                 print(f"Error cleaning up temporary directory {temp_dir}: {e}")


    if not predictions:
        print("No valid wind predictions were successfully processed.")
        return None

    # Sort predictions by time (should be mostly sorted by processing order, but good practice)
    predictions.sort(key=lambda x: x['date'])

    # Convert the list of dictionaries to a JSON formatted string
    json_output = json.dumps(predictions, indent=4)

    return json_output

# --- Command Line Argument Parsing ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get DWD ICON-EU wind forecast from local GRIB.bz2 files.')
    parser.add_argument('base_icon_eu_dir', type=str,
                        help='The base directory containing the variable subdirectories (e.g., /path/to/your/dwd/icon-eu)')
    parser.add_argument('latitude', type=float,
                        help='The latitude of the location')
    parser.add_argument('longitude', type=float,
                        help='The longitude of the location')
    parser.add_argument('min_gusts_knots', type=float,
                        help='Your min gusts in knots')

    args = parser.parse_args()

    print(f"Attempting to read full wind forecast from base directory {args.base_icon_eu_dir} at Latitude: {args.latitude}, Longitude: {args.longitude}, Min Gusts: {args.min_gusts_knots}...")

    full_wind_forecast_json_output = get_full_wind_forecast_robust_json(
        args.base_icon_eu_dir,
        args.latitude,
        args.longitude
    )

    if full_wind_forecast_json_output:
        print("\nFull Wind Forecast (JSON Output):")
        print("-" * 40)
        print(full_wind_forecast_json_output)
        print("-" * 40)
    else:
        print("\nCould not retrieve the full wind forecast from the GRIB files.")

    forecast = json.loads(full_wind_forecast_json_output)
    filtered = [entry for entry in forecast if entry["wind_gusts_knots"] >= args.min_gusts_knots]
    
    calendar = Calendar()
    
    for i, entry in enumerate(filtered):
        start = datetime.fromisoformat(entry["date"])
        end = datetime.fromisoformat(filtered[i + 1]["date"]) if i + 1 < len(filtered) else start + timedelta(hours=1)
    
        e = Event()
        e.name = "Wind: Gust ≥ {:.1f}kt".format(entry["wind_gusts_knots"])
        e.begin = start
        e.end = end
        e.description = f"Gusts: {entry['wind_gusts_knots']}kt\nAvg: {entry['wind_speed_avg_knots']}kt\nDirection: {entry['wind_direction_degrees']}°"
        calendar.events.add(e)
    
    with open("wind_forecast.ics", "w") as f:
        f.writelines(calendar)
    print("\nDone MF.")