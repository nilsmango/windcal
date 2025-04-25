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

def get_full_wind_forecast_json(base_icon_eu_dir: str, latitude: float, longitude: float):
    """
    Reads wind data for all available forecast steps for a specific
    initialization time from local DWD ICON-EU GRIB files compressed with bz2.
    The initialization time and forecast hour are automatically determined
    from the filenames. Exports the results as a JSON array.

    Args:
        base_icon_eu_dir: The base directory containing the variable subdirectories
                          (e.g., 'path/to/your/dwd/icon-eu').
        latitude: The latitude of the location.
        longitude: The longitude of the location.

    Returns:
        A JSON formatted string containing an array of dictionaries,
        each with time, average 10-min wind speed (knots), wind gusts (knots),
        and wind direction (degrees) for each forecast step, sorted by time.
        Returns None if an error occurs or no files are found.
    """
    # --- Identify Variables and construct expected subdirectory paths ---
    u_avg_component_var = 'U_10M'
    v_avg_component_var = 'V_10M'
    gust_var = 'VMAX_10M'

    # Expected variable subdirectory names (lowercase based on typical file paths)
    u_avg_subdir = os.path.join(base_icon_eu_dir, u_avg_component_var.lower())
    v_avg_subdir = os.path.join(base_icon_eu_dir, v_avg_component_var.lower())
    gust_subdir = os.path.join(base_icon_eu_dir, gust_var.lower())

    # Check if variable subdirectories exist
    required_subdirs = [u_avg_subdir, v_avg_subdir]
    for subdir in required_subdirs:
        if not os.path.isdir(subdir):
            print(f"Error: Required variable directory not found: {subdir}")
            return None

    has_gust_subdir = os.path.isdir(gust_subdir)
    if not has_gust_subdir:
        print(f"Warning: Gust variable directory not found: {gust_subdir}. Gust data will not be included.")

    # --- Determine Initialization Time and collect files ---
    # Scan one of the variable directories (e.g., u_10m_av) to find all filenames
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
    all_grib_files = []
    processed_forecast_steps = set() # Keep track of forecast steps to avoid duplicates if files are listed multiple times

    # Process files from u_10m_av to get initialization time and forecast steps
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

            # Add files for all relevant variables for this forecast hour
            if forecast_hour_str not in processed_forecast_steps:
                processed_forecast_steps.add(forecast_hour_str)

                # Updated filename template for .grib2.bz2
                filename_template = f'icon-eu_europe_regular-lat-lon_single-level_{initialization_time_str}_{forecast_hour_str}_'

                # Updated file paths for .grib2.bz2 - using .upper() for variable names as in your example filename
                u_avg_filepath = os.path.join(u_avg_subdir, f'{filename_template}{u_avg_component_var.upper()}.grib2.bz2')
                v_avg_filepath = os.path.join(v_avg_subdir, f'{filename_template}{v_avg_component_var.upper()}.grib2.bz2')
                gust_filepath = os.path.join(gust_subdir, f'{filename_template}{gust_var.upper()}.grib2.bz2')


                # Add files if they exist
                if os.path.exists(u_avg_filepath) and os.path.exists(v_avg_filepath):
                    all_grib_files.append(u_avg_filepath)
                    all_grib_files.append(v_avg_filepath)
                    if os.path.exists(gust_filepath):
                        all_grib_files.append(gust_filepath)
                    else:
                        print(f"Warning: Gust file not found for forecast hour {forecast_hour_str}: {gust_filepath}. Gust data will be missing for this step.")
                else:
                    print(f"Error: Required average wind files not found for forecast hour {forecast_hour_str}. Skipping this step.")
                    print(f"Missing: {u_avg_filepath}, {v_avg_filepath}")


    if not all_grib_files:
        print(f"Error: No complete sets of GRIB files found for initialization time {initialization_time_str} across specified variable directories.")
        return None

    print(f"Found {len(all_grib_files)} GRIB files for processing for initialization {initialization_time_str}.")

    try:
        # Open multiple GRIB files as a single dataset
        # combine='by_coords' helps xarray align data based on coordinates (time, lat, lon)
        # cfgrib should handle the .bz2 decompression automatically
        # Removed suppress_errors='warning' as it caused an error
        ds = xr.open_mfdataset(all_grib_files, engine='cfgrib', combine='by_coords',
                               # suppress_errors='warning', # Removed this argument
                               # Add a preprocess function if needed, e.g., to ensure consistent variable names
                               # preprocess=lambda d: d.rename({list(d.data_vars)[0]: 'variable_name'})) # Example preprocess
                               )

        # Ensure the dataset has the expected variables after opening
        required_vars = [u_avg_component_var, v_avg_component_var]
        for var in required_vars:
            if var not in ds.data_vars:
                # Sometimes cfgrib might rename variables, check original_name attribute
                found_var = False
                for ds_var_name, data_array in ds.data_vars.items():
                    # Check original_name attribute if available
                    if 'GRIB_originalName' in data_array.attrs and data_array.attrs['GRIB_originalName'].lower() == var.lower():
                         print(f"Warning: Variable '{var}' not found directly, but found a variable with matching original_name: '{ds_var_name}'. Using this variable.")
                         # If cfgrib renames variables, you might need to map them here
                         # For now, we'll assume the direct name is used by cfgrib if available
                         found_var = True
                         # You might want to update the variable name in the list being checked
                         # required_vars[required_vars.index(var)] = ds_var_name # This would modify the list while iterating, be cautious
                         break
                    # Fallback check based on filename if originalName is not reliable
                    if 'GRIB_originalFileName' in data_array.attrs and var.lower() in data_array.attrs['GRIB_originalFileName'].lower():
                         print(f"Warning: Variable '{var}' not found directly, but found a variable likely corresponding to it: '{ds_var_name}' based on original filename.")
                         found_var = True
                         break

                if not found_var:
                   print(f"Error: Required variable '{var}' not found in the combined dataset.")
                   print("Available variables:", list(ds.data_vars))
                   return None


        has_gusts = gust_var in ds.data_vars
        if not has_gusts:
            print(f"Warning: Wind gusts variable ('{gust_var}') not found in the combined dataset. Gust data will not be included.")


        # --- Select Data for Location ---
        # Select the nearest grid point to the specified latitude and longitude across all time steps
        try:
            location_data = ds.sel(latitude=latitude, longitude=longitude, method='nearest')
        except KeyError:
             print(f"Error: Could not find nearest grid point for latitude {latitude}, longitude {longitude}.")
             print("Check if the location is within the GRIB file's coverage area.")
             return None

        # --- Process Data for Each Time Step ---
        if 'time' not in location_data.coords:
             print("Error: 'time' coordinate not found in the selected data. Cannot process time series.")
             return None

        times = location_data['time'].values
        u_avg_components = location_data[u_avg_component_var].values
        v_avg_components = location_data[v_avg_component_var].values
        # Get gust data if available, otherwise use a list of NaNs matching the time dimension size
        gusts = location_data[gust_var].values if has_gusts else np.full(len(times), np.nan)

        predictions = []

        # Parse initialization time string into a datetime object (for potential future use if needed)
        try:
            init_datetime = datetime.strptime(initialization_time_str, '%Y%m%d%H')
        except ValueError:
            print(f"Error: Could not parse initialization time string: {initialization_time_str}")
            # Continue processing with validity time from GRIB, but log error
            init_datetime = None


        for i in range(len(times)):
            # The 'time' coordinate in the dataset should represent the validity time.
            # We can directly use this time.
            validity_time_obj = times[i].astype('M64[s]').astype('O')

            u_avg = u_avg_components[i]
            v_avg = v_avg_components[i]
            gust = gusts[i]

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

        # Sort predictions by time (should already be sorted by open_mfdataset with combine='by_coords', but good practice)
        predictions.sort(key=lambda x: x['date'])

        # Convert the list of dictionaries to a JSON formatted string
        json_output = json.dumps(predictions, indent=4)

        return json_output

    except FileNotFoundError as e:
        print(f"Error: A required GRIB file was not found during open_mfdataset: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while processing the GRIB files: {e}")
        # Print the specific error for debugging
        import traceback
        traceback.print_exc()
        return None

# --- Command Line Argument Parsing ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get DWD ICON-EU wind forecast from local GRIB.bz2 files.')
    parser.add_argument('base_icon_eu_dir', type=str,
                        help='The base directory containing the variable subdirectories (e.g., /path/to/your/dwd/icon-eu)')
    parser.add_argument('latitude', type=float,
                        help='The latitude of the location')
    parser.add_argument('longitude', type=float,
                        help='The longitude of the location')

    args = parser.parse_args()

    print(f"Attempting to read full wind forecast from base directory {args.base_icon_eu_dir} at Latitude: {args.latitude}, Longitude: {args.longitude}...")

    full_wind_forecast_json_output = get_full_wind_forecast_json(
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

