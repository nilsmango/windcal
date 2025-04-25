import requests
import pandas as pd
from geopy.distance import geodesic
import json
import numpy as np
from collections import Counter
import io

def download_csv(url):
    """Downloads the CSV file from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
        return None

def parse_station_info(csv_content):
    """Parses station information (codes, longitude, latitude) from the CSV header."""
    station_info = {}
    lines = csv_content.splitlines()
    indicator_line = None
    longitude_line = None
    latitude_line = None

    # Find the lines containing station indicators, longitude, and latitude
    for line in lines:
        if line.startswith('Indicator:;'):
            indicator_line = line
        elif line.startswith('Grid_longitude:;'):
            longitude_line = line
        elif line.startswith('Grid_latitude:;'):
            latitude_line = line

    if not all([indicator_line, longitude_line, latitude_line]):
        print("Could not find all required station header lines in the CSV.")
        return None

    # Extract station codes, longitudes, and latitudes, filtering out empty strings
    stations = [s.strip() for s in indicator_line.split(';')[1:] if s.strip()]
    longitudes = [float(lon) for lon in longitude_line.split(';')[1:] if lon.strip()]
    latitudes = [float(lat) for lat in latitude_line.split(';')[1:] if lat.strip()]

    # Store station info in a dictionary
    if len(stations) == len(longitudes) == len(latitudes):
        for i, station_code in enumerate(stations):
            station_info[station_code] = {
                'longitude': longitudes[i],
                'latitude': latitudes[i]
            }
    else:
        print("Mismatch in the number of stations, longitudes, and latitudes.")
        print(f"Stations found: {len(stations)}")
        print(f"Longitudes found: {len(longitudes)}")
        print(f"Latitudes found: {len(latitudes)}")
        return None

    return station_info

def find_closest_station(user_lat, user_lon, station_info):
    """Finds the station closest to the user's coordinates."""
    closest_station = None
    min_distance = float('inf')
    user_coords = (user_lat, user_lon)

    for station_code, coords in station_info.items():
        station_coords = (coords['latitude'], coords['longitude'])
        distance = geodesic(user_coords, station_coords).km
        if distance < min_distance:
            min_distance = distance
            closest_station = station_code

    return closest_station, min_distance

def get_wind_direction_16_points(degrees):
    """Converts wind direction in degrees to one of 16 cardinal/intercardinal points."""
    if degrees is None or degrees < 0:
        return "Unknown" # Handle missing or invalid data

    # Normalize degrees to be within 0-360
    degrees = degrees % 360

    # Define the 16 points and their boundaries (midpoint of each sector)
    # N starts at 348.75 and ends at 11.25 (wrapping around 0)
    points = [
        (0, "N"), (22.5, "NNE"), (45, "NE"), (67.5, "ENE"),
        (90, "E"), (112.5, "ESE"), (135, "SE"), (157.5, "SSE"),
        (180, "S"), (202.5, "SSW"), (225, "SW"), (247.5, "WSW"),
        (270, "W"), (292.5, "WNW"), (315, "NW"), (337.5, "NNW"),
        (360, "N") # Add N again for wrapping
    ]

    # Find the sector the degree falls into
    for i in range(len(points) - 1):
        start_angle, start_point = points[i]
        end_angle, end_point = points[i+1]

        # Handle the wrap-around case for North (0-11.25 and 348.75-360)
        if (start_point == "NNW" and end_point == "N" and (degrees >= start_angle or degrees < end_angle - 360)) or \
           (start_angle <= degrees < end_angle):
             return start_point


    # If degrees is exactly 360, it's North
    if degrees == 360:
        return "N"

    # Fallback for edge cases, though the logic should cover 0-360
    return "Unknown"


def analyze_wind_data(df, station_code, missing_value_code=-999.0):
    """Analyzes wind data for a specific station."""
    station_df = df[df['stn'] == station_code].copy()

    if station_df.empty:
        print(f"No data found for station {station_code}.")
        return None

    # Find FF_10M and DD_10M columns for all members
    ff_cols = [col for col in station_df.columns if col.startswith('FF_10M')]
    dd_cols = [col for col in station_df.columns if col.startswith('DD_10M')]

    if not ff_cols or not dd_cols:
        print(f"Could not find wind speed (FF_10M) or wind direction (DD_10M) columns for station {station_code}.")
        return None

    wind_forecast = []

    # Process data for each time step
    for index, row in station_df.iterrows():
        time = row['time']
        leadtime = row['leadtime']

        # Extract wind speeds and directions for all members, handling missing values
        speeds = [row[col] for col in ff_cols if pd.notna(row[col]) and row[col] != missing_value_code]
        directions = [row[col] for col in dd_cols if pd.notna(row[col]) and row[col] != missing_value_code]

        avg_speed = np.mean(speeds) if speeds else 0.0
        max_speed = np.max(speeds) if speeds else 0.0

        # Determine dominant wind direction
        dominant_direction = "Unknown"
        if directions:
            # Convert degrees to 16-point directions
            direction_points = [get_wind_direction_16_points(d) for d in directions]
            # Count occurrences of each direction point
            direction_counts = Counter(direction_points)
            # Find the most common direction point
            most_common = direction_counts.most_common(1)
            if most_common:
                dominant_direction = most_common[0][0]
            else:
                 dominant_direction = "Unknown" # Should not happen if directions is not empty

        wind_forecast.append({
            'time': time,
            'leadtime': leadtime,
            'average_wind_speed (m/s)': round(avg_speed, 2),
            'max_wind_speed (m/s)': round(max_speed, 2),
            'dominant_wind_direction': dominant_direction
        })

    return wind_forecast

def main():
    csv_url = "https://data.geo.admin.ch/ch.meteoschweiz.prognosen/punktprognosen/COSMO-E-all-stations.csv"
    print(f"Downloading weather data from: {csv_url}")

    csv_content = download_csv(csv_url)
    if not csv_content:
        return

    # --- Parse Station Info ---
    station_info = parse_station_info(csv_content)
    if not station_info:
        return
    print(f"Found {len(station_info)} weather stations.")

    # --- Get User Coordinates ---
    while True:
        try:
            user_lat = float(input("Enter your latitude: "))
            user_lon = float(input("Enter your longitude: "))
            break
        except ValueError:
            print("Invalid input. Please enter numerical values for latitude and longitude.")

    # --- Find Closest Station ---
    closest_station_code, distance = find_closest_station(user_lat, user_lon, station_info)

    if not closest_station_code:
        print("Could not find a closest station.")
        return

    print(f"\nClosest station found: {closest_station_code} (Distance: {distance:.2f} km)")
    print(f"Coordinates: Latitude={station_info[closest_station_code]['latitude']}, Longitude={station_info[closest_station_code]['longitude']}")

    # --- Read Data Table using Pandas ---
    # Find the line number where the actual data table headers start
    data_start_line = 0
    lines = csv_content.splitlines()
    for i, line in enumerate(lines):
        if line.startswith('stn;time;leadtime;'):
            data_start_line = i
            break

    if data_start_line == 0:
        print("Could not find the start of the data table in the CSV.")
        return

    # Read the CSV data table using pandas, skipping initial rows
    # Use io.StringIO to treat the string content as a file
    try:
        # Read the first data header row to get column names mapping
        data_header_line = lines[data_start_line]
        raw_columns = data_header_line.split(';')

        # Read the data, skipping the initial metadata and the 3 data header rows
        df = pd.read_csv(io.StringIO(csv_content), sep=';', skiprows=data_start_line + 3, header=None, na_values=[-999.0])

        # Assign column names based on the raw_columns and member numbers
        # The structure is param1_mem00, param1_mem01, ..., param2_mem00, ...
        # We need to map the raw column index to the parameter name and member
        column_names = ['stn', 'time', 'leadtime']
        param_counts = {} # To track how many times each parameter has appeared

        for i, raw_col_name in enumerate(raw_columns[3:]): # Skip stn, time, leadtime
            param_name = raw_col_name # The parameter name is the raw column name
            if param_name not in param_counts:
                param_counts[param_name] = 0
            else:
                param_counts[param_name] += 1

            member_number = f"{param_counts[param_name]:02d}"
            column_names.append(f"{param_name}_member{member_number}")

        # Ensure the number of generated column names matches the DataFrame columns
        if len(column_names) == df.shape[1]:
             df.columns = column_names
        else:
             print(f"Column name mismatch. Expected {df.shape[1]} columns, generated {len(column_names)}.")
             print("Generated column names:", column_names)
             print("First row of data:", df.iloc[0].tolist())
             return


    except Exception as e:
        print(f"Error reading CSV data with pandas: {e}")
        return

    # --- Analyze Wind Data ---
    wind_forecast_data = analyze_wind_data(df, closest_station_code, missing_value_code=-999.0)

    # --- Output JSON ---
    if wind_forecast_data is not None:
        output_json = {
            'station': closest_station_code,
            'station_coordinates': {
                'latitude': station_info[closest_station_code]['latitude'],
                'longitude': station_info[closest_station_code]['longitude']
            },
            'wind_forecast': wind_forecast_data
        }
        print("\nWind Forecast JSON:")
        print(json.dumps(output_json, indent=4))

if __name__ == "__main__":
    main()
