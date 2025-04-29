import json
import datetime
import sys
import math # Needed for floor/round in direction conversion
import pytz # For timezone handling
from timezonefinder import TimezoneFinder # To get timezone from lat/lon

def degrees_to_cardinal(degrees):
    """
    Converts wind direction in degrees (0-360) to a cardinal or
    intercardinal direction string (e.g., N, NE, E, SSE, etc.).
    """
    if degrees is None:
        return "N/A"

    # Ensure degrees is within 0-360 range
    degrees = degrees % 360

    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    # Add 11.25 degrees to shift the range center, then divide by 22.5
    # Use modulo 16 to wrap around
    index = math.floor((degrees + 11.25) / 22.5) % 16
    return directions[index]

def format_ical_description_line(local_dt_obj, speed_kt, gust_kt, direction_deg):
    """
    Formats a single line for the calendar event description using a LOCAL datetime object,
    escaping characters as needed for iCalendar.
    """
    # Format the local time object
    time_only = local_dt_obj.strftime('%H:%M')

    direction_cardinal = degrees_to_cardinal(direction_deg)

    gust_info = f", Gusts: {gust_kt:.1f} kt" if gust_kt is not None else ""
    line = f"{time_only}: Wind: {speed_kt:.1f} kt{gust_info}, Dir: {direction_cardinal}"

    # Escape iCalendar special characters: \, ; , \n
    # Use \\n for a literal newline within the DESCRIPTION property value
    line = line.replace('\\', '\\\\').replace(';', '\\;').replace(',', '\\,')

    return line


def create_gust_calendar(json_file, gust_threshold_kt):
    """
    Reads wind forecast data from a JSON file, identifies continuous blocks
    of time with wind gusts exceeding a threshold, and creates a single
    iCalendar (.ics) event for each continuous block.

    Each event covers the duration of the continuous block. The description
    lists details (local time, speed, gust, cardinal direction) for each hour
    within the block. Calendar event times (DTSTART, DTEND) are in UTC.

    Always creates a calendar file, which will be empty if no gusts exceed
    the threshold or if no continuous blocks are found. The output file is
    always overwritten if it exists.

    Args:
        json_file (str): The path to the input JSON file.
        gust_threshold_kt (float or int): The wind gust threshold in knots.

    Returns:
        str or None: The name of the created iCalendar file if successful,
                     otherwise None.
    """
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_file}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {json_file}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading JSON: {e}")
        return None

    try:
        # Extract location data
        latitude = data['location']['latitude']
        longitude = data['location']['longitude']
    except KeyError:
        print("Error: JSON structure missing 'location', 'latitude', or 'longitude'.")
        return None

    # --- Determine Local Timezone ---
    tf = TimezoneFinder()
    tz_name = tf.timezone_at(lat=latitude, lng=longitude)

    if tz_name is None:
        print(f"Warning: Could not determine timezone for lat={latitude}, lon={longitude}. Description times will remain in UTC.")
        local_tz = pytz.utc # Fallback to UTC if timezone not found
    else:
        try:
            local_tz = pytz.timezone(tz_name)
            print(f"Determined local timezone: {tz_name}")
        except pytz.UnknownTimeZoneError:
            print(f"Warning: Unknown timezone '{tz_name}' found. Description times will remain in UTC.")
            local_tz = pytz.utc # Fallback if timezone name is invalid

    # Create the output filename
    # Format lat/lon to a few decimal places for cleaner filenames
    filename = f"lat{latitude:.3f}lon{longitude:.3f}kn{int(gust_threshold_kt)}.ics"

    ical_content = []
    ical_content.append("BEGIN:VCALENDAR")
    ical_content.append("VERSION:2.0")
    ical_content.append("PRODID:-//project7III//WindCal//EN")
    ical_content.append("CALSCALE:GREGORIAN")

    events_added_count = 0 # Counter to track how many events were added
    now_utc = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ') # Timestamp for DTSTAMP

    forecasts = data.get('forecasts', [])
    if not isinstance(forecasts, list):
         print("Warning: JSON structure missing 'forecasts' list or it's not a list.")
         forecasts = [] # Ensure forecasts is an iterable empty list

    num_forecasts = len(forecasts)
    if num_forecasts == 0:
        print("No forecast data found.")
        # Still create an empty calendar file
    else:
        # Calculate the typical interval between forecasts (used for the end time of the last block if applicable)
        # Default to 1 hour if less than 2 forecasts
        interval = datetime.timedelta(hours=1)
        if num_forecasts >= 2:
             try:
                 dt1 = datetime.datetime.strptime(forecasts[0]['datetime'], '%Y-%m-%d %H:%M:%S')
                 dt2 = datetime.datetime.strptime(forecasts[1]['datetime'], '%Y-%m-%d %H:%M:%S')
                 interval = dt2 - dt1
                 # Ensure interval is positive
                 if interval.total_seconds() <= 0:
                      print(f"Warning: Non-positive interval calculated from first two forecasts. Defaulting to 1 hour.")
                      interval = datetime.timedelta(hours=1)
             except (ValueError, KeyError) as e:
                 print(f"Warning: Could not calculate interval from first two forecasts ({e}). Defaulting to 1 hour.")
                 interval = datetime.timedelta(hours=1)


        current_block = []

        # Iterate through forecasts to find continuous blocks
        for i in range(num_forecasts):
            forecast = forecasts[i]
            # Use .get() with default for safety against missing keys
            gust = forecast.get('wind_gust_kt', 0)
            speed = forecast.get('wind_speed_kt', 0)
            direction = forecast.get('wind_dir_deg')
            forecast_utc_str = forecast.get('datetime')


            # Validate essential data for the entry
            if forecast_utc_str is None:
                print(f"Warning: Skipping forecast entry {i} due to missing 'datetime'.")
                # If current_block is not empty, process it as it ended unexpectedly
                if current_block:
                     # Process the completed block (ends at the time of the last valid entry + interval)
                    events_added_count += 1
                    start_dt_obj_utc = datetime.datetime.strptime(current_block[0]['datetime'], '%Y-%m-%d %H:%M:%S')
                    end_dt_obj_utc = datetime.datetime.strptime(current_block[-1]['datetime'], '%Y-%m-%d %H:%M:%S') + interval
                    # Build description etc. (same as end-of-loop logic)
                    description_lines = []
                    for block_entry in current_block:
                         # Convert UTC time for description line
                         block_entry_utc_naive = datetime.datetime.strptime(block_entry['datetime'], '%Y-%m-%d %H:%M:%S')
                         block_entry_utc_aware = pytz.utc.localize(block_entry_utc_naive)
                         block_entry_local = block_entry_utc_aware.astimezone(local_tz)

                         desc_line = format_ical_description_line(
                             block_entry_local,
                             block_entry.get('wind_speed_kt', 0),
                             block_entry.get('wind_gust_kt'),
                             block_entry.get('wind_dir_deg')
                         )
                         description_lines.append(desc_line)

                    description = "\\n".join(description_lines)

                    uid = f"{start_dt_obj_utc.strftime('%Y%m%dT%H%M%S')}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"

                    ical_content.append("BEGIN:VEVENT")
                    ical_content.append(f"UID:{uid}")
                    ical_content.append(f"DTSTAMP:{now_utc}")
                    ical_content.append(f"DTSTART:{start_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    ical_content.append(f"DTEND:{end_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    ical_content.append(f"SUMMARY:Gusts > {int(gust_threshold_kt)} kt")
                    ical_content.append(f"LOCATION:{latitude:.3f},{longitude:.3f}")
                    ical_content.append(f"DESCRIPTION:{description}")
                    ical_content.append("END:VEVENT")

                    current_block = [] # Clear the block
                continue # Skip to the next forecast entry

            # Now we know forecast_utc_str is valid
            try:
                 forecast_dt_utc_naive = datetime.datetime.strptime(forecast_utc_str, '%Y-%m-%d %H:%M:%S')
                 # Make the datetime UTC-aware for conversion
                 forecast_dt_utc_aware = pytz.utc.localize(forecast_dt_utc_naive)
            except ValueError:
                 print(f"Warning: Could not parse datetime string '{forecast_utc_str}' in forecast entry {i}. Skipping this entry.")
                 # If current_block is not empty, process it as it ended unexpectedly
                 if current_block:
                     # Process the completed block (ends at the time of the last valid entry + interval)
                    events_added_count += 1
                    start_dt_obj_utc = datetime.datetime.strptime(current_block[0]['datetime'], '%Y-%m-%d %H:%M:%S')
                    end_dt_obj_utc = datetime.datetime.strptime(current_block[-1]['datetime'], '%Y-%m-%d %H:%M:%S') + interval

                    # Build description etc.
                    description_lines = []
                    for block_entry in current_block:
                         # Convert UTC time for description line
                         block_entry_utc_naive = datetime.datetime.strptime(block_entry['datetime'], '%Y-%m-%d %H:%M:%S')
                         block_entry_utc_aware = pytz.utc.localize(block_entry_utc_naive)
                         block_entry_local = block_entry_utc_aware.astimezone(local_tz)

                         desc_line = format_ical_description_line(
                             block_entry_local,
                             block_entry.get('wind_speed_kt', 0),
                             block_entry.get('wind_gust_kt'),
                             block_entry.get('wind_dir_deg')
                         )
                         description_lines.append(desc_line)

                    description = "\\n".join(description_lines)

                    uid = f"{start_dt_obj_utc.strftime('%Y%m%dT%H%M%S')}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"

                    ical_content.append("BEGIN:VEVENT")
                    ical_content.append(f"UID:{uid}")
                    ical_content.append(f"DTSTAMP:{now_utc}")
                    ical_content.append(f"DTSTART:{start_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    ical_content.append(f"DTEND:{end_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    ical_content.append(f"SUMMARY:Gusts > {int(gust_threshold_kt)} kt (UTC: {start_dt_obj_utc.strftime('%H:%M')} - {end_dt_obj_utc.strftime('%H:%M')})") # Explicitly state UTC
                    ical_content.append(f"LOCATION:{latitude:.3f},{longitude:.3f}")
                    ical_content.append(f"DESCRIPTION:{description}")
                    ical_content.append("END:VEVENT")

                    current_block = [] # Clear the block
                 continue # Skip to the next forecast entry


            is_over_threshold = gust > gust_threshold_kt # Already defaulted gust to 0 if None

            if is_over_threshold:
                # Add current forecast to the potential block
                current_block.append(forecast)
            else:
                # If we were in a block, process it now that it's ended
                if current_block:
                    events_added_count += 1
                    # Process the completed block
                    start_dt_obj_utc = datetime.datetime.strptime(current_block[0]['datetime'], '%Y-%m-%d %H:%M:%S')
                    # The block ends when the forecast entry *after* the last block entry starts
                    # This 'next_forecast_time' is the time of the entry at index 'i' (the one below threshold)
                    end_dt_obj_utc = datetime.datetime.strptime(forecasts[i]['datetime'], '%Y-%m-%d %H:%M:%S')


                    # Build the description
                    description_lines = []
                    for block_entry in current_block:
                         # Convert UTC time for description line
                         block_entry_utc_naive = datetime.datetime.strptime(block_entry['datetime'], '%Y-%m-%d %H:%M:%S')
                         block_entry_utc_aware = pytz.utc.localize(block_entry_utc_naive)
                         block_entry_local = block_entry_utc_aware.astimezone(local_tz)

                         desc_line = format_ical_description_line(
                             block_entry_local, # Pass the local datetime object
                             block_entry.get('wind_speed_kt', 0),
                             block_entry.get('wind_gust_kt'),
                             block_entry.get('wind_dir_deg')
                         )
                         description_lines.append(desc_line)

                    description = "\\n".join(description_lines)

                    # Generate UID based on block start time and location
                    uid = f"{start_dt_obj_utc.strftime('%Y%m%dT%H%M%S')}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"


                    ical_content.append("BEGIN:VEVENT")
                    ical_content.append(f"UID:{uid}")
                    ical_content.append(f"DTSTAMP:{now_utc}")
                    ical_content.append(f"DTSTART:{start_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    ical_content.append(f"DTEND:{end_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
                    # Explicitly state UTC in the summary time range
                    ical_content.append(f"SUMMARY:Gusts > {int(gust_threshold_kt)} kt (UTC: {start_dt_obj_utc.strftime('%H:%M')} - {end_dt_obj_utc.strftime('%H:%M')})")
                    # Add LOCATION property with lat/lon
                    ical_content.append(f"LOCATION:{latitude:.3f},{longitude:.3f}")
                    ical_content.append(f"DESCRIPTION:{description}")
                    ical_content.append("END:VEVENT")

                    # Clear the block for the next potential one
                    current_block = []
                # Else: current_block was empty and current entry is below threshold, do nothing.

        # After the loop, check if there's a pending block at the very end of the forecasts
        if current_block:
            events_added_count += 1
            # Process the final block
            start_dt_obj_utc = datetime.datetime.strptime(current_block[0]['datetime'], '%Y-%m-%d %H:%M:%S')
            # The block ends after the last entry + the typical interval
            end_dt_obj_utc = datetime.datetime.strptime(current_block[-1]['datetime'], '%Y-%m-%d %H:%M:%S') + interval

            # Build the description (same logic as inside the loop)
            description_lines = []
            for block_entry in current_block:
                 # Convert UTC time for description line
                 block_entry_utc_naive = datetime.datetime.strptime(block_entry['datetime'], '%Y-%m-%d %H:%M:%S')
                 block_entry_utc_aware = pytz.utc.localize(block_entry_utc_naive)
                 block_entry_local = block_entry_utc_aware.astimezone(local_tz)

                 desc_line = format_ical_description_line(
                     block_entry_local, # Pass the local datetime object
                     block_entry.get('wind_speed_kt', 0),
                     block_entry.get('wind_gust_kt'),
                     block_entry.get('wind_dir_deg')
                 )
                 description_lines.append(desc_line)


            description = "\\n".join(description_lines)

            # Generate UID
            uid = f"{start_dt_obj_utc.strftime('%Y%m%dT%H%M%S')}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"

            ical_content.append("BEGIN:VEVENT")
            ical_content.append(f"UID:{uid}")
            ical_content.append(f"DTSTAMP:{now_utc}")
            ical_content.append(f"DTSTART:{start_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
            ical_content.append(f"DTEND:{end_dt_obj_utc.strftime('%Y%m%dT%H%M%SZ')}")
            # Explicitly state UTC
            ical_content.append(f"SUMMARY:Gusts > {int(gust_threshold_kt)} kt (UTC: {start_dt_obj_utc.strftime('%H:%M')} - {end_dt_obj_utc.strftime('%H:%M')})")
            ical_content.append(f"LOCATION:{latitude:.3f},{longitude:.3f}")
            ical_content.append(f"DESCRIPTION:{description}")
            ical_content.append("END:VEVENT")


    if events_added_count == 0:
        print(f"No continuous blocks with wind gusts over {gust_threshold_kt} knots found.")
        print("An empty calendar file will be created.")
    else:
        print(f"Found {events_added_count} continuous blocks with gusts over {gust_threshold_kt} knots.")


    ical_content.append("END:VCALENDAR")

    # Write the calendar to the file
    try:
        with open(filename, 'w') as f:
            f.write('\n'.join(ical_content))
        print(f"Successfully created/overwritten iCalendar file: {filename}")
    except IOError as e:
        print(f"Error: Could not write calendar file {filename}: {e}")
        return None

    return filename # Return filename on success

if __name__ == "__main__":
    # Check for correct number of command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python create_calendar.py <json_file_path> <gust_threshold_knots>")
        sys.exit(1)

    json_file_path = sys.argv[1]
    try:
        gust_threshold = float(sys.argv[2])
    except ValueError:
        print("Error: Gust threshold must be a number.")
        sys.exit(1)

    create_gust_calendar(json_file_path, gust_threshold)