import json
import datetime
import sys
import os

def create_gust_calendar(json_file, gust_threshold_kt):
    """
    Reads wind forecast data from a JSON file, identifies times with wind gusts
    exceeding a threshold, and creates an iCalendar (.ics) file.

    Events are created for each forecast entry exceeding the threshold, with the
    duration determined by the time difference between that entry's datetime and
    the next entry's datetime. If it's the last entry, a default 3-hour duration is used.

    Always creates a calendar file, which will be empty if no gusts exceed
    the threshold. The output file is always overwritten if it exists.

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
        # Extract location data for filename
        latitude = data['location']['latitude']
        longitude = data['location']['longitude']
    except KeyError:
        print("Error: JSON structure missing 'location', 'latitude', or 'longitude'.")
        return None

    # Create the output filename
    # Format lat/lon to a few decimal places for cleaner filenames
    filename = f"lat{latitude:.3f}lon{longitude:.3f}kn{int(gust_threshold_kt)}.ics"

    ical_content = []
    ical_content.append("BEGIN:VCALENDAR")
    ical_content.append("VERSION:2.0")
    ical_content.append("PRODID:-//project7III//WindCal//EN")
    ical_content.append("CALSCALE:GREGORIAN") # Or FLOATING for times without Z/TZID

    events_added = 0 # Counter to track how many events were added
    now_utc = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ') # Timestamp for DTSTAMP

    if 'forecasts' in data and isinstance(data['forecasts'], list):
        forecasts = data['forecasts']
        num_forecasts = len(forecasts)

        for i in range(num_forecasts):
            forecast = forecasts[i]
            try:
                # Check if wind gust exceeds the threshold
                gust = forecast.get('wind_gust_kt', 0) # Use .get() with default for safety
                if gust > gust_threshold_kt:
                    events_added += 1
                    
                    # Parse the current forecast datetime string
                    # Assuming the datetime is in 'YYYY-MM-DD HH:MM:SS' format and is in UTC
                    dt_obj_start = datetime.datetime.strptime(forecast['datetime'], '%Y-%m-%d %H:%M:%S')

                    # Determine the end time based on the next entry's start time
                    if i + 1 < num_forecasts:
                        # Get the start time of the next entry
                        next_forecast = forecasts[i+1]
                        try:
                             dt_obj_end = datetime.datetime.strptime(next_forecast['datetime'], '%Y-%m-%d %H:%M:%S')
                             # The duration is the difference between the next entry's start and the current entry's start
                             # Note: This makes the event end exactly when the next one starts.
                             # If preferred, you could set end to dt_obj_start + calculated duration,
                             # but using the next start time directly is usually more precise for time blocks.
                             # Duration is implicitly defined by DTSTART and DTEND.

                        except ValueError:
                            print(f"Warning: Could not parse datetime string for the next forecast entry (index {i+1}). Falling back to 1-hour duration for current entry (index {i}).")
                            # Fallback to 1 hour if the next datetime is unparseable
                            dt_obj_end = dt_obj_start + datetime.timedelta(hours=1)
                        except KeyError:
                             print(f"Warning: Missing 'datetime' key in the next forecast entry (index {i+1}). Falling back to 1-hour duration for current entry (index {i}).")
                             # Fallback to 1 hour if the next datetime key is missing
                             dt_obj_end = dt_obj_start + datetime.timedelta(hours=1)

                    else:
                        # This is the last entry, use a default last duration (3 hours)
                        dt_obj_end = dt_obj_start + datetime.timedelta(hours=3)

                    dt_start_str = dt_obj_start.strftime('%Y%m%dT%H%M%SZ')  # <-- 'Z' means UTC
                    dt_end_str = dt_obj_end.strftime('%Y%m%dT%H%M%SZ')      # <-- 'Z' means UTC
                    
                    # Generate a simple unique identifier for the event
                    # Combine datetime, gust value, and location for uniqueness
                    # Use the start time of the event for the UID base
                    uid = f"{dt_obj_start.strftime('%Y%m%dT%H%M%S')}-{int(gust)}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"

                    ical_content.append("BEGIN:VEVENT")
                    ical_content.append(f"UID:{uid}")
                    ical_content.append(f"DTSTAMP:{now_utc}")
                    ical_content.append(f"DTSTART:{dt_start_str}")
                    ical_content.append(f"DTEND:{dt_end_str}")
                    ical_content.append(f"SUMMARY:Gust over {gust_threshold_kt} kt ({gust:.1f} kt)")
                    ical_content.append("END:VEVENT")

            except ValueError:
                 print(f"Warning: Could not parse datetime string in forecast entry {i}: {forecast.get('datetime')}. Skipping this entry.")
            except KeyError as e:
                 print(f"Warning: Missing expected key {e} in forecast entry {i}. Skipping this entry. Entry: {forecast}")
            except Exception as e:
                 print(f"An unexpected error occurred processing forecast entry {i}: {e}. Skipping this entry. Entry: {forecast}")

    else:
        print("Warning: JSON structure missing 'forecasts' list or it's not a list.")

    if events_added == 0:
        print(f"No wind gusts over {gust_threshold_kt} knots found in the data. An empty calendar will be created.")
    else:
        print(f"Found {events_added} times with gusts over {gust_threshold_kt} knots.")


    ical_content.append("END:VCALENDAR")

    # Write the calendar to the file
    # Using 'w' mode ensures the file is created if it doesn't exist
    # and overwritten if it does exist.
    try:
        with open(filename, 'w') as f:
            f.write('\n'.join(ical_content))
        print(f"Successfully created/overwritten iCalendar file: {filename}")
        return filename
    except IOError as e:
        print(f"Error: Could not write calendar file {filename}: {e}")
        return None

if __name__ == "__main__":
    # Check for correct number of command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python your_script_name.py <json_file_path> <gust_threshold_knots>")
        sys.exit(1)

    json_file_path = sys.argv[1]
    try:
        gust_threshold = float(sys.argv[2])
    except ValueError:
        print("Error: Gust threshold must be a number.")
        sys.exit(1)

    create_gust_calendar(json_file_path, gust_threshold)