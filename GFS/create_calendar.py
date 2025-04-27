import json
import datetime
import sys
import os

def create_gust_calendar(json_file, gust_threshold_kt):
    """
    Reads wind forecast data from a JSON file, identifies times with wind gusts
    exceeding a threshold, and creates an iCalendar (.ics) file.

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

    events_found = False
    now_utc = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ') # Timestamp for DTSTAMP

    if 'forecasts' in data and isinstance(data['forecasts'], list):
        for forecast in data['forecasts']:
            try:
                # Check if wind gust exceeds the threshold
                gust = forecast.get('wind_gust_kt', 0) # Use .get() with default for safety
                if gust > gust_threshold_kt:
                    events_found = True
                    # Parse the datetime string
                    # Assuming the datetime is in 'YYYY-MM-DD HH:MM:SS' format
                    dt_obj = datetime.datetime.strptime(forecast['datetime'], '%Y-%m-%d %H:%M:%S')

                    # iCalendar uses YYYYMMDDTHHMMSS format
                    # For simplicity here, we'll treat the JSON times as local
                    # and not add 'Z' for UTC or specify a TZID.
                    # A calendar client will typically interpret this as floating time
                    # or according to local system settings.
                    dt_start_str = dt_obj.strftime('%Y%m%dT%H%M%S')

                    # Create a simple 1-hour event
                    dt_end_obj = dt_obj + datetime.timedelta(hours=1)
                    dt_end_str = dt_end_obj.strftime('%Y%m%dT%H%M%S')

                    # Generate a simple unique identifier for the event
                    # Combine datetime, gust value, and location for uniqueness
                    uid = f"{dt_obj.strftime('%Y%m%dT%H%M%S')}-{int(gust)}-{latitude:.3f}-{longitude:.3f}@gustcalendar.local"

                    ical_content.append("BEGIN:VEVENT")
                    ical_content.append(f"UID:{uid}")
                    ical_content.append(f"DTSTAMP:{now_utc}") # When this event definition was created
                    ical_content.append(f"DTSTART:{dt_start_str}")
                    ical_content.append(f"DTEND:{dt_end_str}")
                    ical_content.append(f"SUMMARY:Gust over {gust_threshold_kt} kt ({gust:.1f} kt)")
                    ical_content.append("END:VEVENT")

            except ValueError:
                 print(f"Warning: Could not parse datetime string in forecast: {forecast.get('datetime')}")
            except KeyError as e:
                 print(f"Warning: Missing expected key in a forecast entry: {e}. Entry: {forecast}")
            except Exception as e:
                 print(f"An unexpected error occurred processing a forecast entry: {e}. Entry: {forecast}")

    else:
        print("Warning: JSON structure missing 'forecasts' list or it's not a list.")


    if not events_found:
        print(f"No wind gusts over {gust_threshold_kt} knots found in the data.")
        # You might choose to not create the file in this case,
        # or create an empty calendar file.
        # Let's choose to not create the file if no events are found.
        return None

    ical_content.append("END:VCALENDAR")

    # Write the calendar to the file
    try:
        with open(filename, 'w') as f:
            f.write('\n'.join(ical_content))
        print(f"Successfully created iCalendar file: {filename}")
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