import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import concurrent.futures

def get_timestamp_directories(url):
    """Fetch all directories with timestamps from the main page."""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    directories = []
    
    # Find all anchor tags that represent directories
    for a_tag in soup.find_all('a'):
        href = a_tag.get('href')
        if href.endswith('/') and href != '../':
            # Get the timestamp from the same row
            parent = a_tag.parent
            if parent.name == 'pre':
                text = parent.get_text()
                # Extract the date and time for this directory
                match = re.search(rf'{href}\s+(\d+-\w+-\d+ \d+:\d+:\d+)', text)
                if match:
                    timestamp_str = match.group(1)
                    timestamp = datetime.strptime(timestamp_str, '%d-%b-%Y %H:%M:%S')
                    directories.append((href, timestamp))
    
    return directories

def get_newest_directory(directories):
    """Return the directory with the newest timestamp."""
    if not directories:
        return None
    return max(directories, key=lambda x: x[1])[0]

def download_file(url, save_path):
    """Download a file from URL and save it to the specified path."""
    response = requests.get(url, stream=True)
    
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # Save the file
    with open(save_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    
    print(f"Downloaded: {save_path}")

def download_files_from_subdirectories(base_url, newest_dir, subdirs_to_download):
    """Download all files from the specified subdirectories."""
    url = f"{base_url}{newest_dir}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Create a base downloads directory
    downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads", "icon-eu")
    
    tasks = []
    
    # Check each subdirectory we're interested in
    for subdir in subdirs_to_download:
        # First, make sure this subdirectory exists
        subdir_url = f"{url}{subdir}/"
        subdir_response = requests.get(subdir_url)
        
        if subdir_response.status_code == 200:
            subdir_soup = BeautifulSoup(subdir_response.text, 'html.parser')
            
            # Find all files in this subdirectory
            for a_tag in subdir_soup.find_all('a'):
                href = a_tag.get('href')
                if href != '../' and not href.endswith('/'):  # It's a file, not a directory
                    file_url = f"{subdir_url}{href}"
                    save_path = os.path.join(downloads_dir, subdir, href)
                    
                    tasks.append((file_url, save_path))
    
    # Download files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(download_file, url, path) for url, path in tasks]
        concurrent.futures.wait(futures)

def main():
    base_url = "https://opendata.dwd.de/weather/nwp/icon-eu/grib/"
    subdirs_to_download = ["u_10m/", "v_10m/", "vmax_10m/"]
    
    print("Fetching directories...")
    directories = get_timestamp_directories(base_url)
    
    newest_dir = get_newest_directory(directories)
    if newest_dir:
        print(f"Newest directory: {newest_dir}")
        download_files_from_subdirectories(base_url, newest_dir, subdirs_to_download)
        print("Download complete!")
    else:
        print("No directories found.")

if __name__ == "__main__":
    main()