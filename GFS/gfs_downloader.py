import requests
from datetime import datetime

# Config
today = datetime.utcnow().strftime("%Y%m%d")
hour = "00"  # adjust if needed
steps = range(0, 121)  # f000 to f120 (5 days hourly)

base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

for step in steps:
    fxx = f"{step:03d}"
    params = {
        "file": f"gfs.t{hour}z.pgrb2.0p25.f{fxx}",
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
        "dir": f"/gfs.{today}/{hour}/atmos"
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        filename = f"gfs_{today}_{hour}_f{fxx}.grib2"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded {filename}")
    else:
        print(f"⚠️ Failed to download f{fxx}")