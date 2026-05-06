import requests
import pandas as pd
from datetime import datetime
import os

# ========================
# CONFIG
# ========================
API_KEY = os.getenv("API_KEY")
CITY = "Karachi"
LAT = 24.8607
LON = 67.0011

file_path = "aqi_dataset.csv"

# ========================
# 1. WEATHER API (SAFE)
# ========================
weather_url = "http://api.openweathermap.org/data/2.5/weather?q=Karachi&appid=1055a336d17d290b5102f6f66e5dcd58&units=metric"

weather_response = requests.get(weather_url)
weather_data = weather_response.json()

if "main" not in weather_data:
    raise Exception("Weather API failed or invalid key")

# ========================
# 2. AQI API (SAFE)
# ========================
aqi_url = (
    f"https://air-quality-api.open-meteo.com/v1/air-quality"
    f"?latitude={LAT}&longitude={LON}"
    f"&hourly=pm10,pm2_5,nitrogen_dioxide,sulphur_dioxide,ozone"
    f"&timezone=auto"
)

aqi_response = requests.get(aqi_url)
aqi_data = aqi_response.json()

hourly = aqi_data.get("hourly", {})

pm2_5 = hourly.get("pm2_5", [0])[-1]
pm10 = hourly.get("pm10", [0])[-1]
no2 = hourly.get("nitrogen_dioxide", [0])[-1]
so2 = hourly.get("sulphur_dioxide", [0])[-1]
o3 = hourly.get("ozone", [0])[-1]

# ========================
# 3. AQI CALCULATION (FIXED)
# ========================
values = [pm2_5, pm10, no2, so2, o3]

# convert safe numeric values
clean_values = [v for v in values if v is not None]

# ❌ agar bilkul bhi data nahi hai
if len(clean_values) == 0:
    print("Skipping row: no valid AQI data")

else:
    # ✔ safe AQI calculation
    values = [v if v is not None else 0 for v in values]
    aqi = max(values)


    # ========================
    # ONLY CREATE RECORD HERE
    # ========================
    record = {
        "time": datetime.now(),
        "city": CITY,
        "latitude": LAT,
        "longitude": LON,
        "temp": weather_data["main"]["temp"],
        "humidity": weather_data["main"]["humidity"],
        "pm2_5": pm2_5,
        "pm10": pm10,
        "no2": no2,
        "so2": so2,
        "o3": o3,
        "aqi": aqi
    }

    df_new = pd.DataFrame([record])

    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df["time"] = pd.to_datetime(df["time"])
        df = pd.concat([df, df_new], ignore_index=True)
    else:
        df = df_new

    df.to_csv(file_path, index=False)

    print("✅ Saved valid row only")