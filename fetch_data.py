import requests
import pandas as pd
from datetime import datetime
import os

# ========================
# CONFIG
# ========================
API_KEY = "YOUR_API_KEY"
CITY = "Karachi"
LAT = 24.8607
LON = 67.0011

file_path = "aqi_dataset.csv"

# ========================
# 1. WEATHER API
# ========================
weather_url = f"http://api.openweathermap.org/data/2.5/weather?q={"Karachi"}&appid={"1055a336d17d290b5102f6f66e5dcd58"}&units=metric"

weather_response = requests.get(weather_url)
weather_data = weather_response.json()

# ========================
# 2. AQI API (OPEN-METEO)
# ========================
aqi_url = (
    f"https://air-quality-api.open-meteo.com/v1/air-quality"
    f"?latitude={LAT}&longitude={LON}"
    f"&hourly=pm10,pm2_5,nitrogen_dioxide,sulphur_dioxide,ozone"
    f"&timezone=auto"
)

aqi_response = requests.get(aqi_url)
aqi_data = aqi_response.json()

hourly = aqi_data["hourly"]

# ========================
# 3. LATEST POLLUTANTS
# ========================
pm2_5 = hourly["pm2_5"][-1]
pm10 = hourly["pm10"][-1]
no2 = hourly["nitrogen_dioxide"][-1]
so2 = hourly["sulphur_dioxide"][-1]
o3 = hourly["ozone"][-1]

# ========================
# 4. AQI CALCULATION
# ========================
aqi = max(pm2_5, pm10, no2, so2, o3)

# ========================
# 5. CREATE RECORD
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

# ========================
# 6. LOAD EXISTING DATA
# ========================
df_new = pd.DataFrame([record])

if os.path.exists(file_path):
    df = pd.read_csv(file_path)
    df["time"] = pd.to_datetime(df["time"])
    df = pd.concat([df, df_new], ignore_index=True)
else:
    df = df_new

# ========================
# 7. KEEP ONLY LAST 3 DAYS DATA
# ========================
cutoff = datetime.now() - pd.Timedelta(days=3)
df = df[df["time"] >= cutoff]

# ========================
# 8. SAVE BACK (NO DELETE ISSUE)
# ========================
df.to_csv(file_path, index=False)

print("✅ 3-Day Live Dataset Updated")