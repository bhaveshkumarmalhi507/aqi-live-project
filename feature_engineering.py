import pandas as pd

# Load dataset
df = pd.read_csv("data.csv")

# Convert time column
df['time'] = pd.to_datetime(df['time'])

# Time Features
df['hour'] = df['time'].dt.hour
df['day'] = df['time'].dt.day
df['month'] = df['time'].dt.month

# Change Features
df['temp_change'] = df['temp'].diff()
df['humidity_change'] = df['humidity'].diff()

# Remove empty rows
df = df.dropna()

# Save new dataset
df.to_csv("features.csv", index=False)

print(df.head())
