import requests
import snowflake.connector
from datetime import datetime
import pytz
import os

# Location example: Navi Mumbai
LAT = 19.0330
LON = 73.0297
TZ = "Asia/Kolkata"
LOCATION_ID = "NAVI_MUMBAI"

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": LAT,
    "longitude": LON,
    "hourly": "temperature_2m,relative_humidity_2m,rain,precipitation,windspeed_10m,winddirection_10m",
    "timezone": "UTC"
}

def fetch_data():
    res = requests.get(FORECAST_URL, params=PARAMS, timeout=20)
    res.raise_for_status()
    return res.json()

def connect_snowflake():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="WH_WEATHER",
        database="WEATHER_RT",
        schema="PUBLIC"
    )

def main():
    data = fetch_data()
    conn = connect_snowflake()
    cur = conn.cursor()

    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    hums = data["hourly"]["relative_humidity_2m"]
    rain = data["hourly"]["rain"]
    precip = data["hourly"]["precipitation"]
    windspd = data["hourly"]["windspeed_10m"]
    winddir = data["hourly"]["winddirection_10m"]

    rows = []

    for i, t in enumerate(times[-2:]):   # last 2 timestamps = "near real-time"
        dt_utc = datetime.fromisoformat(t.replace("Z","+00:00"))
        dt_local = dt_utc.astimezone(pytz.timezone(TZ))

        rows.append((
            LOCATION_ID, LAT, LON, dt_utc, dt_local,
            temps[i], hums[i], windspd[i], winddir[i], rain[i], precip[i],
            "open-meteo"
        ))

    sql = """
        INSERT INTO WEATHER_EVENTS_RAW (
            location_id, latitude, longitude, event_ts_utc, event_ts_local,
            temperature_c, humidity_pct, wind_speed_kmh, wind_direction_deg,
            rain_mm, precipitation_mm, source
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cur.executemany(sql, rows)
    conn.commit()
    print("Inserted", len(rows), "rows into Snowflake")

if __name__ == "__main__":
    main()
