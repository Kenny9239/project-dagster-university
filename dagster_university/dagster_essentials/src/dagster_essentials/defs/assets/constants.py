TAXI_ZONES_FILE_PATH = "data/raw/taxi_zones.csv"
TAXI_TRIPS_TEMPLATE_FILE_PATH = "data/raw/taxi_trips_{}.parquet"

TRIPS_BY_AIRPORT_FILE_PATH = "data/outputs/trips_by_airport.csv"
TRIPS_BY_WEEK_FILE_PATH = "data/outputs/trips_by_week.csv"
MANHATTAN_STATS_FILE_PATH = "data/staging/manhattan_stats.geojson"
MANHATTAN_MAP_FILE_PATH = "data/outputs/manhattan_map.png"

REQUEST_DESTINATION_TEMPLATE_FILE_PATH = "data/outputs/{}.png"

DATE_FORMAT = "%Y-%m-%d"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"


RAIN_DATA_URL = "https://hilltop.gw.govt.nz/Data.hts?Service=Hilltop&Request=GetData&Collection=Rainfall&TimeInterval=P24H&Method=Total&Interval=1hours&Alignment=00:00"    
RAIN_HOUR_OUTPUT_FILE_PATH = "data/outputs/RainHour_output_from_url.htm"