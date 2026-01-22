import psycopg2
import requests
import os

DB_URL = os.environ["DATABASE_URL"]

LIMIT = 10000
SAMPLE_URL = f"https://data.cityofnewyork.us/resource/5zhs-2jue.geojson?$limit={LIMIT}&$offset=0"

def infer_pg_type(values):
    for v in values:
        if v is None:
            continue
        try:
            int(v)
            return "BIGINT"
        except:
            try:
                float(v)
                return "NUMERIC"
            except:
                return "TEXT"
    return "TEXT"

print(f"Fetching {LIMIT} GeoJSON features for schema inference...")
data = requests.get(SAMPLE_URL).json()
features = data["features"]

print("Collecting property samples...")
samples = {}

for feat in features:
    for k, v in feat["properties"].items():
        samples.setdefault(k, []).append(v)

print("Inferring schema from real dataset...")
columns = []
for k, values in samples.items():
    pg_type = infer_pg_type(values)
    columns.append(f'"{k}" {pg_type}')

schema_sql = ",\n".join(columns)

print("Connecting to Postgres...")
conn = psycopg2.connect(DB_URL, sslmode="require")
cur = conn.cursor()

print("Enabling PostGIS...")
cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

print("Creating buildings table dynamically...")
create_sql = f"""
CREATE TABLE IF NOT EXISTS buildings (
    {schema_sql},
    geom GEOMETRY(MultiPolygon, 4326)
);
"""
cur.execute(create_sql)

print("Creating spatial index...")
cur.execute("""
CREATE INDEX IF NOT EXISTS buildings_geom_idx
ON buildings
USING GIST (geom);
""")

conn.commit()
cur.close()
conn.close()

print("Schema created from 10k real features. Ready for ingestion.")
