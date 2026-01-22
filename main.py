import os
import requests
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry
from shapely.geometry import shape
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env file")

# 2. Database Setup
Base = declarative_base()

class NycBuilding(Base):
    __tablename__ = 'nyc_buildings'

    id = Column(Integer, primary_key=True)
    bin = Column(String, unique=True, index=True) # Building Identification Number
    base_bbl = Column(String)
    construction_year = Column(Integer, nullable=True)
    height_roof = Column(Float, nullable=True)
    doitt_id = Column(Integer)
    
    # Stores the raw properties just in case we miss something
    raw_properties = Column(JSON)
    
    # PostGIS Geometry Column (SRID 4326 = Lat/Lon)
    geom = Geometry('MULTIPOLYGON', srid=4326)

# Connect to DB
engine = create_engine(DATABASE_URL)

# Create the table if it doesn't exist (also adds the PostGIS extension if needed)
# Note: Ensure 'CREATE EXTENSION postgis;' was run on your DB. Railway usually does this by default.
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# 3. Scraper Configuration
API_ENDPOINT = "https://data.cityofnewyork.us/resource/5zhs-2jue.geojson"
BATCH_SIZE = 1000  # How many records to fetch at once

def process_feature(feature):
    """Converts a GeoJSON feature into a DB Object"""
    props = feature['properties']
    geo = feature['geometry']

    # Convert GeoJSON geometry to WKB (Well Known Binary) for PostGIS using Shapely
    shapely_geom = shape(geo)
    
    # Handle data type conversion safely
    try:
        c_year = int(props.get('construction_year')) if props.get('construction_year') else None
    except ValueError:
        c_year = None

    try:
        h_roof = float(props.get('height_roof')) if props.get('height_roof') else None
    except ValueError:
        h_roof = None

    return NycBuilding(
        bin=props.get('bin'),
        base_bbl=props.get('base_bbl'),
        construction_year=c_year,
        height_roof=h_roof,
        doitt_id=int(props.get('doitt_id')) if props.get('doitt_id') else None,
        raw_properties=props,
        geom=shapely_geom.wkt  # GeoAlchemy will handle the WKT conversion
    )

def run_scraper():
    offset = 0
    total_inserted = 0
    
    print("🚀 Starting Scraper...")

    while True:
        # Fetch data with pagination
        params = {
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "doitt_id" # Order ensures we don't get duplicates/missing pages
        }
        
        try:
            response = requests.get(API_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ API Error: {e}")
            break

        features = data.get('features', [])
        
        if not features:
            print("✅ No more data found. Scraping complete.")
            break

        # Process batch
        new_objects = []
        for feat in features:
            # Check if BIN already exists to prevent duplicates (Simple check)
            # For high performance, use 'ON CONFLICT DO NOTHING' in raw SQL, 
            # but this is safer for a basic script.
            bin_id = feat['properties'].get('bin')
            exists = session.query(NycBuilding).filter_by(bin=bin_id).first()
            
            if not exists:
                new_objects.append(process_feature(feat))

        if new_objects:
            session.add_all(new_objects)
            session.commit()
            total_inserted += len(new_objects)
            print(f"🔹 Offset {offset}: Inserted {len(new_objects)} buildings.")
        else:
            print(f"🔸 Offset {offset}: Skipped (All exist).")

        offset += BATCH_SIZE

    print(f"🎉 Finished! Total records inserted: {total_inserted}")

if __name__ == "__main__":
    run_scraper()
