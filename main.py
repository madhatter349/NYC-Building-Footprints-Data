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

# --- FIX: Handle SQLAlchemy protocol mismatch ---
# Railway provides 'postgres://', but SQLAlchemy 1.4+ requires 'postgresql://'
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found. Make sure it is set in .env or Railway variables.")

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

# Create the table if it doesn't exist
# This will also ensure the PostGIS extension functions are available
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

    # Convert GeoJSON geometry to WKT (Well Known Text) for PostGIS using Shapely
    # This handles the conversion from the API's JSON format to what the DB expects
    shapely_geom = shape(geo)
    
    # Handle data type conversion safely (handle None or empty strings)
    try:
        c_year = int(props.get('construction_year')) if props.get('construction_year') else None
    except ValueError:
        c_year = None

    try:
        h_roof = float(props.get('height_roof')) if props.get('height_roof') else None
    except ValueError:
        h_roof = None
    
    try:
        d_id = int(props.get('doitt_id')) if props.get('doitt_id') else None
    except ValueError:
        d_id = None

    return NycBuilding(
        bin=props.get('bin'),
        base_bbl=props.get('base_bbl'),
        construction_year=c_year,
        height_roof=h_roof,
        doitt_id=d_id,
        raw_properties=props,
        geom=shapely_geom.wkt 
    )

def run_scraper():
    offset = 0
    total_inserted = 0
    
    print("Starting Scraper...")

    while True:
        # Fetch data with pagination
        params = {
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "doitt_id" # Order ensures we don't get duplicates/missing pages
        }
        
        try:
            print(f"Fetching offset {offset}...")
            response = requests.get(API_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"API Error: {e}")
            break

        features = data.get('features', [])
        
        if not features:
            print("No more data found. Scraping complete.")
            break

        # Process batch
        new_objects = []
        
        # Get list of BINs in this batch to minimize DB queries
        batch_bins = [f['properties'].get('bin') for f in features if f['properties'].get('bin')]
        
        # Check which BINs already exist in DB
        existing_bins = set()
        if batch_bins:
            existing_records = session.query(NycBuilding.bin).filter(NycBuilding.bin.in_(batch_bins)).all()
            existing_bins = {r[0] for r in existing_records}

        for feat in features:
            bin_id = feat['properties'].get('bin')
            
            # Only add if it doesn't exist
            if bin_id not in existing_bins:
                new_objects.append(process_feature(feat))
                # Add to set so we don't try to add duplicates within the same batch
                existing_bins.add(bin_id)

        if new_objects:
            try:
                session.add_all(new_objects)
                session.commit()
                total_inserted += len(new_objects)
                print(f"Offset {offset}: Inserted {len(new_objects)} buildings.")
            except Exception as e:
                session.rollback()
                print(f"DB Error on offset {offset}: {e}")
        else:
            print(f"Offset {offset}: Skipped (All exist).")

        offset += BATCH_SIZE

    print(f"Finished! Total records inserted: {total_inserted}")

if __name__ == "__main__":
    run_scraper()
