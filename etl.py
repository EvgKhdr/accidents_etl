import json
import os
import glob
import logging
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

directory_name = "json_accidents_data"

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "your_database"),
    "user":     os.getenv("DB_USER", "your_user"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
}

JSON_DIR = "json_accidents_data"


VALID_SEVERITY = {"Fatal", "Slight", "Serious", 'Unknown'}
VALID_BOROUGHS = {
    "Brent", "Redbridge", "Waltham Forest", "Lewisham", "Wandsworth",
    "Barnet", "Southwark", "Bexley", "Lambeth", "Richmond upon Thames",
    "Hounslow", "Sutton", "Enfield", "City of Westminster", "Tower Hamlets",
    "Kensington and Chelsea", "Camden", "Islington", "City of London",
    "Havering", "Hammersmith and Fulham", "Newham", "Harrow", "Merton",
    "Bromley", "Hackney", "Hillingdon", "Ealing", "Greenwich", "Kingston",
    "Barking and Dagenham", "Croydon", "Haringey",'Unknown'
}
VALID_CLASS = {"Driver", "Pedestrian", "Passenger",'Unknown'}
VALID_MODE  = {
    "PrivateHire", "Taxi", "GoodsVehicle", "Car", "BusOrCoach",
    "Pedestrian", "PoweredTwoWheeler", "OtherVehicle", "PedalCycle",'Unknown'
}
VALID_AGE= {"Child", "Adult",'Unknown'}


def clean(value, valid_set, default="Unknown"):
    """Return value if it belongs to valid_set, else default."""
    return value if value in valid_set else default


def load_json_files(directory: str) -> list[dict]:
    """Load and merge all JSON files from directory (each file may be a list or a single object)."""
    pattern = os.path.join(directory, "**", "*.json")
    files   = glob.glob(pattern, recursive=True)

    if not files:

        files = glob.glob(os.path.join(directory, "*.json"))

    if not files:
        raise FileNotFoundError(f"No JSON files found in '{directory}'")

    records = []
    for path in sorted(files):
        log.info("Reading %s", path)
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            records.extend(data)
        elif isinstance(data, dict):
            records.append(data)
        else:
            log.warning("Unexpected JSON structure in %s – skipping", path)

    log.info("Loaded %d accident records from %d file(s)", len(records), len(files))
    return records



def run_etl(records: list[dict], conn) -> None:
    with conn.cursor() as cur:
        accident_rows = []
        for r in records:
            accident_rows.append((
                r["id"],
                r.get("lat"),
                r.get("lon"),
                r.get("date"),
                clean(r.get("severity"), VALID_SEVERITY),
                clean(r.get("borough"),  VALID_BOROUGHS),
            ))

        execute_batch(
            cur,
            """
            INSERT INTO accidents (accident_id, latitude, longitude, date, severity, borough)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (accident_id) DO UPDATE SET
                latitude  = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                date      = EXCLUDED.date,
                severity  = EXCLUDED.severity,
                borough   = EXCLUDED.borough
            """,
            accident_rows,
            page_size=500,
        )
        log.info("Upserted %d accident rows", len(accident_rows))


        accident_ids = [r["id"] for r in records]
        cur.execute(
            "SELECT accident_id, id FROM accidents WHERE accident_id = ANY(%s)",
            (accident_ids,),
        )
        pk_map = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute(
            "DELETE FROM casualties WHERE accident_id = ANY(%s)",
            (list(pk_map.values()),),
        )

        casualty_rows = []
        for r in records:
            pk = pk_map.get(r["id"])
            if pk is None:
                continue
            for c in r.get("casualties", []):
                casualty_rows.append((
                    pk,
                    c.get("age"),
                    clean(c.get("class"),    VALID_CLASS),
                    clean(c.get("mode"),     VALID_MODE),
                    clean(c.get("severity"), VALID_SEVERITY),
                    clean(c.get("ageBand"), VALID_AGE),
                ))

        if casualty_rows:
            execute_batch(
                cur,
                """
                INSERT INTO casualties (accident_id, age, class, mode, severity, age_band)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                casualty_rows,
                page_size=500,
            )
        log.info("Inserted %d casualty rows", len(casualty_rows))

        cur.execute(
            "DELETE FROM vehicles WHERE accident_id = ANY(%s)",
            (list(pk_map.values()),),
        )

        vehicle_rows = []
        for r in records:
            pk = pk_map.get(r["id"])
            if pk is None:
                continue
            for v in r.get("vehicles", []):
                vehicle_rows.append((pk, v.get("type", "Unknown")))

        if vehicle_rows:
            execute_batch(
                cur,
                """
                INSERT INTO vehicles (accident_id, type)
                VALUES (%s, %s)
                """,
                vehicle_rows,
                page_size=500,
            )
        log.info("Inserted %d vehicle rows", len(vehicle_rows))

    conn.commit()
    log.info("Transaction committed successfully ✓")



def main():
    records = load_json_files(JSON_DIR)

    log.info("Connecting to PostgreSQL at %s:%s/%s", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        run_etl(records, conn)
    except Exception:
        conn.rollback()
        log.exception("ETL failed – transaction rolled back")
        raise
    finally:
        conn.close()
        log.info("Connection closed")


if __name__ == "__main__":
    main()