# /media/bodega/procesador/scripts/migrate_dnc_data.py
import time
import logging
from sqlalchemy import create_engine, text
import traceback
import os      # NEW: Import for process ID
import psutil  # NEW: Import for memory usage

# Setup basic logging for the migration script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_data():
    """
    A one-time script to migrate DNC data using a robust two-phase approach.
    Phase 1: Read all keys into memory.
    Phase 2: Process and write data in batches.
    """
    logger.info("🚀 Starting DNC data migration with new robust strategy...")
    engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
    
    BATCH_SIZE = 50000
    total_migrated = 0
    start_time = time.time()

    try:
        with engine.connect() as conn:
            # --- Setup Phase ---
            logger.info("Verifying 'dnc_records' table exists...")
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dnc_records (
                number BIGINT PRIMARY KEY,
                state VARCHAR(10),
                created_at TIMESTAMP DEFAULT NOW()
            );
            """))
            conn.commit()
            logger.info("✅ 'dnc_records' table is ready.")

            # --- PHASE 1: Read all DNC numbers into memory ---
            logger.info("Reading all DNC phone numbers from old table into memory...")
            dnc_numbers_result = conn.execute(text("SELECT number FROM records WHERE is_dnc = TRUE")).fetchall()
            dnc_numbers = [row.number for row in dnc_numbers_result]
            total_to_migrate = len(dnc_numbers)
            
            # --- NEW: Log actual memory usage ---
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            rss_mb = mem_info.rss / (1024 * 1024) # Resident Set Size in Megabytes
            logger.info(f"✅ Successfully loaded {total_to_migrate:,} numbers. Peak memory usage: {rss_mb:.2f} MB.")
            
            if total_to_migrate == 0:
                logger.info("No DNC records found to migrate. Exiting.")
                return


        # --- PHASE 2: Process and write the data in batches ---
        logger.info(f"Beginning migration in batches of {BATCH_SIZE:,}...")
        for i in range(0, total_to_migrate, BATCH_SIZE):
            batch_numbers = dnc_numbers[i:i + BATCH_SIZE]
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # Fetch the full row data for the current batch of numbers
                    rows_to_migrate = conn.execute(
                        text("SELECT number, state FROM records WHERE number = ANY(:numbers)"),
                        {'numbers': batch_numbers}
                    ).fetchall()

                    # Insert the fetched data into the new table
                    for row in rows_to_migrate:
                        conn.execute(text("""
                            INSERT INTO dnc_records (number, state)
                            VALUES (:number, :state)
                            ON CONFLICT (number) DO NOTHING;
                        """), {
                            'number': row.number,
                            'state': row.state or 'UNKNOWN'
                        })
                    
                    trans.commit()
                    total_migrated += len(rows_to_migrate)
                    percentage = (total_migrated / total_to_migrate) * 100
                    logger.info(f"Moved {total_migrated:,} / {total_to_migrate:,} records ({percentage:.2f}%)")

                except Exception:
                    logger.error("Error processing a batch. Rolling back this batch and stopping.")
                    trans.rollback()
                    raise

    except Exception as e:
        logger.error("A fatal error occurred during migration.")
        logger.error(traceback.format_exc())

    end_time = time.time()
    logger.info(f"✅ Migration process finished. Total records moved: {total_migrated:,}. Total time: {end_time - start_time:.2f} seconds.")

if __name__ == '__main__':
    migrate_data()