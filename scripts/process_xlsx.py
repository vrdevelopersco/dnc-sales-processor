# /media/bodega/procesador/scripts/process_xlsx.py
import argparse, json, pandas as pd, redis, os, traceback, logging
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('/media/bodega/procesador/logs/xlsx_processing.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class XLSXProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        """Creates the dedicated suppression_records table."""
        with self.engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS suppression_records (
                number BIGINT PRIMARY KEY,
                commodity VARCHAR(255),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """))
            conn.commit()
            logger.info("Table 'suppression_records' verified.")
            
    def insert_suppression_chunk(self, records):
        """Inserts a chunk of suppression data into the suppression_records table."""
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                for record in records:
                    conn.execute(text("""
                        INSERT INTO suppression_records (number, commodity, updated_at)
                        VALUES (:number, :commodity, NOW())
                        ON CONFLICT (number) DO UPDATE SET
                            commodity = EXCLUDED.commodity,
                            updated_at = NOW();
                    """), {'number': record['number'], 'commodity': record['commodity']})
                trans.commit()
            except Exception as e:
                logger.error(f"Error during chunk insert, rolling back. Error: {e}")
                trans.rollback()
                raise

    def update_progress(self, current, total, status='processing', message=None):
        # This helper function remains the same
        try:
            job_data = json.loads(self.redis_client.get(f'job:{self.job_id}') or '{}')
            job_data.update({'current': current, 'total': total, 'status': status, 'progress': (current / total * 100) if total > 0 else 0, 'updated_at': datetime.now().isoformat()})
            if message: job_data['message'] = message
            self.redis_client.setex(f'job:{self.job_id}', 3600, json.dumps(job_data))
        except Exception as e: logger.error(f"Error updating progress: {e}")

    def process_file(self, file_path):
        try:
            self.update_progress(0, 100, 'processing', 'Reading XLSX file...')
            df = pd.read_excel(file_path, engine='openpyxl')
            df.columns = [str(c).lower().strip() for c in df.columns]
            if 'serv_phone_num' not in df.columns or 'commodity' not in df.columns: raise ValueError("'Serv_phone_num' and 'Commodity' columns are required.")
            total_rows = len(df)
            self.update_progress(0, total_rows, 'processing', f'Found {total_rows} records.')
            
            chunk_size, total_processed, bad_rows_skipped = 5000, 0, 0
            
            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                chunk_df = df.iloc[start:end]
                records_to_insert = []
                
                for index, row in chunk_df.iterrows():
                    try: # This is the robust row-level error handling
                        phone_raw, commodity_raw = str(row.get('serv_phone_num', '')), str(row.get('commodity', ''))
                        phone_clean = ''.join(filter(str.isdigit, phone_raw))
                        if len(phone_clean) >= 10 and commodity_raw:
                            records_to_insert.append({'number': int(phone_clean), 'commodity': commodity_raw.strip()})
                    except Exception as row_error:
                        bad_rows_skipped += 1
                        logger.warning(f"Skipping bad row #{start + index + 2} due to error: {row_error}")
                
                if records_to_insert:
                    self.insert_suppression_chunk(records_to_insert)
                
                total_processed += len(chunk_df)
                self.update_progress(total_processed, total_rows, 'processing', f'Processed {total_processed} records.')
            
            completion_message = f'Successfully processed file. Total Skipped Rows: {bad_rows_skipped}'
            self.update_progress(total_rows, total_rows, 'completed', completion_message)
        except Exception as e:
            error_msg = f"FATAL ERROR processing XLSX file: {e}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.update_progress(0, 100, 'failed', error_msg)

def main():
    # This remains the same
    parser = argparse.ArgumentParser(description='Process XLSX suppression file')
    parser.add_argument('--file-path', required=True)
    parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    processor = XLSXProcessor(args.job_id)
    processor.process_file(args.file_path)

if __name__ == '__main__':
    main()