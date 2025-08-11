# /media/bodega/procesador/scripts/process_sales_csv.py
import argparse, json, pandas as pd, redis, os, traceback, logging
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('/media/bodega/procesador/logs/sales_processing.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class SalesCSVProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        # The create_table function is called from the process_file method to ensure it exists
        
    def update_progress(self, current, total, status='processing', message=None):
        try:
            job_data = json.loads(self.redis_client.get(f'job:{self.job_id}') or '{}')
            job_data.update({'current': current, 'total': total, 'status': status, 'progress': (current / total * 100) if total > 0 else 0, 'updated_at': datetime.now().isoformat()})
            if message: job_data['message'] = message
            self.redis_client.setex(f'job:{self.job_id}', 3600, json.dumps(job_data))
        except Exception as e: logger.error(f"Error updating progress: {e}")

    def process_file(self, file_path):
        try:
            self.update_progress(0, 100, 'processing', 'Reading Sales CSV file...')
            df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig', dtype=str, on_bad_lines='warn')
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            records_to_insert = []
            for _, row in df.iterrows():
                primary_clean = ''.join(filter(str.isdigit, str(row.get('primary_number', ''))))
                if len(primary_clean) >= 9:
                    alt_clean = ''.join(filter(str.isdigit, str(row.get('alternate_number', ''))))
                    records_to_insert.append({
                        'primary_number': int(primary_clean),
                        'alternate_number': int(alt_clean) if alt_clean else None,
                        'sale_date': pd.to_datetime(row.get('sale_date')).date() if pd.notna(row.get('sale_date')) else None,
                        'provider': str(row.get('provider', '')).strip(),
                        'commodity': str(row.get('commodity', '')).strip(),
                        'comments': str(row.get('comments', '')).strip()
                    })
            
            total_good_rows = len(records_to_insert)
            self.update_progress(0, total_good_rows, 'processing', f'Inserting {total_good_rows:,} valid sales records...')

            with self.engine.connect() as conn:
                # Create table and clear old data in one transaction
                conn.execute(text("CREATE TABLE IF NOT EXISTS sales_records (primary_number BIGINT PRIMARY KEY, sale_date DATE, alternate_number BIGINT, provider VARCHAR(255), commodity VARCHAR(255), comments TEXT, updated_at TIMESTAMP DEFAULT NOW());"))
                conn.execute(text("TRUNCATE TABLE sales_records;"))
                conn.commit()

                # Insert new data
                with conn.begin():
                    for record in records_to_insert:
                        conn.execute(text("""
                            INSERT INTO sales_records (primary_number, sale_date, alternate_number, provider, commodity, comments, updated_at)
                            VALUES (:primary_number, :sale_date, :alternate_number, :provider, :commodity, :comments, NOW())
                        """), record)
                self.update_progress(total_good_rows, total_good_rows, 'completed', f"Successfully inserted {total_good_rows:,} sales records.")
        except Exception as e:
            error_msg = f"FATAL ERROR: {e}\n{traceback.format_exc()}"
            self.update_progress(0, 100, 'failed', error_msg)

def main():
    parser = argparse.ArgumentParser(description='Process Sales CSV file')
    parser.add_argument('--file-path', required=True); parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    SalesCSVProcessor(args.job_id).process_file(args.file_path)

if __name__ == '__main__':
    main()