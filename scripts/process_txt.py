# /media/bodega/procesador/scripts/process_txt.py
import argparse, json, redis, time, os, logging
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('/media/bodega/procesador/logs/txt_processing.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class TXTProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        """Creates the dedicated dnc_records table."""
        with self.engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dnc_records (
                number BIGINT PRIMARY KEY,
                state VARCHAR(10),
                created_at TIMESTAMP DEFAULT NOW()
            );
            """))
            conn.commit()
            logger.info("Table 'dnc_records' verified.")

    def insert_dnc_chunk(self, numbers, state_code):
        """Inserts a chunk of numbers into the dnc_records table."""
        db_state = state_code or 'UNKNOWN'
        with self.engine.connect() as conn:
            inserted_count = 0
            for number in numbers:
                try:
                    conn.execute(text("""
                        INSERT INTO dnc_records (number, state)
                        VALUES (:number, :state)
                        ON CONFLICT (number) DO NOTHING;
                    """), {'number': number, 'state': db_state})
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Error inserting DNC number {number}: {e}")
            conn.commit()
            return inserted_count
            
    def update_progress(self, current, total, status='processing', message=None):
        try:
            job_data = json.loads(self.redis_client.get(f'job:{self.job_id}') or '{}')
            job_data.update({'current': current, 'total': total, 'status': status, 'progress': (current / total * 100) if total > 0 else 0, 'updated_at': datetime.now().isoformat()})
            if message: job_data['message'] = message
            self.redis_client.setex(f'job:{self.job_id}', 3600, json.dumps(job_data))
        except Exception as e: logger.error(f"Error updating progress: {e}")

    def extract_state_from_filename(self, file_path):
        filename = os.path.basename(file_path).upper()
        state_codes = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC']
        for state in state_codes:
            if filename == f"{state}.TXT" or filename.startswith(f"{state}_") or filename.endswith(f"_{state}.TXT"): return state
        for state in state_codes:
            if state in filename: return state
        logger.warning(f"Could not extract state from filename: {filename}. Defaulting to UNKNOWN.")
        return 'UNKNOWN'

    def process_file(self, file_path):
        try:
            self.update_progress(0, 100, 'processing', 'Analyzing DNC file...')
            with open(file_path, 'r', errors='ignore') as f: total_lines = sum(1 for _ in f)
            state_code = self.extract_state_from_filename(file_path)
            self.update_progress(0, total_lines, 'processing', f'Processing DNC numbers for state {state_code}...')

            chunk_size, chunk_numbers, total_inserted = 10000, [], 0
            with open(file_path, 'r', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    number_clean = ''.join(filter(str.isdigit, line.split(',')[0]))
                    if len(number_clean) >= 10: chunk_numbers.append(int(number_clean))
                    if len(chunk_numbers) >= chunk_size:
                        total_inserted += self.insert_dnc_chunk(chunk_numbers, state_code)
                        chunk_numbers = []
                        self.update_progress(line_num, total_lines, 'processing', f'Processed {total_inserted} DNC numbers for {state_code}')
                if chunk_numbers: total_inserted += self.insert_dnc_chunk(chunk_numbers, state_code)
            self.update_progress(total_lines, total_lines, 'completed', f'Successfully processed {total_inserted} DNC numbers.')
        except Exception as e:
            logger.error(f"Error processing DNC file: {e}")
            self.update_progress(0, 100, 'failed', str(e))

def main():
    parser = argparse.ArgumentParser(description='Process DNC TXT file')
    parser.add_argument('--file-path', required=True)
    parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    processor = TXTProcessor(args.job_id)
    processor.process_file(args.file_path)

if __name__ == '__main__':
    main()