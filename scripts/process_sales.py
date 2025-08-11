# /media/bodega/procesador/scripts/process_sales.py --- VERSIÓN FINAL CON COMBINACIÓN AVANZADA ---
import argparse, json, pandas as pd, redis, os, traceback, logging, re
from datetime import datetime
from sqlalchemy import create_engine, text, inspect

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('/media/bodega/procesador/logs/sales_processing.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class SalesProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        self.verify_and_maintain_schema()

    def update_progress(self, current, total, status='processing', message=None):
        try:
            job_data = json.loads(self.redis_client.get(f'job:{self.job_id}') or '{}')
            job_data.update({'current': current, 'total': total, 'status': status, 'progress': (current / total * 100) if total > 0 else 0, 'updated_at': datetime.now().isoformat()})
            if message: job_data['message'] = message
            self.redis_client.setex(f'job:{self.job_id}', 3600, json.dumps(job_data))
        except Exception as e: logger.error(f"Error actualizando el progreso: {e}")

    def verify_and_maintain_schema(self):
        inspector = inspect(self.engine)
        with self.engine.connect() as conn:
            if not inspector.has_table("sales_records"):
                conn.execute(text("""CREATE TABLE sales_records (primary_number BIGINT PRIMARY KEY, sale_date DATE, alternate_number BIGINT, provider TEXT, commodity TEXT, comments TEXT, updated_at TIMESTAMP DEFAULT NOW());"""))
                conn.commit()
        logger.info("Verificación de esquema completada.")

    def parse_phone_numbers(self, text_field):
        if not isinstance(text_field, str): return None, None
        all_digits = "".join(re.findall(r'\d', text_field))
        primary = int(all_digits[0:10]) if len(all_digits) >= 10 else None
        alternate = int(all_digits[10:20]) if len(all_digits) >= 20 else None
        return primary, alternate

    def aggregate_sales_data(self, group):
        """Función para agregar los datos de ventas de un número duplicado."""
        if group.empty:
            return None
        # Ordena por fecha para determinar la venta principal
        sorted_group = group.sort_values(by='sale_date', ascending=True)
        
        first_record = sorted_group.iloc[0]
        other_records = sorted_group.iloc[1:]
        
        # El proveedor principal es el de la primera venta
        final_provider = str(first_record['provider']) if pd.notna(first_record['provider']) else ""
        
        # Anexa los proveedores y fechas de las ventas subsecuentes
        for _, row in other_records.iterrows():
            provider_name = str(row['provider']) if pd.notna(row['provider']) else "N/A"
            provider_date = row['sale_date'].strftime('%d/%m/%y') if pd.notna(row['sale_date']) else 'N/A'
            final_provider += f" ({provider_name}: {provider_date})"
            
        # Construye el registro final, usando los datos del primer registro como base
        final_record = first_record.copy()
        final_record['provider'] = final_provider
        
        return final_record

    def process_file(self, file_path):
        try:
            self.update_progress(0, 100, 'processing', 'Leyendo archivo de ventas...')
            df = pd.read_excel(file_path, engine='openpyxl', dtype=str)
            df.columns = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
            
            phone_col_name = next((col for col in df.columns if 'number' in col), None)
            date_col_name = next((col for col in df.columns if 'date' in col), None)
            if not phone_col_name or not date_col_name:
                raise ValueError("El archivo debe contener una columna de número y una de fecha.")

            all_records = []
            for _, row in df.iterrows():
                primary_num, alternate_num = self.parse_phone_numbers(row.get(phone_col_name))
                if primary_num:
                    all_records.append({
                        'primary_number': primary_num, 'alternate_number': alternate_num,
                        'sale_date': pd.to_datetime(row.get(date_col_name), dayfirst=True, errors='coerce'),
                        'provider': str(row.get('provider', '')).strip(),
                        'commodity': str(row.get('commodity', '')).strip(),
                        'comments': str(row.get('notes', '')).strip() if pd.notna(row.get('notes')) else None
                    })
            
            processed_df = pd.DataFrame(all_records).dropna(subset=['primary_number'])
            logger.info(f"Extracción completada. {len(processed_df):,} registros válidos extraídos.")

            # --- LÓGICA DE AGRUPACIÓN AVANZADA ---
            logger.info("Agrupando registros y combinando datos de proveedores...")
            final_df = processed_df.groupby('primary_number').apply(self.aggregate_sales_data).reset_index(drop=True)
            records_to_insert = final_df.to_dict('records')
            
            total_good_rows = len(records_to_insert)
            self.update_progress(0, total_good_rows, 'processing', f'Insertando {total_good_rows:,} registros únicos y combinados...')

            # --- INSERCIÓN EN LA BASE DE DATOS ---
            with self.engine.connect() as conn:
                logger.info("Limpiando datos de ventas antiguos...")
                trans = conn.begin()
                try:
                    conn.execute(text("TRUNCATE TABLE sales_records;"))
                    for record in records_to_insert:
                        if pd.isna(record['sale_date']): record['sale_date'] = None
                        else: record['sale_date'] = record['sale_date'].date()
                        if 'alternate_number' in record and pd.isna(record['alternate_number']): record['alternate_number'] = None
                        
                        conn.execute(text("INSERT INTO sales_records (primary_number, sale_date, alternate_number, provider, commodity, comments, updated_at) VALUES (:primary_number, :sale_date, :alternate_number, :provider, :commodity, :comments, NOW())"), record)
                    trans.commit()
                    logger.info("### ¡ÉXITO TOTAL! La transacción se ha guardado permanentemente. ###")
                except Exception as e:
                    logger.error(f"Error durante la inserción, revirtiendo cambios. Error: {e}"); trans.rollback(); raise
                
                self.update_progress(total_good_rows, total_good_rows, 'completed', f"Éxito! Insertados {total_good_rows:,} registros únicos.")
        except Exception as e:
            error_msg = f"ERROR FATAL: {e}\n{traceback.format_exc()}"
            self.update_progress(0, 100, 'failed', error_msg)

def main():
    parser = argparse.ArgumentParser(description='Process Sales XLSX file');
    parser.add_argument('--file-path', required=True); parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    SalesProcessor(args.job_id).process_file(args.file_path)

if __name__ == '__main__':
    main()