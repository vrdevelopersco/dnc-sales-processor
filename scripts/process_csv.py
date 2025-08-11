# /media/bodega/procesador/scripts/process_csv.py --- VERSIÓN FINAL INTELIGENTE ---
import argparse, json, pandas as pd, redis, os, traceback, logging
from datetime import datetime
from sqlalchemy import create_engine, text

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('/media/bodega/procesador/logs/csv_processing.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')

    def update_progress(self, current, total, status='processing', message=None):
        try:
            job_data = json.loads(self.redis_client.get(f'job:{self.job_id}') or '{}')
            job_data.update({'current': current, 'total': total, 'status': status, 'progress': (current / total * 100) if total > 0 else 0, 'updated_at': datetime.now().isoformat()})
            if message: job_data['message'] = message
            self.redis_client.setex(f'job:{self.job_id}', 3600, json.dumps(job_data))
        except Exception as e: logger.error(f"Error actualizando el progreso: {e}")

    def process_file(self, file_path):
        try:
            # PASO 1: CARGAR EL ARCHIVO COMPLETO
            self.update_progress(0, 100, 'processing', 'Cargando archivo en memoria...')
            logger.info("Intentando cargar el archivo CSV completo en memoria...")
            df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig', dtype=str, on_bad_lines='warn')
            df.columns = [str(c).lower().strip() for c in df.columns]
            total_rows = len(df)
            logger.info(f"Se cargaron {total_rows:,} filas. Validando y preparando datos...")

            # PASO 2: VALIDAR Y PREPARAR DATOS
            if 'serv_phone_num' not in df.columns or 'commodity' not in df.columns:
                raise ValueError(f"No se encontraron las columnas requeridas. Columnas encontradas: {list(df.columns)}")

            # Limpiar números de teléfono
            df['number'] = df['serv_phone_num'].str.replace(r'\D', '', regex=True)
            
            # Filtrar filas con números de teléfono inválidos
            valid_df = df[df['number'].str.len() >= 9].copy()
            bad_rows_skipped = total_rows - len(valid_df)
            
            # Reemplazar commodities en blanco
            valid_df['commodity'] = valid_df['commodity'].str.strip()
            valid_df.loc[valid_df['commodity'] == '', 'commodity'] = 'UNKNOWN COMMODITY. CONTACT ADMIN'
            
            # Convertir el número a entero para agrupar
            valid_df['number'] = pd.to_numeric(valid_df['number'])

            # --- LÓGICA CLAVE: Agrupar por número y combinar commodities ---
            logger.info("Agrupando duplicados dentro del archivo...")
            # La función de agregación toma todos los commodities para un número, elimina duplicados, los ordena y los une.
            agg_func = lambda x: ' and '.join(sorted(list(set(x))))
            aggregated_df = valid_df.groupby('number').agg(commodity=('commodity', agg_func)).reset_index()
            
            records_to_insert = aggregated_df.to_dict('records')
            total_good_rows = len(records_to_insert)
            
            logger.info(f"Validación completa. Registros a insertar (únicos): {total_good_rows:,}, Registros omitidos (teléfono inválido): {bad_rows_skipped:,}")
            self.update_progress(0, total_good_rows, 'processing', f'Validación completa. Insertando {total_good_rows:,} registros únicos...')

            # PASO 3: LIMPIAR TABLA E INSERTAR DATOS
            with self.engine.connect() as conn:
                logger.info("Limpiando datos de supresión antiguos...")
                conn.execute(text("CREATE TABLE IF NOT EXISTS suppression_records (number BIGINT PRIMARY KEY, commodity VARCHAR(255), updated_at TIMESTAMP);"))
                conn.execute(text("TRUNCATE TABLE suppression_records;"))
                conn.commit()
                logger.info("Tabla truncada. Empezando inserción.")

                chunk_size = 10000
                with conn.begin(): # Usar una sola transacción para todas las inserciones por velocidad
                    for i in range(0, total_good_rows, chunk_size):
                        batch = records_to_insert[i:i + chunk_size]
                        for record in batch:
                            # --- LÓGICA SQL INTELIGENTE PARA COMBINAR ---
                            conn.execute(text("""
                                INSERT INTO suppression_records (number, commodity, updated_at)
                                VALUES (:number, :commodity, NOW())
                                ON CONFLICT (number) DO UPDATE SET
                                    commodity = CASE
                                        -- Si el commodity ya existe en la cadena, no hacer nada.
                                        WHEN STRPOS(suppression_records.commodity, EXCLUDED.commodity) > 0 THEN suppression_records.commodity
                                        -- Si el commodity existente es el valor por defecto, reemplazarlo.
                                        WHEN suppression_records.commodity = 'UNKNOWN COMMODITY. CONTACT ADMIN' THEN EXCLUDED.commodity
                                        -- De lo contrario, combinarlo.
                                        ELSE suppression_records.commodity || ' and ' || EXCLUDED.commodity
                                    END,
                                    updated_at = NOW();
                            """), record)
                        self.update_progress(i + len(batch), total_good_rows, 'processing', f'Insertados {i + len(batch):,} registros...')

            completion_message = f"Carga completa. Registros únicos insertados: {total_good_rows:,}. Omitidos (teléfono inválido): {bad_rows_skipped:,}"
            self.update_progress(total_good_rows, total_good_rows, 'completed', completion_message)

        except Exception as e:
            error_msg = f"ERROR FATAL: {e}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.update_progress(0, 100, 'failed', error_msg)

def main():
    parser = argparse.ArgumentParser(description='Procesa archivo CSV de supresión')
    parser.add_argument('--file-path', required=True); parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    CSVProcessor(args.job_id).process_file(args.file_path)

if __name__ == '__main__':
    main()