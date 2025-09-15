# /media/bodega/procesador/web_app/app.py --- VERSIÓN FINAL DEFINITIVA ---
import os
import json
import uuid
import redis
import subprocess
import threading
import logging
import traceback
import pytz
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# aqui aja #tusabes #padrelinero
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


# En app.py, al principio
import sys
# Esta es la forma más robusta de importar un script de una carpeta hermana.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts import run_tcpa_search as tcpa_script


# --- CONFIGURACIÓN DEL LOG PRINCIPAL ---
log_dir = '/media/bodega/procesador/logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [FlaskWebApp] - %(message)s', handlers=[logging.FileHandler(os.path.join(log_dir, 'web_app.log')), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DEL LOG DE AUDITORÍA ---
audit_log_path = os.path.join(log_dir, 'search_audit.log')
audit_handler = logging.FileHandler(audit_log_path)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger = logging.getLogger('search_audit')
audit_logger.setLevel(logging.INFO)
audit_logger.addHandler(audit_handler)
audit_logger.propagate = False

# --- INICIALIZACIÓN DE LA APP FLASK ---
app = Flask(__name__, template_folder='/media/bodega/procesador/templates')
app.secret_key = 'your-secret-key-change-this-to-something-secure'

# --- CONFIGURACIÓN Y GLOBALES ---
ADMIN_PASSWORD = 'dejameacuerdo'
UPLOAD_FOLDER = '/media/bodega/procesador/uploads'
SAFE_STORAGE = '/media/bodega/procesador/safe_storage'
ALLOWED_EXTENSIONS = {'txt', 'xlsx', 'csv'}
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
ALLOWED_IPS = ['127.0.0.1', '186.115.102.248', '192.168.1.25'] # Asegúrate de que tu IP esté aquí

# correccion del decorador de seguridad
def get_real_ip():
    if 'CF-Connecting-IP' in request.headers: return request.headers['CF-Connecting-IP']
    return request.remote_addr

def require_admin():
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get('admin_logged_in', False): return redirect(url_for('admin_login'))
            return f(*args, **kwargs)
        return wrapper
    return decorator

def apply_security_rules(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        ip_address = get_real_ip()
        if ip_address not in ALLOWED_IPS:
            audit_logger.critical(f"BLOQUEADO (IP no autorizada) - IP: {ip_address}")
            return jsonify({'error': 'Acceso desde tu IP no está permitido.'}), 403

        colombia_tz = pytz.timezone('America/Bogota')
        now = datetime.now(colombia_tz)
        is_weekday = 0 <= now.weekday() <= 4 and 8 <= now.hour < 18
        is_saturday = now.weekday() == 5 and 8 <= now.hour < 13
        
        user_agent = request.headers.get('User-Agent', 'Unknown')
        searched_number = request.args.get('number', 'N/A')

        if is_weekday or is_saturday:
            response = func(*args, **kwargs)
            if hasattr(response, 'is_json') and response.is_json and response.status_code == 200:
                searched_number = request.args.get('number', 'N/A')
                user_agent = request.headers.get('User-Agent', 'Unknown')
                audit_logger.info(f"ÉXITO - IP: {ip_address}, Búsqueda: {searched_number}, Cliente: {user_agent}")
            return response
        else:
            searched_number = request.args.get('number', 'N/A')
            user_agent = request.headers.get('User-Agent', 'Unknown')
            audit_logger.warning(f"BLOQUEADO (Fuera de Horario) - IP: {ip_address}, Intento: {searched_number}, Cliente: {user_agent}")
            return jsonify({'error': 'Acceso denegado: Fuera del horario laboral'}), 403

    return decorated_function

@app.after_request
def add_header(response):
    """
    Añade cabeceras a cada respuesta para evitar el caché.
    """
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


def move_to_safe_storage(uploaded_file_path, file_type):
    filename = os.path.basename(uploaded_file_path)
    if file_type == 'txt':
        safe_folder = os.path.join(SAFE_STORAGE, 'txt_files')
    else: # xlsx, csv
        date_folder = datetime.now().strftime('%Y-%m-%d')
        safe_folder = os.path.join(SAFE_STORAGE, 'spreadsheet_files', date_folder)
    os.makedirs(safe_folder, exist_ok=True)
    safe_path = os.path.join(safe_folder, filename)
    os.rename(uploaded_file_path, safe_path)
    return safe_path

def start_processing(job_id, file_path, file_type):
    try:
        script_path = None
        if file_type == 'txt':
            script_path = '/media/bodega/procesador/scripts/process_txt.py'
        elif file_type == 'suppression_xlsx':
            script_path = '/media/bodega/procesador/scripts/process_xlsx.py'
        elif file_type == 'suppression_csv':
            script_path = '/media/bodega/procesador/scripts/process_csv.py'
        elif file_type == 'sales_xlsx':
            script_path = '/media/bodega/procesador/scripts/process_sales.py'
        elif file_type == 'sales_csv':
            script_path = '/media/bodega/procesador/scripts/process_sales_csv.py'
        else:
            logger.error(f"Unknown file type for processing: {file_type}")
            return

        venv_python = '/media/bodega/procesador/bin/python'
        logger.info(f"Starting processing: {script_path} for job {job_id}")
        
        result = subprocess.run([venv_python, script_path, '--file-path', file_path, '--job-id', job_id], capture_output=True, text=True)
        
        if result.returncode != 0:
             logger.error(f"Script for job {job_id} failed. STDERR: {result.stderr}")
        else:
             logger.info(f"Script for job {job_id} completed. STDOUT: {result.stdout}")
    except Exception as e:
        logger.error(f"Failed to start processing thread for job {job_id}: {e}")

# --- RUTAS DE PÁGINAS ---
@app.route('/')
def index(): return render_template('index.html')
@app.route('/dnc')
def dnc_search(): return render_template('dnc_search.html')
@app.route('/suppression')
def suppression_search(): return render_template('suppression_search.html')
@app.route('/sales-search')
def sales_search(): return render_template('sales_search.html')
    
# --- RUTAS DE ADMIN ---
@app.route('/admin-login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin_logged_in'] = True; return redirect(url_for('admin'))
        else: flash('Contraseña inválida.', 'error')
    return render_template('admin_login.html')

@app.route('/admin-logout')
def admin_logout():
    session.pop('admin_logged_in', None); return redirect(url_for('index'))

@app.route('/admin')
@require_admin()
def admin():
    recent_jobs = []
    try:
        job_keys = sorted(redis_client.keys('job:*'), reverse=True)[:10]
        for key in job_keys: recent_jobs.append(json.loads(redis_client.get(key) or '{}'))
    except Exception as e: logger.error(f"No se pudieron obtener las tareas: {e}")
    return render_template('admin.html', recent_jobs=recent_jobs)
    
@app.route('/progress/<job_id>')
@require_admin()
def progress(job_id): return render_template('progress.html', job_id=job_id)

# --- RUTAS DE API ---
@app.route('/upload', methods=['POST'])
@require_admin()
def upload_file():
    if 'file' not in request.files or request.files['file'].filename == '':
        flash('No se seleccionó ningún archivo.', 'error')
        return redirect(url_for('admin'))
    
    file = request.files['file']
    file_type = request.form.get('file_type')
    
    if not file_type:
        flash('Por favor, selecciona un tipo de archivo.', 'error')
        return redirect(url_for('admin'))
        
    if file and file_type:
        job_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        upload_path = os.path.join(UPLOAD_FOLDER, f"{job_id}_{filename}")
        file.save(upload_path)
        safe_path = move_to_safe_storage(upload_path, file_type)
        
        job_data = {'job_id': job_id, 'filename': filename, 'file_type': file_type, 'status': 'queued', 'started_at': datetime.now().isoformat()}
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_data))
        
        thread = threading.Thread(target=start_processing, args=(job_id, safe_path, file_type))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('progress', job_id=job_id))
    
    flash('Tipo de archivo inválido.', 'error')
    return redirect(url_for('admin'))

@app.route('/api/progress/<job_id>')
def api_progress(job_id):
    job_data = redis_client.get(f'job:{job_id}')
    return jsonify(json.loads(job_data)) if job_data else ({'status': 'not_found'}, 404)

@app.route('/admin/clear-database', methods=['POST'])
@require_admin()
def clear_database():
    try:
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute(text("DROP TABLE IF EXISTS dnc_records;"))
            conn.execute(text("DROP TABLE IF EXISTS suppression_records;"))
            conn.execute(text("DROP TABLE IF EXISTS sales_records;"))
            trans.commit()
        flash('Todas las tablas de datos han sido eliminadas exitosamente.', 'success')
    except Exception as e:
        flash(f'Error al limpiar la base de datos: {e}', 'error')
    return redirect(url_for('admin'))

@app.route('/admin/database-stats')
@require_admin()
def database_stats():
    dnc_count, suppression_count, sales_count = 0, 0, 0
    state_breakdown = []
    try:
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        with engine.connect() as conn:
            try:
                dnc_count = conn.execute(text("SELECT COUNT(*) FROM dnc_records")).scalar_one()
                state_results = conn.execute(text("SELECT state, COUNT(*) as count FROM dnc_records GROUP BY state ORDER BY state")).fetchall()
                state_breakdown = [{'state': row.state, 'count': row.count} for row in state_results]
            except ProgrammingError: logger.warning("La tabla dnc_records no existe, se reportará 0.")
            except Exception as e: logger.error(f"No se pudo contar dnc_records: {e}")
            try:
                suppression_count = conn.execute(text("SELECT COUNT(*) FROM suppression_records")).scalar_one()
            except ProgrammingError: logger.warning("La tabla suppression_records no existe, se reportará 0.")
            except Exception as e: logger.error(f"No se pudo contar suppression_records: {e}")
            try:
                sales_count = conn.execute(text("SELECT COUNT(*) FROM sales_records")).scalar_one()
            except ProgrammingError: logger.warning("La tabla sales_records no existe, se reportará 0.")
            except Exception as e: logger.error(f"No se pudo contar sales_records: {e}")
        
        return jsonify({
            'total_records': dnc_count + suppression_count + sales_count,
            'record_counts': [
                {'type': 'Registros DNC', 'count': dnc_count},
                {'type': 'Registros de Supresión', 'count': suppression_count},
                {'type': 'Registros de Ventas', 'count': sales_count}
            ],
            'state_breakdown': state_breakdown
        })
    except Exception as e:
        logger.error(f"Error en la conexión para estadísticas: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
@apply_security_rules
def api_search():
    number = request.args.get('number')
    if not number: return jsonify({'error': 'Se requiere un número'}), 400
    try:
        search_number = int(''.join(filter(str.isdigit, number)))
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        with engine.connect() as conn:
            result = conn.execute(text("SELECT number, state FROM dnc_records WHERE number = :number"), {'number': search_number}).fetchone()
        if result: return jsonify({'found': True, 'number': str(result.number), 'state': result.state})
        else: return jsonify({'found': False})
    except Exception as e: logger.error(f"Error en búsqueda DNC: {e}"); return jsonify({'error': 'Error del servidor'}), 500


# En app.py, reemplaza la función api_suppression_search existente por esta

@app.route('/api/suppression-search')
@apply_security_rules
def api_suppression_search():
    number = request.args.get('number')
    if not number:
        return jsonify({'error': 'Se requiere un número'}), 400
    
    try:
        search_number = int(''.join(filter(str.isdigit, number)))
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        
        dnc_res, supp_res = None, None
        
        with engine.connect() as conn:
            # Lógica copiada directamente de la Búsqueda Maestra (garantiza que funciona)
            try:
                dnc_q = conn.execute(
                    text("SELECT * FROM dnc_records WHERE number = :num"),
                    {'num': search_number}
                ).fetchone()
                if dnc_q:
                    dnc_res = dnc_q._asdict()
            except ProgrammingError:
                logger.warning("La tabla dnc_records no existe, se omitirá en la búsqueda combinada.")
                pass

            try:
                supp_q = conn.execute(
                    text("SELECT * FROM suppression_records WHERE number = :num"),
                    {'num': search_number}
                ).fetchone()
                if supp_q:
                    supp_res = supp_q._asdict()
            except ProgrammingError:
                logger.warning("La tabla suppression_records no existe, se omitirá en la búsqueda combinada.")
                pass

        return jsonify({
            'found': bool(dnc_res or supp_res),
            'search_number': str(search_number),
            'dnc_status': dnc_res,
            'suppression_status': supp_res
        })
            
    except Exception as e:
        logger.error(f"Error en la búsqueda combinada: {e}")
        return jsonify({'error': 'Error del servidor durante la búsqueda.'}), 500
    
        

# --- RUTA DE BÚSQUEDA DE VENTAS CORREGIDA ---
@app.route('/api/sales-search')
@apply_security_rules
def api_sales_search():
    number = request.args.get('number')
    if not number: return jsonify({'error': 'Se requiere un número'}), 400
    try:
        search_number = int(''.join(filter(str.isdigit, number)))
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        
        sales_res = None
        with engine.connect() as conn:
            # Lógica clonada de la Búsqueda Maestra para máxima fiabilidad
            try:
                sales_q = conn.execute(text("SELECT * FROM sales_records WHERE primary_number = :num"), {'num': search_number}).fetchone()
                if sales_q:
                    sales_res = sales_q._asdict()
                    if sales_res.get('sale_date'):
                        sales_res['sale_date'] = sales_res['sale_date'].isoformat()
            except ProgrammingError:
                pass # Ignora si la tabla no existe

        # Construye la respuesta final
        if sales_res:
            response_data = {'found': True, **sales_res}
        else:
            response_data = {'found': False}
            
        # LOG DE DIAGNÓSTICO CLAVE
        logger.info(f"### DEBUG ### Respuesta JSON final para sales-search: {response_data}")
        
        return jsonify(response_data)
            
    except Exception as e:
        logger.error(f"Error en la búsqueda de ventas: {e}\n{traceback.format_exc()}")
        return jsonify({'error': 'Error del servidor durante la búsqueda.'}), 500


@app.route('/api/master-search')
@require_admin()
def api_master_search():
    number = request.args.get('number')
    if not number: return jsonify({'error': 'Se requiere un número'}), 400
    try:
        search_number = int(''.join(filter(str.isdigit, number)))
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        dnc_res, supp_res, sales_res = None, None, None
        with engine.connect() as conn:
            try:
                dnc_q = conn.execute(text("SELECT * FROM dnc_records WHERE number = :num"), {'num': search_number}).fetchone()
                if dnc_q: dnc_res = dnc_q._asdict()
            except ProgrammingError: pass
            try:
                supp_q = conn.execute(text("SELECT * FROM suppression_records WHERE number = :num"), {'num': search_number}).fetchone()
                if supp_q: supp_res = supp_q._asdict()
            except ProgrammingError: pass
            try:
                sales_q = conn.execute(text("SELECT * FROM sales_records WHERE primary_number = :num"), {'num': search_number}).fetchone()
                if sales_q:
                    sales_res = sales_q._asdict()
                    if sales_res.get('sale_date'): sales_res['sale_date'] = sales_res['sale_date'].isoformat()
            except ProgrammingError: pass
        return jsonify({'dnc_result': dnc_res, 'suppression_result': supp_res, 'sales_result': sales_res})
    except Exception as e:
        logger.error(f"Error en Búsqueda Maestra: {e}"); return jsonify({'error': str(e)}), 500

@app.route('/api/quick-check')
@require_admin()
def api_quick_check():
    table_name = request.args.get('table')
    allowed_tables = ['dnc_records', 'suppression_records', 'sales_records']
    if table_name not in allowed_tables:
        return jsonify({'error': 'Nombre de tabla inválido.'}), 400
    try:
        engine = create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')
        with engine.connect() as conn:
            query = text(f"SELECT * FROM {table_name} LIMIT 10")
            result = conn.execute(query).fetchall()
            records = [row._asdict() for row in result]
            for record in records:
                for key, value in record.items():
                    if isinstance(value, datetime.date):
                        record[key] = value.isoformat()
            return jsonify({'table_name': table_name, 'records': records})
    except Exception as e:
        logger.error(f"Error en Quick Check para {table_name}: {e}"); return jsonify({'error': str(e)}), 500


# nueva funcion de busqueda TCPA SIMPLE
@app.route('/tcpa-search-simple', methods=['GET', 'POST'])
@apply_security_rules
def tcpa_search_simple():
    # Usamos una clave en Redis como "cerrojo"
    lock_key = "tcpa_search_lock"
    
    # Comprobar si alguien ya está usando la herramienta
    if redis_client.get(lock_key):
        flash("El buscador está ocupado por otro usuario. Por favor, inténtalo en un minuto.", "warning")
        return render_template('tcpa_search_simple.html', is_busy=True)

    if request.method == 'POST':
        phone_number = request.form.get('phone_number', '').replace('-', '')
        if not phone_number.isdigit() or len(phone_number) < 9:
            flash('Por favor, ingresa un número válido.', 'error')
            return render_template('tcpa_search_simple.html')

        try:
            # Ponemos el "cerrojo" con un tiempo de vida de 90 segundos
            redis_client.set(lock_key, "busy", ex=90)
            
            # Ejecutamos la búsqueda directamente (esto bloqueará la app)
            resultado = tcpa_script.buscar_numero(phone_number)
            
            return render_template('tcpa_search_simple.html', resultado=resultado)
            
        finally:
            # Quitamos el "cerrojo" sin importar lo que pase
            redis_client.delete(lock_key)

    return render_template('tcpa_search_simple.html', is_busy=False)


@app.route('/tcpa-search', methods=['GET', 'POST'])
# @apply_security_rules # Puedes descomentar esto cuando todo funcione
def tcpa_search():
    lock_key = "tcpa_search_lock"
    
    if redis_client.get(lock_key):
        flash("El buscador TCPA está ocupado. Por favor, inténtalo en un minuto.", "warning")
        return render_template('tcpa_search.html', is_busy=True)

    if request.method == 'POST':
        phone_number = request.form.get('phone_number', '').replace('-', '')
        if not phone_number.isdigit() or len(phone_number) < 9:
            flash('Por favor, ingresa un número válido.', 'error')
            return render_template('tcpa_search.html')
        try:
            redis_client.set(lock_key, "busy", ex=90)
            resultado = tcpa_script.buscar_numero(phone_number)
            return render_template('tcpa_search.html', resultado=resultado)
        finally:
            redis_client.delete(lock_key)

    return render_template('tcpa_search.html', is_busy=False)



@app.route('/tcpa-result/<job_id>')
def tcpa_result(job_id):
    return render_template('tcpa_result.html', job_id=job_id)

@app.route('/api/tcpa-status/<job_id>')
def api_tcpa_status(job_id):
    job_data = redis_client.get(f'job:{job_id}')
    return jsonify(json.loads(job_data)) if job_data else ({'status': 'not_found'}, 404)


# --- EJECUCIÓN PRINCIPAL ---
if __name__ == '__main__':
    logger.info("🚀 Starting Flask app on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
