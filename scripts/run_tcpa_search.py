# /media/bodega/procesador/scripts/run_tcpa_search.py
import sys, json, redis
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def buscar_y_guardar(numero_a_buscar, job_id):
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    
    def update_status(status, message, result=None):
        job_data = {'status': status, 'message': message, 'result': result}
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_data))

    update_status('processing', 'Iniciando navegador...')
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    driver = None
    
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get("https://tcpalitigatorlist.com/")
        wait = WebDriverWait(driver, 20)

        update_status('processing', 'Página cargada. Rellenando formulario...')
        wait.until(EC.element_to_be_clickable((By.ID, "snlu_phone_number"))).send_keys(numero_a_buscar)
        driver.find_element(By.ID, "snlu_phone").send_keys(numero_a_buscar)
        driver.find_element(By.ID, "snlu_email").send_keys("test@example.com")
        
        update_status('processing', 'Enviando consulta...')
        driver.execute_script("arguments[0].click();", driver.find_element(By.CSS_SELECTOR, "button.tf-button"))

        update_status('processing', 'Esperando resultados...')
        wait.until(EC.text_to_be_present_in_element((By.ID, "single-number-look-up-front"), "Search result"))
        
        update_status('processing', 'Analizando resultados...')
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        tabla = soup.find("table", class_="llp-admin-table")

        if not tabla:
            update_status('completed', f"El número '{numero_a_buscar}' no fue encontrado.", {'found': False})
            return

        celdas = tabla.find('tbody').find('tr').find_all('td')
        resultado = {
            "Number": celdas[0].text.strip(), "Status": celdas[1].text.strip(),
            "Case title": celdas[2].text.strip(), "Multiple Cases": celdas[3].text.strip(),
            "Phone type": celdas[4].text.strip(), 'found': True
        }
        update_status('completed', "Búsqueda finalizada con éxito.", resultado)

    except Exception as e:
        update_status('failed', f"Ocurrió un error: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    job_id_arg = sys.argv[1]
    numero_arg = sys.argv[2]
    buscar_y_guardar(numero_arg, job_id_arg)

