# /media/bodega/procesador/scripts/run_tcpa_search.py --- VERSIÓN FINAL CON BÚSQUEDA PRECISA ---
import sys, os, tempfile, shutil, time, re, traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def buscar_numero(numero_a_buscar):
    temp_profile_dir = tempfile.mkdtemp()
    options = webdriver.ChromeOptions()
    #options.add_argument('--headless');
    options.add_argument('--no-sandbox');
    options.add_argument('--disable-dev-shm-usage'); options.add_argument('--disable-gpu');
    options.add_argument(f"--user-data-dir={temp_profile_dir}")
    driver = None
    try:
        service = ChromeService()
        driver = webdriver.Chrome(service=service, options=options)
        time.sleep(2)
        driver.get("https://tcpalitigatorlist.com/")
        wait = WebDriverWait(driver, 60)

        # Rellenar formulario (sin cambios)
        wait.until(EC.element_to_be_clickable((By.ID, "snlu_phone_number"))).send_keys(numero_a_buscar)
        driver.find_element(By.ID, "snlu_phone").send_keys(numero_a_buscar)
        driver.find_element(By.ID, "snlu_email").send_keys("test@example.com")
        driver.execute_script("arguments[0].click();", driver.find_element(By.CSS_SELECTOR, "button.tf-button"))

        # Espera a que el texto genérico "Search result" aparezca para saber que la página respondió.
        wait.until(EC.text_to_be_present_in_element((By.ID, "single-number-look-up-front"), "Search result"))
        time.sleep(2) # Pausa final para que la tabla se dibuje

        # --- LÓGICA DE EXTRACCIÓN PRECISA ---
        # 1. Primero, encuentra el contenedor principal de los resultados.
        results_container = driver.find_element(By.ID, "single-number-look-up-front")
        
        # 2. Ahora, busca la tabla de resultados ÚNICAMENTE DENTRO de ese contenedor.
        #    Esto ignora por completo la tabla de "Last week..." que está afuera.
        soup = BeautifulSoup(results_container.get_attribute('innerHTML'), 'html.parser')
        tabla = soup.find("table", class_="llp-admin-table")

        if not tabla or "No results" in results_container.text:
            return {'found': False, 'message': f"El número '{numero_a_buscar}' no fue encontrado."}
        else:
            celdas = tabla.find('tbody').find('tr').find_all('td')
            return {"Number": celdas[0].text.strip(), "Status": celdas[1].text.strip(), "Case title": celdas[2].text.strip(), "Multiple Cases": celdas[3].text.strip(), "Phone type": celdas[4].text.strip(), 'found': True}
            
    except Exception as e:
        return {'found': False, 'message': f"Ocurrió un error: {traceback.format_exc()}"}
    finally:
        if driver: driver.quit()
        if os.path.exists(temp_profile_dir): shutil.rmtree(temp_profile_dir)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        import json
        print(json.dumps(buscar_numero(sys.argv[1]), indent=2))