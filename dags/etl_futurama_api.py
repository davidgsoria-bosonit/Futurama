"""
DAG ETL para extraer, validar y transformar datos de la API de Futurama
usando TaskFlow API y buenas prácticas de Airflow.
"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.dates import days_ago
from datetime import datetime
from pathlib import Path
import requests
import json
import csv
import hashlib
import logging
import os

## Muy bien comentarios de código para organizar el fichero
## Muy bien separar la función grande en varias funciones
## Muy bien uso de decoradores para PythonOperator, el código es más limpio. Ten en cuenta que solo sirve para PythonOperator
## Muy bien control de excepciones

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

## Bien usar Variable para ocultar URL, como lo harías usando Connection
## Si harcodeas por defecto la URL sigue estando en código, lo mejor es no mostrar URLs directamente en el código
API_URL = Variable.get(
    "futurama_api_url",
    default_var="https://api.sampleapis.com/futurama/characters"
)

BASE_DIR = Variable.get(
    "etl_base_path",
    default_var="/opt/airflow/data"
)

RAW_DIR = f"{BASE_DIR}/raw"
PROCESSED_DIR = f"{BASE_DIR}/processed"

REQUIRED_FIELDS = ["id", "name", "age"]
CSV_FIELDS = ["id", "name", "age", "gender", "occupation", "images", "sayings"]

logger = logging.getLogger(__name__)

# ============================================================================
# UTILIDADES
# ============================================================================

## Función bien definida, que podría pasar si el fichero con el que genera el hash es muy grande?
def calculate_hash(data: list) -> str:
    """Calcula un hash SHA256 del contenido JSON."""
    serialized = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()

# ============================================================================
# TASKS (TaskFlow API)
# ============================================================================

## Que cambiarías en esta función para que sea idempotente?
@task
def extract_data() -> dict:
    """
    Extrae datos de la API.
    - Si no hay cambios respecto al último archivo → SKIP
    - Si hay cambios → genera un nuevo JSON
    """
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or not data:
        raise AirflowFailException("La API devolvió datos inválidos")

    current_hash = calculate_hash(data)

    # Comprobar último archivo existente
    existing_files = sorted(Path(RAW_DIR).glob("futurama_characters_*.json"))
    if existing_files:
        latest_file = existing_files[-1]
        with open(latest_file, "r", encoding="utf-8") as f:
            old_data = json.load(f)
            old_hash = calculate_hash(old_data)

        if current_hash == old_hash:
            raise AirflowSkipException("No hay cambios en los datos de la API")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = f"{RAW_DIR}/futurama_characters_{ts}.json"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"JSON generado: {json_path}")

    return {
        "json_path": json_path,
        "records": len(data),
    }


@task
def validate_data(metadata: dict) -> dict:
    """
    Valida el archivo JSON:
    - Existe
    - Es JSON válido
    - Contiene los campos requeridos
    """
    json_path = metadata["json_path"]

    if not os.path.exists(json_path):
        raise AirflowFailException("El archivo JSON no existe")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for idx, item in enumerate(data):
        missing = [f for f in REQUIRED_FIELDS if f not in item]
        if missing:
            raise AirflowFailException(
                f"Registro {idx} sin campos requeridos: {missing}"
            )

    logger.info("Validación completada correctamente")
    return metadata


@task
def transform_to_csv(metadata: dict) -> str:
    """
    Transforma el JSON validado en un archivo CSV.
    """
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

    json_path = metadata["json_path"]
    csv_filename = os.path.basename(json_path).replace(".json", ".csv")
    csv_path = f"{PROCESSED_DIR}/{csv_filename}"

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        ## Está bien hecho pero hay una función que hace que el código se vea más limpio: writer.writerows(...)
        for row in data:
            clean_row = {}
            for field in CSV_FIELDS:
                value = row.get(field, "")
                clean_row[field] = (
                    json.dumps(value) if isinstance(value, (list, dict)) else value
                )
            writer.writerow(clean_row)

    logger.info(f"CSV generado: {csv_path}")
    return csv_path

# ============================================================================
# DAG
# ============================================================================

with DAG(
    dag_id="etl_futurama_api_taskflow",
    description="ETL Futurama API con TaskFlow (sin borrado de archivos)",
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "api", "taskflow"],
) as dag:

    extracted = extract_data()
    validated = validate_data(extracted)
    transform_to_csv(validated)
