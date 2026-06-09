import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

VILLES = {
    "Paris": {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon": {"latitude": 45.7640, "longitude": 4.8357},
    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
}

OUTPUT_DIR = "/opt/airflow/dags/meteo_data"

DEFAULT_ARGS = {
    "owner": "oukhemanou",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

CHAMPS_METIER = ["temperature_2m", "wind_speed_10m", "precipitation", "time"]

def fetch_meteo(ville: str, latitude: float, longitude: float, **context):
    hook = HttpHook(method="GET", http_conn_id="open_meteo_api")
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,wind_speed_10m,precipitation",
        "timezone": "Europe/Paris",
    }
    
    response = hook.run(
        endpoint="/v1/forecast",
        data=params,
    )
    
    raw_data = response.json()
    context["ti"].xcom_push(key=f"raw_{ville}", value=raw_data)


def transform_data(ville: str, **context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(key=f"raw_{ville}", task_ids=f"fetch_meteo_{ville}")

    if not raw_data:
        raise ValueError(f"[transform] Aucune donnée brute disponible pour {ville}.")

    current = raw_data.get("current")
    if not current:
        raise ValueError(f"[transform] Clé 'current' absente dans la réponse pour {ville}.")

    champs_manquants = [c for c in CHAMPS_METIER if c not in current]
    if champs_manquants:
        raise ValueError(
            f"[transform] Champs manquants pour {ville} : {champs_manquants}"
        )

    transformed = {
        "ville": ville,
        "heure": current["time"],
        "temperature_c": current["temperature_2m"],
        "vent_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
        "date_execution": context["ds"],
    }

    logging.info(f"[transform] Données transformées pour {ville} : {transformed}")
    ti.xcom_push(key=f"transformed_{ville}", value=transformed)


def load_data(ville: str, **context):
    ti = context["ti"]
    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )

    if not transformed:
        raise ValueError(f"[load] Aucune donnée transformée à charger pour {ville}.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{ville}_{context['ds']}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)

    logging.info(f"[load] {ville} → {filepath}")


def alert_on_failure(**context):
    logging.error("[alert] ÉCHEC détecté dans le pipeline d'ingestion météo.")
    logging.error(f"[alert] Date d'exécution : {context['ds']}")


def log_execution(**context):
    logging.info("=" * 60)
    logging.info(f"[log] openmeteo_ingestion — bilan d'exécution")
    logging.info(f"[log] Date : {context['ds']} | Villes : {list(VILLES.keys())}")
    logging.info("=" * 60)


with DAG(
    dag_id="openmeteo_ingestion",
    description="Ingestion Open-Meteo — fetch / transform / load par ville",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["open-meteo", "tp3"],
) as dag:

    toutes_taches = []

    for ville, coords in VILLES.items():

        fetch = PythonOperator(
            task_id=f"fetch_meteo_{ville}",
            python_callable=fetch_meteo,
            op_kwargs={
                "ville": ville,
                "latitude": coords["latitude"],
                "longitude": coords["longitude"],
            },
        )

        transform = PythonOperator(
            task_id=f"transform_data_{ville}",
            python_callable=transform_data,
            op_kwargs={"ville": ville},
        )

        load = PythonOperator(
            task_id=f"load_data_{ville}",
            python_callable=load_data,
            op_kwargs={"ville": ville},
        )

        fetch >> transform >> load

        toutes_taches += [fetch, transform, load]

    alerte = PythonOperator(
        task_id="alert_on_failure",
        python_callable=alert_on_failure,
        trigger_rule="one_failed",
    )

    log = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
        trigger_rule="all_done",
    )

    toutes_taches >> alerte
    toutes_taches >> log