from __future__ import annotations

from datetime import timedelta
from typing import List, Dict
from calendar import monthrange

import pendulum
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ====== CONFIG ======
GCP_PROJECT  = "enap-470914"     
BQ_DATASET   = "openfda"                
BQ_TABLE     = "food_event_reports"  
BQ_LOCATION  = "US"                       
GCP_CONN_ID  = "google_cloud_default"      # Airflow connection with a SA that can write to BQ
# ====================

DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Fabiana Nascimento,Open in Cloud IDE",
}

DESCRIPTION = (
    "Coletar eventos adversos e reclamações de alimentos, suplementos dietéticos e cosméticos. Salvar no BigQuery"
)

@dag(
    description=DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # daily at 00:00 UTC
    start_date=pendulum.datetime(2020, 6, 1, tz="UTC"),
    catchup=True,
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm42rbvn10lqk01nlco70l0b8/cm44gkosq0tof01mxajutk86g",
    },
    tags=["fda", "food", "dietary", "bigquery", "bigqueryhook"],
)
def openfda_food_monthly():

    @task(task_id="fetch_openfda")
    def fetch_openfda(**context) -> List[Dict]:
        """
        Buscar dados do mês anterior na OpenFDA Food API, montar DataFrame e enviar via XCom.
        """
        data_interval_start = context["data_interval_start"]

        # Primeiro e último dia do mês anterior
        prev_month = (data_interval_start - timedelta(days=1)).replace(day=1)
        inicio = prev_month.strftime("%Y%m%d")
        last_day = monthrange(prev_month.year, prev_month.month)[1]
        fim = prev_month.replace(day=last_day).strftime("%Y%m%d")

        base_url = "https://api.fda.gov/food/event.json"
        search_query = f"date_started:[{inicio}+TO+{fim}]"
        params = {"search": search_query, "count": "date_started"}

        resp = requests.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        results = payload.get("results", [])
        if not isinstance(results, list):
            results = []

        df = pd.DataFrame(results)
        if not df.empty:
            if "time" in df.columns and "date_started" not in df.columns:
                df = df.rename(columns={"time": "date_started"})
            df = df[["date_started", "count"]].copy()
            df["date_started"] = df["date_started"].astype(str)
            df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
        else:
            df = pd.DataFrame(columns=["date_started", "count"])

        return df.to_dict(orient="records")

    @task(task_id="save_to_bigquery")
    def save_to_bigquery(records: List[Dict]):
        """
        Ler o DataFrame do XCom e gravar no BigQuery (append).
        """
        df = pd.DataFrame.from_records(records, columns=["date_started", "count"])
        if df.empty:
            df = pd.DataFrame(columns=["date_started", "count"])

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        client = hook.get_client()

        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config={"write_disposition": "WRITE_APPEND"},
            location=BQ_LOCATION,
        )
        job.result()

    fetched = fetch_openfda()
    save_to_bigquery(fetched)


dag = openfda_food_monthly()
