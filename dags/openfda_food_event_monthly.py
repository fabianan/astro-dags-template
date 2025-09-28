from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import requests
import pendulum

# Configurações
START_YEAR = 2020
END_YEAR = 2021
BQ_PROJECT_ID = "enap-470914"
BQ_DATASET = "openfda"
BQ_TABLE = "food_event_adverse_reports"
BASE_URL = "https://api.fda.gov/food/event.json"

@dag(
    schedule_interval="0 0 1 * *",  # Executa no primeiro dia de cada mês
    start_date=pendulum.datetime(2020, 6, 1, tz="UTC")  # Escolha a data desejada),
    catchup=True,
    tags=["openfda", "food", "bigquery", "etl"],
)
def openfda_food_event_monthly():
    @task()
    def extract_events(execution_date=None):
        # Data de execução (sempre primeiro dia do mês seguinte ao coletado)
        if execution_date is None:
            dt = pendulum.now("UTC")
        else:
            dt = pendulum.parse(execution_date)
        # Identifica o mês anterior
        ref_month = dt.subtract(months=1)
        year = ref_month.year
        month = ref_month.month

        # Garante que só busca entre 2020-2025
        if year < START_YEAR or year > END_YEAR:
            return None

        # Descobre quantos dias tem o mês anterior
        days_in_month = ref_month.days_in_month

        # Monta intervalo de datas para a busca
        start_str = f"{year}{month:02d}01"
        end_str = f"{year}{month:02d}{days_in_month:02d}"
        api_url = f"{BASE_URL}?search=date_started:[{start_str}+TO+{end_str}]&limit=100"

        # Faz a requisição
        resp = requests.get(api_url)
        data = resp.json()

        # Converte para DataFrame (ajuste conforme estrutura do resultado)
        if "results" in data:
            df = pd.DataFrame(data["results"])
        else:
            df = pd.DataFrame([])

        return df.to_dict("records")  # Salva como lista de dicts (XCom friendly)

    @task()
    def load_to_bigquery(records):
        if not records:
            return
        df = pd.DataFrame(records)
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        bq_hook.insert_rows_dataframe(
            project_id=BQ_PROJECT_ID,
            dataset_id=BQ_DATASET,
            table_id=BQ_TABLE,
            dataframe=df,
            reauth=False,
            allow_jagged_rows=True,
            allow_quoted_newlines=True,
        )

    events = extract_events()
    load_to_bigquery(events)

dag = openfda_food_event_monthly()
