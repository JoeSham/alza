import datetime
import logging
import time

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# note: some imports are inside tasks to minimize top-level code which has to be run by airflow scheduler
# to consider for later: change jobs and queries from full-table calc to incremental

task_logger = logging.getLogger("airflow.task")
ALZA_PROJECT_ID = "alza-415512"
ALZA_TMP_DATASET = "alza_tmp"
ALZA_RAW_DATASET = "alza_raw"
ALZA_DATASET = "alza"

default_args = {
    "owner": "joe",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


@dag(
    catchup=False,
    default_args=default_args,
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2024, 3, 1, 2),
)
def alza_case_study_1():
    @task
    def get_bikes_data():
        import decimal
        import pandas as pd
        import pandas_gbq

        bikes_data_sas_url = ("https://stalzadfcase.blob.core.windows.net/sales-data/bikes_data.alza?"
                              "sp=r&st=2024-02-09T08:03:24Z&se=2024-03-01T22:59:24Z&spr=https&"
                              "sv=2022-11-02&sr=c&sig=zyFMI4nma4jpQFLczRIKrPsAv%2FxHVXaZWkw3QdADnK0%3D")
        task_logger.info("Skipping download from Azure as SAS token is expired")

        def download_data_from_azure(sas_url):
            import io
            from azure.storage.blob import BlobClient

            blob_client = BlobClient.from_blob_url(sas_url)
            blob_file = io.BytesIO()
            download_stream = blob_client.download_blob()
            blob_file.write(download_stream.readall())
            blob_file.seek(0)
            return blob_file

        # bikes_data_file_obj = download_data_from_azure(bikes_data_sas_url)

        # note: manually fixed couple rows (5551, ...), where decimal sep. was "," instead of "."
        # also, a column separator can be not just ",", but also ";" or "|"
        df = pd.read_csv("dags/alza_case_study_1/seeds/bikes_data.alza", sep=",|;|\|", header=0, decimal=".")
        df["price_eur"] = df["price_eur"].astype(str)
        df["price_eur"] = df["price_eur"].apply(lambda price_eur: decimal.Decimal(price_eur))
        # bike_id is not unique, i.e. bike_id 21514: 3x for FirmA, 1x for FirmB
        # would need further context of the seeds to decide how to handle it properly
        # for now I made a tmp decision to keep only the last occurrence, and drop the rest
        df.drop_duplicates(subset=["bike_id"], keep="last", inplace=True, ignore_index=True)
        task_logger.info(df.head(20))
        task_logger.info(f"num of lines: {len(df)}")
        tmp_table_id = f"{ALZA_TMP_DATASET}.tmp_bikes"
        pandas_gbq.to_gbq(df, tmp_table_id, project_id=ALZA_PROJECT_ID, if_exists="replace",
                          table_schema=[{"name": "price_eur", "type": "BIGNUMERIC"}])

    upsert_bikes_data_task = BigQueryInsertJobOperator(
        task_id="upsert_bikes_data",
        configuration={
            "query": {
                "query": "{% include 'sql/upsert_bikes_data.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    @task
    def get_cnb_rates():
        import decimal
        import pandas as pd
        import pandas_gbq

        def daterange(date1, date2):
            for day_num in range(int((date2 - date1).days) + 1):
                yield date1 + datetime.timedelta(day_num)

        start_dt = datetime.date(2022, 12, 20)
        end_dt = datetime.date(2023, 2, 3)
        dates = list(daterange(start_dt, end_dt))
        tmp_table_id = f"{ALZA_TMP_DATASET}.tmp_cnb_rates"

        df_all = pd.DataFrame()
        for date in dates:
            date_str = date.strftime("%d.%m.%Y")
            url = (f"https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/"
                   f"kurzy-devizoveho-trhu/denni_kurz.txt?date={date_str}")
            df = pd.read_csv(url, sep="|", header=0, skiprows=1, decimal=",",
                             names=["country", "currency", "amount_czk", "currency_code", "rate_for_amount"])
            df["date"] = pd.to_datetime(date.strftime("%Y-%m-%d"))
            df["rate"] = df["rate_for_amount"] / df["amount_czk"]
            decimal_context = decimal.Context(prec=10)
            df["rate"] = df["rate"].apply(decimal_context.create_decimal_from_float)
            df.drop(columns=["rate_for_amount", "country", "amount_czk", "currency"], inplace=True)
            task_logger.info(df.head(50))
            df_all = pd.concat([df_all, df], ignore_index=True)
            time.sleep(0.2)  # to be nice to the cnb endpoint
        pandas_gbq.to_gbq(df_all, tmp_table_id, project_id=ALZA_PROJECT_ID, if_exists="replace",
                          table_schema=[{"name": "date", "type": "DATE"}, {"name": "rate", "type": "BIGNUMERIC"}])

    upsert_cnb_rates_task = BigQueryInsertJobOperator(
        task_id="upsert_cnb_rates",
        configuration={
            "query": {
                "query": "{% include 'sql/upsert_cnb_rates.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    @task
    def get_weather_data():
        import decimal
        import pandas as pd
        import pandas_gbq

        url = ("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/"
               "timeline/Austin/2022-12-20/2023-02-03?unitGroup=metric&include=days"
               "&key=NE89ZLSKWSC3V8EWMHAJWNMV8&contentType=csv")
        df = pd.read_csv(url, sep=",", quotechar='"', header=0, decimal=".",
                         usecols=["name", "datetime", "temp", "conditions", "description"])
        df.rename(columns={"name": "city", "datetime": "date", "temp": "temperature"}, inplace=True)
        df["temperature"] = df["temperature"].astype(str)
        df["temperature"] = df["temperature"].apply(lambda temp: decimal.Decimal(temp))
        df["date"] = pd.to_datetime(df["date"])
        task_logger.info(df.head(10))
        task_logger.info(f"rows: {len(df)}")
        tmp_table_id = f"{ALZA_TMP_DATASET}.tmp_weather"
        pandas_gbq.to_gbq(df, tmp_table_id, project_id=ALZA_PROJECT_ID, if_exists="replace",
                          table_schema=[{"name": "temperature", "type": "NUMERIC"}, {"name": "date", "type": "DATE"}])

    upsert_weather_data_task = BigQueryInsertJobOperator(
        task_id="upsert_weather_data",
        configuration={
            "query": {
                "query": "{% include 'sql/upsert_weather_data.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    @task
    def get_austin_bikeshare_data():
        import decimal
        import pandas as pd
        import pandas_gbq

        with open("dags/alza_case_study_1/sql/get_austin_bikeshare_data.sql", "r") as f:
            query = f.read()
        df = pd.read_gbq(query, dialect="standard")
        task_logger.info(df.head(10))
        task_logger.info(f"rows: {len(df)}")
        tmp_table_id = f"{ALZA_TMP_DATASET}.tmp_austin_bikeshare"
        df["date"] = pd.to_datetime(df["date"])
        pandas_gbq.to_gbq(df, tmp_table_id, project_id=ALZA_PROJECT_ID, if_exists="replace",
                          table_schema=[{"name": "date", "type": "DATE"}])

    upsert_austin_bikeshare_data_task = BigQueryInsertJobOperator(
        task_id="upsert_austin_bikeshare_data",
        configuration={
            "query": {
                "query": "{% include 'sql/upsert_austin_bikeshare_data.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    calculate_enriched_bikeshare_data_task = BigQueryInsertJobOperator(
        task_id="calculate_enriched_bikeshare_data",
        configuration={
            "query": {
                "query": "{% include 'sql/calculate_enriched_bikeshare_data.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    bikes_data_task = get_bikes_data()
    get_cnb_rates_task = get_cnb_rates()
    get_weather_data_task = get_weather_data()
    get_austin_bikeshare_data_task = get_austin_bikeshare_data()

    bikes_data_task >> upsert_bikes_data_task >> get_cnb_rates_task >> upsert_cnb_rates_task
    upsert_bikes_data_task >> get_weather_data_task >> upsert_weather_data_task
    upsert_cnb_rates_task >> get_austin_bikeshare_data_task
    upsert_weather_data_task >> get_austin_bikeshare_data_task
    get_austin_bikeshare_data_task >> upsert_austin_bikeshare_data_task
    upsert_austin_bikeshare_data_task >> calculate_enriched_bikeshare_data_task

alza_case_study_1()
