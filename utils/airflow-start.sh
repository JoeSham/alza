#!/bin/bash
export AIRFLOW_HOME=/home/joeshamcz/airflow-alza
cd /home/joeshamcz/airflow-alza

# 2024-03-06 update: dags folder is now mounted directly from gcs using gcsfuse
# rm -rf /home/joeshamcz/airflow-alza/_dags-backup
# mv /home/joeshamcz/airflow-alza/dags /home/joeshamcz/airflow-alza/_dags-backup
# gsutil cp -r gs://airflow-alza/dags /home/joeshamcz/airflow-alza/

conda activate /home/joeshamcz/miniconda3/envs/airflow-alza
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &
