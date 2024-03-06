# this is a helper script for the self-hosted Airflow solution, to automatically mount
# the GCS bucket/folder with the DAGs to the VM
# it has to be run manually when the VM is started

gcsfuse -o nonempty --implicit-dirs --only-dir dags airflow-alza ~/airflow-alza/dags

# to unmouunt, run:
# fusermount -u ~/airflow-alza/dags
