import argparse
import subprocess


def upload_dags_to_gcs(dags_dir="dags/", bucket="gs://airflow-alza/dags"):
    subprocess.run(["gsutil", "cp", "-m", "-r", dags_dir, bucket])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dags_directory",
        help="Relative path to the source directory containing your DAGs",
    )
    parser.add_argument(
        "--dags_bucket",
        help="Name of the DAGs bucket without the gs:// prefix",
    )

    args = parser.parse_args()
    upload_dags_to_gcs(args.dags_directory, args.dags_bucket)
