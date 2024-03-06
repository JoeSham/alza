from __future__ import annotations

import argparse
import glob
import os

from google.cloud import storage


def upload_dags_to_gcs(
    dags_directory: str, bucket_name: str, name_replacement: str = "dags/"
) -> None:
    """
    Given a directory, this function moves all DAG files from that directory
    to a temporary directory, then uploads all contents of the temporary directory
    to a given cloud storage bucket
    Args:
        dags_directory (str): a fully qualified path to a directory that contains a "dags/" subdirectory
        bucket_name (str): the GCS bucket to upload DAGs to
        name_replacement (str, optional): the name of the "dags/" subdirectory that will be used when constructing the temporary directory path name Defaults to "dags/".
    """
    dags = glob.glob(f"{dags_directory}/*")

    if len(dags) > 0:
        # Note - the GCS client library does not currently support batch requests on uploads
        # if you have a large number of files, consider using
        # the Python subprocess module to run gsutil -m cp -r on your dags
        # See https://cloud.google.com/storage/docs/gsutil/commands/cp for more info
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for dag in dags:
            try:
                blob = bucket.blob(dag)
                blob.upload_from_filename(dag)
                print(f"File {dag} uploaded to {bucket_name}/{dag}.")
            except FileNotFoundError:
                current_directory = os.listdir()
                print(
                    f"{name_replacement} directory not found in {current_directory},"
                    f" you may need to override the default value of name_replacement to point to a relative directory"
                )
                raise

    else:
        print("No DAGs to upload.")


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
        help="Name of the DAGs bucket of your gcs environment without the gs:// prefix",
    )

    args = parser.parse_args()

    upload_dags_to_gcs(args.dags_directory, args.dags_bucket)
