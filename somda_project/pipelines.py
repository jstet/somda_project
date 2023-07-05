from somda_project.helpers import download_file, bz2_to_parquet
from somda_project.s3_funcs import upload_file
import os


def get_upload_parquet(url, minio_client, bucket_id):
    bz2_path, id_ = download_file(url)
    output_filepath = bz2_to_parquet(bz2_path, id_)
    os.remove(bz2_path)
    s3_path = upload_file(minio_client, f"{id_}.parquet", output_filepath, bucket_id)
    os.remove(output_filepath)
    return s3_path
