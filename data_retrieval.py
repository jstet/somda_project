from somda_project.pipelines import get_upload_parquet, get_timeseries_day, concat_csvs, get_election_data
from somda_project.helpers import gen_urls, get_env_vars
from somda_project.IO_handlers import create_minio_client, retrieve_file
from dotenv import load_dotenv
import modal
import os

load_dotenv()

image = modal.Image.debian_slim().poetry_install_from_file("pyproject.toml")

stub = modal.Stub(name="somda_project")


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def f1(url):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    get_upload_parquet(url, client, bucket_id)


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def f2(url):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    return get_timeseries_day(url, client, bucket_id)


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def f3(urls):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    return concat_csvs(urls, client, bucket_id)


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def f4():
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    return get_election_data(client, bucket_id)


@stub.function(image=image, timeout=1000)
def f5():
    return gen_urls()


@stub.local_entrypoint()
def main():
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    # urls = f5.call()
    # list(f1.map(urls))
    # list(f2.map(urls))
    # merged = f3.call(urls)
    # csv = retrieve_file(client, merged, bucket_id)
    # os.replace(f"./{csv}", f"./data/{csv}")
    election_data = f4.call()
    csv = retrieve_file(client, election_data, bucket_id)
    os.replace(f"./{csv}", f"./data/{csv}")
