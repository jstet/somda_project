from somda_project.pipelines import get_upload_parquet
from somda_project.helpers import gen_urls
from somda_project.s3_funcs import create_minio_client
import modal
import os

image = modal.Image.debian_slim().poetry_install_from_file("pyproject.toml")

stub = modal.Stub(name="somda_project")


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def f1(url):
    endpoint = os.environ.get("BUCKET_ENDPOINT")
    bucket_id = os.environ.get("BUCKET_ID")
    access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
    secret_key = os.environ.get("BUCKET_SECRET_KEY")
    region = os.environ.get("BUCKET_REGION")
    client = create_minio_client(endpoint, access_key, secret_key, region)
    get_upload_parquet(url, client, bucket_id)


@stub.function(image=image, timeout=1000)
def f2():
    return gen_urls()


@stub.local_entrypoint()
def main():
    for path in f1.map(f2.call()):
        print(path)
