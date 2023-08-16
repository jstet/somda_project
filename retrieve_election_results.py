from somda_project.pipelines import get_election_data
from somda_project.helpers import get_env_vars
from somda_project.IO_handlers import create_minio_client, retrieve_file
import os
from dotenv import load_dotenv

load_dotenv()

endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
print(endpoint)
client = create_minio_client(endpoint, access_key, secret_key, region)
election_data = get_election_data(client, bucket_id)
csv = retrieve_file(client, election_data, bucket_id)
os.replace(f"./{csv}", f"./data/{csv}")
