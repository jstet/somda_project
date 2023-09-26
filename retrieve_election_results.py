from somda_project.pipelines import get_election_data
from somda_project.helpers import get_env_vars, json_serial
from somda_project.IO_handlers import create_minio_client, retrieve_file, upload_file
import os
import json
from dotenv import load_dotenv

load_dotenv()

endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)

client = create_minio_client(endpoint, access_key, secret_key, region)
election_data = get_election_data()
json_path = "eu_elections.json"
with open(json_path, "w") as outfile:
    json.dump(election_data, outfile, default=json_serial)

os.remove(json_path)
s3_path = upload_file(client, json_path, json_path, bucket_id)
csv = retrieve_file(client, election_data, bucket_id)
os.replace(f"./{csv}", f"./data/{csv}")
