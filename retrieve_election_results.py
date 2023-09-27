from somda_project.helpers import json_serial, get_election_data
import json

election_data = get_election_data()
json_path = "./data/eu_elections.json"
with open(json_path, "w") as outfile:
    json.dump(election_data, outfile, default=json_serial)
