import yaml

with open("config/project_config.yaml", "r") as file:
    config = yaml.safe_load(file)

PROJECT_ID = config["project_id"]
REGION = config["region"]
BUCKET_NAME = config["bucket_raw"]
DATASET_NAME = config["dataset_warehouse"]
