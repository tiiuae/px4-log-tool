from srta_drone_dataset.util.logger import log
from srta_drone_dataset.processing_modules.metagen import get_file_metadata
import os
import json
import yaml


def generate_ulog_metadata(verbose, directory_address, filter):
    with open(filter, "r") as f:
        data = yaml.safe_load(f)

    metadata_fields = data["metadata_fields"]
    for dirpath, _, filenames in os.walk(directory_address):
        if len(filenames) > 0:
            mission_data = []
            for file in filenames:
                if file.split(".")[-1] == "ulg" or file.split(".")[-1] == "ulog":
                    mission_metadata = get_file_metadata(metadata_fields, dirpath, file)
                    mission_metadata["mission_name"] = file.split(".")[0]
                    mission_data.append(mission_metadata)
            mission_data.sort(key=lambda x: x["mission_name"])
            json_data = {
                "mission": mission_data,
                "total_duration": sum(item["duration"] for item in mission_data),
                "average_duration": sum(item["duration"] for item in mission_data)
                / len(mission_data)
                if mission_data
                else 0,
            }
            json_filepath = os.path.join(dirpath, "metadata.json")
            with open(json_filepath, "w") as f:
                json.dump(json_data, f, indent=4)
    return
