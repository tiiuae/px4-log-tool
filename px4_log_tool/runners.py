#!/usr/bin python3
import os
import json
from px4_log_tool.processing_modules.converter import convert_ros2bag2csv
from px4_log_tool.processing_modules.metagen import get_file_metadata
from px4_log_tool.util.logger import log
from px4_log_tool.util.components import (
    convert_dir_csv_db3,
    get_csv_dirs,
    get_msg_reference,
    extract_filter,
    get_ulog_files,
    convert_dir_ulog_csv,
    merge_csvs,
    resample_unified,
    adjust_topics,
)
import shutil


FILTER = dict()


def ulog_csv(
    verbose: bool,
    ulog_dir: str,
    filter: str,
    output_dir: str | None,
    merge: bool = False,
    clean: bool = False,
    resample: bool = False,
):
    global FILTER

    FILTER = extract_filter(filter_str=filter, verbose=verbose)

    ulog_files: list[tuple[str,str]] = get_ulog_files(ulog_dir=ulog_dir, verbose=verbose)

    if output_dir is None:
        output_dir = "./output_dir"
    convert_dir_ulog_csv(
        ulog_files=ulog_files, output_dir=output_dir, filter=FILTER, verbose=verbose
    )

    if merge:
        unified_df = merge_csvs(output_dir=output_dir, verbose=verbose)
        msg_reference = get_msg_reference(verbose=verbose)
        if resample and msg_reference is not None:
            _ = resample_unified(
                unified_df=unified_df,
                msg_reference=msg_reference,
                resample_params=FILTER["resample_params"],
                in_place=True,
                verbose=verbose,
            )

    if resample and not merge:
        log("Cannot resample without merging!", log_level=2, verbosity=verbose)

    if clean:
        log("Cleaning directory and breadcrumbs.", verbosity=verbose, log_level=0)
        shutil.rmtree(output_dir)
    return


def csv_db3(
    verbose: bool,
    directory_address: str,
    filter: str,
    output_dir: str | None,
):
    global FILTER

    FILTER = extract_filter(filter_str=filter, verbose=verbose)
    csv_dirs = get_csv_dirs(csv_dir=directory_address, verbose=verbose)

    if output_dir is None:
        log(".db3 ROS 2 Bags will be created in-place.", log_level=1, verbosity=verbose)
        output_dir = ""

    convert_dir_csv_db3(
        csv_dirs=csv_dirs,
        output_dir=output_dir,
        topic_prefix=FILTER["bag_params"]["topic_prefix"],
        capitalise_topics=FILTER["bag_params"]["capitalise_topics"],
        verbose=verbose,
    )
    return


def ulog_db3(
    verbose: bool,
    directory_address: str,
    filter: str,
    output_dir: str | None,
):
    global FILTER

    if output_dir is None:
        output_dir = "./output_dir"
    ulog_csv(verbose=verbose, ulog_dir=directory_address, filter=filter, output_dir=output_dir)

    log("ROS 2 Bag topics will be adjusted.", log_level=0, verbosity=verbose)
    adjust_topics(verbose=verbose, directory_address=output_dir, filter=FILTER)

    csv_db3(verbose=verbose, directory_address=output_dir, filter=filter, output_dir=f"{output_dir}_bags")
    return


def db3_csv(
    verbose: bool,
    directory_address: str,
    filter: str,
):
    global FILTER

    FILTER = extract_filter(filter_str=filter, verbose=verbose)

    convert_ros2bag2csv(bag_file_address=directory_address, verbose=verbose)
    return

def generate_ulog_metadata(verbose: bool, directory_address: str, filter: str):
    global FILTER

    FILTER = extract_filter(filter_str=filter, verbose=verbose)

    metadata_fields = FILTER["metadata_fields"]
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
