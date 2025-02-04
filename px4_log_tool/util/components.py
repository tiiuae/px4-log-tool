#!/usr/bin python3
import os
from copy import deepcopy
from multiprocessing import Process
from typing import Any, Dict
from px4_log_tool.util.logger import log
from px4_log_tool.util.tui import progress_bar
from px4_log_tool.processing_modules.converter import convert_csv2ros2bag, convert_ulog2csv
from px4_log_tool.processing_modules.merger import merge_csv
from px4_log_tool.processing_modules.resampler import resample_data

import pandas as pd
import yaml

def resample_unified(
    unified_df: pd.DataFrame,
    msg_reference: pd.DataFrame,
    resample_params: Dict[str, Any],
    in_place: bool = False,
    verbose: bool = False,
) -> pd.DataFrame:
    """Resamples a unified dataframe based on the message reference and
    resample parameters.

    This function iterates through each mission in the unified dataframe,
    identifies numerical and categorical labels based on a provided message
    reference, and then applies the `resample_data` function to resample the
    data for each mission. The resampled dataframes are then concatenated into
    a single dataframe and returned.

    Args:
    - unified_df (pd.DataFrame): The unified dataframe to be resampled.
    - msg_reference (pd.DataFrame): A dataframe containing message references (Alias, Dataclass).
    - resample_params (dict): A dictionary containing resampling parameters:
    - verbose (bool): Verbose output.

    Returns:
    - pd.DataFrame: The resampled dataframe.
    """
    reference = deepcopy(msg_reference)
    num_labels = []
    cat_labels = []
    mission_names = sorted(unified_df["mission_name"].unique())

    resampled_df = pd.DataFrame()

    i = 0
    log("Resampling Progress:", verbosity=verbose, log_level=0,bold=True)
    progress_bar(i / (len(mission_names) + 1), verbose=verbose)

    for mission in mission_names:
        merged_df = unified_df[unified_df["mission_name"] == mission]
        for label in merged_df.columns:
            if label == "timestamp" or label == "mission_name":
                continue
            msg, param = label.split("_", maxsplit=1)
            if msg[-1].isdigit():
                msg = msg[:-1]
            label_dc = reference[reference["Alias"] == f"{msg}_{param}"]
            label_dc = label_dc.reset_index(drop=True)
            dc = label_dc["Dataclass"]
            if dc.size < 1:
                continue
            if dc.iloc[0] == "Numerical":
                num_labels.append(label)
            else:
                cat_labels.append(label)

        merged_df = resample_data(
            merged_df[merged_df.columns[1:]].copy(),
            resample_params["target_frequency_hz"],
            resample_params["num_method"],
            resample_params["cat_method"],
            resample_params["interpolate_numerical"],
            resample_params["interpolate_method"],
            num_labels,
            cat_labels,
            verbose=False,
        )
        merged_df["mission_name"] = mission
        merged_df = merged_df[["mission_name"] + list(merged_df.columns)[:-1]]
        num_labels = []
        cat_labels = []
        resampled_df = pd.concat([resampled_df, merged_df])

        i += 1
        progress_bar(i / (len(mission_names) + 1), verbose=verbose)

    if in_place:
        resampled_df.to_csv("unified.csv", index=False)
    i += 1
    progress_bar(i / (len(mission_names) + 1), verbose=verbose)
    return resampled_df

def get_msg_reference(verbose: bool = False):
    import pkg_resources
    try:
        resource_package = "px4_log_tool"
        resource_path = 'msg_reference.csv'
        msg_reference = pd.read_csv(pkg_resources.resource_filename(resource_package, resource_path))
        return msg_reference
    except FileNotFoundError:
        log("Error: msg_reference.csv not found.", verbosity=verbose, log_level=2)
        log("Case 1 -- Ensure that the msg_reference.csv file is present in the directory this script is being run from.", verbosity=verbose, log_level=2)
        log("Case 2 -- Please restore this repository or download this file from the source.", verbosity=verbose, log_level=2)
        log("Case 3 -- If resampling is not desired, please remove the resample flag from the command line.", verbosity=verbose, log_level=2)
        return None


def extract_filter(filter: str | None, verbose: bool = False):
    """Extracts filter parameters from a YAML file.

    Args:
    - filter (str): Path to the YAML filter file.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
    - None
    """
    FILTER = dict()

    if filter is not None:
        with open(filter, "r") as f:
            FILTER = yaml.safe_load(f)
    else:
        FILTER = {}

    try:
        _ = FILTER["whitelist_messages"]
    except KeyError:
        log("Warning: Missing whitelist_messages in filter.yaml.", verbosity=verbose, log_level=1)
        log("Using default values.", verbosity=verbose, log_level=1)
        log("", verbosity=verbose, log_level=1)
        FILTER["whitelist_messages"] = ["sensor_combined", "actuator_outputs"]
    log("Whitelisted topics are:", verbosity=verbose, log_level=0, bold=True)
    for entry in FILTER["whitelist_messages"]:
        log(f"-- {entry}", verbosity=verbose, log_level=0)

    try:
        _ = FILTER["blacklist_messages"]
    except KeyError:
        log("Warning: Missing blacklist_headers in filter.yaml.", verbosity=verbose, log_level=1)
        log("Using default values.", verbosity=verbose, log_level=1)
        log("", verbosity=verbose, log_level=1)
        FILTER["blacklist_headers"] =[ "timestamp_sample", "device_id", "error_count"]
    log("Blacklisted headers are:", verbosity=verbose, log_level=0,bold=True)
    for entry in FILTER["blacklist_headers"]:
        log(f"-- {entry}", verbosity=verbose, log_level=0)

    try:
        _ = FILTER["resample_params"]["target_frequency_hz"]
        _ = FILTER["resample_params"]["num_method"]
        _ = FILTER["resample_params"]["cat_method"]
        _ = FILTER["resample_params"]["interpolate_numerical"]
        _ = FILTER["resample_params"]["interpolate_method"]
    except KeyError:
        log("Warning: Incomplete resampling parameters provided in filter.yaml.", verbosity=verbose, log_level=1)
        log("Using default values.", verbosity=verbose, log_level=1)
        log("", verbosity=verbose, log_level=1)
        FILTER["resample_params"] = {
            "target_frequency_hz": 10,
            "num_method": "mean",
            "cat_method": "ffill",
            "interpolate_numerical": True,
            "interpolate_method": "linear",
        }

    log("Resampling parameters:", verbosity=verbose, log_level=0)
    log(f"-- target_frequency_hz: {FILTER['resample_params']['target_frequency_hz']}", verbosity=verbose, log_level=0)
    log(f"-- num_method: {FILTER['resample_params']['num_method']}", verbosity=verbose, log_level=0)
    log(f"-- cat_method: {FILTER['resample_params']['cat_method']}", verbosity=verbose, log_level=0)
    log(f"-- target_frequency_hz: {FILTER['resample_params']['interpolate_numerical']}", verbosity=verbose, log_level=0)
    log(f"-- interpolate_method: {FILTER['resample_params']['interpolate_method']}", verbosity=verbose, log_level=0)
    return FILTER


def get_ulog_files(ulog_dir: str, verbose: bool = False) -> list[str]:
    """
    Retrieves a list of `.ulog` files from the specified directory.

    Args:
    - ulog_dir (str): Path to the directory containing `.ulog` files.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
    - list[str]: A list of tuples, where each tuple contains the file path and filename.
    """
    ulog_files = []
    for root, _, files in os.walk(ulog_dir):
        for file in files:
            if file.split(".")[-1] == "ulg" or file.split(".")[-1] == "ulog":
                ulog_files.append((root, file))

    log(msg=f"Converting [{len(ulog_files)}] .ulog files to .csv.", verbosity=verbose, log_level=0)
    return ulog_files


def get_csv_dirs(csv_dir: str, verbose: bool = False) -> list[str]:
    csv_dirs = []
    for root, subdirs, files in os.walk(csv_dir):
        if not subdirs:
            if all(file.endswith(".csv") for file in files):
                csv_dirs.append(root)
    log(msg=f"Converting [{len(csv_dirs)}] .csv directories into .cb3 ROS 2 bags.", verbosity=verbose, log_level=0)
    return csv_dirs


def convert_dir_ulog_csv(ulog_files: list[str], output_dir: str, filter: dict, verbose: bool = False):
    """
    Converts a list of `.ulog` files to `.csv` files in parallel.

    Args:
    - ulog_files (list[str]): A list of tuples, where each tuple contains the file path and filename.
    - output_dir (str): The output directory for the converted `.csv` files.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """

    processes = []
    for file in ulog_files:
        process = Process(
            target=convert_ulog2csv,
            args=(
                file[0],
                file[1],
                filter["whitelist_messages"],
                os.path.join(output_dir, file[0]),
                filter["blacklist_headers"],
                ",",
                None,
                None,
                False,
                verbose
            ),
        )
        processes.append(process)
        process.start()

    i = 0
    total = len(processes)
    log("Conversion Progress:", verbosity=verbose, log_level=0,bold=True)
    for process in processes:
        process.join()
        i += 1
        progress_bar(i / total, verbose)
    log("", verbosity=verbose, log_level=0, color=False,timestamped=False)
    return


def convert_dir_csv_db3(csv_dirs: list[str], output_dir: str, verbose: bool = False):

    processes = []
    for dir in csv_dirs:
        process = Process(
            target=convert_csv2ros2bag,
            args=(
                dir,
                os.path.join(output_dir, dir),
                "/fmu/out",
                False,
                verbose
            )
        )
        processes.append(process)
        process.start()

    i = 0
    total = len(processes)
    log("Conversion Progress:", verbosity=verbose, log_level=0,bold=True)
    for process in processes:
        process.join()
        i += 1
        progress_bar(i / total, verbose)
    log("", verbosity=verbose, log_level=0, color=False,timestamped=False)
    return


def merge_csvs(output_dir: str, verbose: bool = False) -> pd.DataFrame:
    """
    Merges multiple `.csv` files into a single unified `.csv` file, while
    leaving breadcrumb `merged.csv` files in the output directory tree.

    Args:
    - output_dir (str): The directory containing the `.csv` files.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
    - pd.DataFrame: The unified DataFrame.
    """
    csv_files = []
    for root, _, files in os.walk(output_dir):
        if len(files) > 0:
            csv_files.append((root, files))

    log(f"Merging into [{len(csv_files)}] .csv files.", verbosity=verbose, log_level=0)

    processes = []

    for file in csv_files:
        process = Process(
            target=merge_csv,
            args=(file[0], file[1]),
        )
        processes.append(process)
        process.start()

    i = 0
    total = len(processes)
    log("Merging Progress:", verbosity=verbose, log_level=0, bold=True)
    for process in processes:
        process.join()
        i += 1
        progress_bar(i / total, verbose)
    log("", verbosity=verbose, log_level=0, color=False, timestamped=False)

    merge_files = []
    for root, _, files in os.walk(output_dir):
        if "merged.csv" in files:
            merge_files.append(root)

    log("Unifying all 'merged.csv' files into a single 'unified.csv' -- This may take a while.", verbosity=verbose, log_level=0)

    unified_df = pd.concat(
        [pd.read_csv(os.path.join(file, "merged.csv")) for file in merge_files]
    )
    unified_df.to_csv("unified.csv", index=False)
    return unified_df


