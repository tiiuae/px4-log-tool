#!/usr/bin python3
"""Converts ULog files to CSV, with options for filtering, merging, and creating a unified CSV.

This script provides a streamlined way to process PX4 ULog files. It offers flexibility in converting individual files or merging multiple files, filtering specific messages, and resampling data.

**Arguments:**

  -ulog_dir (str): Path to the directory containing ULog files.
  -filter (str): Path to a YAML filter file specifying message whitelist and header blacklist.
  -o, --output_dir (str): Output directory for converted CSV files (default: 'output_dir').
  -m, --merge: Merge CSV files within each subdirectory into 'merged.csv' files.
  -r, --resample: Resample 'merged.csv' files based on parameters in 'filter.yaml'.
  -b, --rosbag: Convert each mission into a ROS 2 bag (sqlite / .db).
  -c, --clean: Clean up intermediate files, leaving only 'unified.csv'.
  -v, --verbose: Enable verbose logging.

**YAML Filter File:**
```yaml
whitelist_messages:
    - sensor_combined
    - vehicle_attitude
    - ... (other message names)
blacklist_headers:
    - timestamp
    - ... (other field names)
resample_params:
    target_frequency_hz: 10
    num_method: mean
    cat_method: ffill
    interpolate_numerical: False
    interpolate_method: linear
```

**Workflow**

1. **File Conversion:** Converts ULog files in the specified directory to individual CSV files and .db3 ROS 2 Bag files, applying filters from the YAML file.
2. **File Merging (Optional):** If the `-m` flag is set, merges CSV files within each subdirectory into a single 'merged.csv'.
3. **File Unification (Optional):** Combines all 'merged.csv' files into a single 'unified.csv' file.
4. **Cleanup (Optional):** If the `-c` flag is set, removes intermediate files and directories, leaving only 'unified.csv'.
"""

import argparse
import os
import shutil
import sys
from copy import deepcopy
from multiprocessing import Process
from typing import Any, Dict

import pandas as pd
import yaml

from srta_drone_dataset.processing_modules.converter import (
    convert_csv2ros2bag,
    convert_ulog2csv,
)
from srta_drone_dataset.processing_modules.merger import merge_csv
from srta_drone_dataset.processing_modules.resampler import resample_data


def resample_unified(
    unified_df: pd.DataFrame,
    msg_reference: pd.DataFrame,
    resample_params: Dict[str, Any],
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Resamples a unified dataframe based on the message reference and resample parameters.

    This function iterates through each mission in the unified dataframe, identifies numerical and categorical labels
    based on a provided message reference, and then applies the `resample_data` function to resample the data for each mission.
    The resampled dataframes are then concatenated into a single dataframe and returned.

    Args:
        unified_df (pd.DataFrame): The unified dataframe to be resampled.
        msg_reference (pd.DataFrame): A dataframe containing message references (Alias, Dataclass).
        resample_params (dict): A dictionary containing resampling parameters:
            * target_frequency_hz (float): The target resampling frequency in Hz.
            * num_method (str): The resampling method for numerical data.
            * cat_method (str): The resampling method for categorical data.
            * interpolate_numerical (bool): Whether to interpolate numerical data.
            * interpolate_method (str): The interpolation method for numerical data.
        verbose (bool): Verbose output.

    Returns:
        pd.DataFrame: The resampled dataframe.
    """

    reference = deepcopy(msg_reference)
    num_labels = []
    cat_labels = []
    mission_names = sorted(unified_df["mission_name"].unique())

    resampled_df = pd.DataFrame()

    i = 0
    if verbose:
        print("Resampling Progress:")
        progress_bar(i / len(mission_names))

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

        # TODO Change from hardcoded verbose
        ### Currently it is way too verbose at the resampler
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
        if verbose:
            progress_bar(i / len(mission_names))

    return resampled_df


def progress_bar(progress: float) -> None:
    """
    Displays a simple progress bar in the console.

    Args:
        progress: A float between 0.0 and 1.0 representing the progress percentage.
    """

    bar_length = 50
    filled_length = int(bar_length * progress)
    bar = f"[{'=' * filled_length}{' ' * (bar_length - filled_length)}]"
    sys.stdout.write(f"\r{bar} {progress * 100:.1f}%")
    sys.stdout.flush()


def ulog_converter():
    parser = argparse.ArgumentParser(
        description="Convert ULOG files inside directory to CSV"
    )
    parser.add_argument(
        "-b",
        "--rosbag",
        action="count",
        help="Convert each mission into a ROS 2 bag (sqlite / .db)",
        default=0,
    )
    parser.add_argument(
        "-m",
        "--merge",
        action="count",
        help="Merge all files into one .csv (leaves breadcrumbs)",
        default=0,
    )
    parser.add_argument(
        "-r",
        "--resample",
        action="count",
        help="Resample data to a given frequency (filter.yaml is mandatory)",
        default=0,
    )
    parser.add_argument(
        "-c",
        "--clean",
        action="count",
        help="Cleans directory and breadcrumbs leaving only unified.csv",
        default=0,
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument(
        "directory",
        help="Path to the directory containing all ULOG files",
        default=None,
    )
    parser.add_argument(
        "filter",
        help="Filter .yaml file for [message whitelists, header blacklists]",
        default=None,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        help="This module creates a mirror directory tree of the one provided with the CSV files in corresponding locations",
        default="output_dir",
    )

    args = parser.parse_args()
    ulog_dir: str = args.directory
    output_dir: str = args.output_dir
    filter: str = args.filter
    verbose: bool = args.verbose
    resample: bool = args.resample
    rosbag: bool = args.rosbag
    merge: bool = args.merge
    clean: bool = args.clean

    if rosbag:
        print("")
        print("WARNING: Since ROS2 bags are created:")
        print("--> cleaning of output_dir is disabled")
        print("--> merging of .csv files is disabled")
        print("")
        try:
            import px4_msgs.msg  # noqa: F401
        except ImportError:
            print("ERROR: px4_msgs package not found.")
            return
        clean = False

    # = File Conversion =#

    ulog_files = []
    for root, _, files in os.walk(ulog_dir):
        for file in files:
            if file.split(".")[-1] == "ulg" or file.split(".")[-1] == "ulog":
                ulog_files.append((root, file))

    if verbose:
        print(f"Converting [{len(ulog_files)}] .ulog files to .csv.")
        print("")

    with open(filter, "r") as f:
        data = yaml.safe_load(f)

    if verbose:
        print("Whitelisted topics are:")
        for entry in data["whitelist_messages"]:
            print(f" - {entry}")
        print("")
        print("Blacklisted headers are:")
        for entry in data["blacklist_headers"]:
            print(f" - {entry}")

    processes = []
    for file in ulog_files:
        process = Process(
            target=convert_ulog2csv,
            args=(
                file[0],
                file[1],
                data["whitelist_messages"],
                os.path.join(output_dir, file[0]),
                data["blacklist_headers"],
            ),
        )
        processes.append(process)
        process.start()

    i = 0
    total = len(processes)
    if verbose:
        print("")
        print("Conversion Progress:")
    for process in processes:
        process.join()
        if verbose:
            i += 1
            progress_bar(i / total)

    # = to bags = #
    if rosbag:
        processes = []
        for file in ulog_files:
            process = Process(
                target=convert_csv2ros2bag,
                args=(
                    os.path.join(
                        output_dir, os.path.join(file[0], file[1].split(".")[0])
                    ),
                    "/fmu/out",
                    True,
                ),
            )
            processes.append(process)
            process.start()
        if verbose:
            print("")
            print("")
            print(84 * "-")
            print("Converting .csv files to ROS2 bags")
            print("")
            total = len(processes)
            i = 0
            progress_bar(i / total)
            print("")

        for process in processes:
            process.join()
            if verbose:
                i += 1
                progress_bar(i / total)

    # = File Merge =#
    if not merge or rosbag:
        return

    if verbose:
        print("")
        print("")
        print(84 * "-")
        print("Merging .csv files -- Breadcrumbs will be created as merged.csv.")
        print("")

    csv_files = []
    for root, _, files in os.walk(output_dir):
        if len(files) > 0:
            csv_files.append((root, files))

    if verbose:
        print(f"Merging into [{len(csv_files)}] .csv files.")
        print("")

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
    if verbose:
        print("Merging Progress:")
    for process in processes:
        process.join()
        if verbose:
            i += 1
            progress_bar(i / total)

    # = File Unification =#

    merge_files = []
    for root, _, files in os.walk(output_dir):
        if "merged.csv" in files:
            merge_files.append(root)

    if verbose:
        print("")
        print("")
        print(84 * "-")
        print(
            "Unifying all 'merged.csv' files into a single 'unified.csv' -- This may take a while."
        )

    unified_df = pd.concat(
        [pd.read_csv(os.path.join(file, "merged.csv")) for file in merge_files]
    )

    msg_reference = None
    if resample:
        print("")
        print("")
        print(84 * "-")
        print("Resampling `unified.csv`.")
        try:
            msg_reference = pd.read_csv(os.path.join(os.getcwd(), "msg_reference.csv"))
        except FileNotFoundError:
            print("Error: msg_reference.csv not found.")
            print(
                "Case 1 -- Ensure that the msg_reference.csv file is present in the directory this script is being run from."
            )
            print(
                "Case 2 -- Please restore this repository or download this file from the source."
            )
            print(
                "Case 3 -- If resampling is not desired, please remove the resample flag from the command line."
            )
            return

        try:
            _ = data["resample_params"]["target_frequency_hz"]
            _ = data["resample_params"]["num_method"]
            _ = data["resample_params"]["cat_method"]
            _ = data["resample_params"]["interpolate_numerical"]
            _ = data["resample_params"]["interpolate_method"]
        except KeyError:
            print("Warning: Incomplete resampling parameters provided in filter.yaml.")
            print("Using default values.")
            print("")
            data["resample_params"] = {
                "target_frequency_hz": 10,
                "num_method": "mean",
                "cat_method": "ffill",
                "interpolate_numerical": True,
                "interpolate_method": "linear",
            }

        if verbose:
            print("Resampling parameters:")
            print(
                f"-- target_frequency_hz: {data['resample_params']['target_frequency_hz']}"
            )
            print(f"-- num_method: {data['resample_params']['num_method']}")
            print(f"-- cat_method: {data['resample_params']['cat_method']}")
            print(
                f"-- target_frequency_hz: {data['resample_params']['interpolate_numerical']}"
            )
            print(
                f"-- interpolate_method: {data['resample_params']['interpolate_method']}"
            )
            print("")

        unified_df = resample_unified(
            unified_df, msg_reference, data["resample_params"], verbose
        )

    unified_df.to_csv("unified.csv", index=False)

    if clean:
        if verbose:
            print("")
            print(84 * "-")
            print("Cleaning directory and breadcrumbs.")
        shutil.rmtree(output_dir)


def ulog_csv(
    verbose: bool,
    ulog_dir: str,
    filter: str,
    output_dir: str,
    merge: bool,
    clean: bool,
    resample: bool,
):
    ulog_files = []
    for root, _, files in os.walk(ulog_dir):
        for file in files:
            if file.split(".")[-1] == "ulg" or file.split(".")[-1] == "ulog":
                ulog_files.append((root, file))

    if verbose:
        print(f"Converting [{len(ulog_files)}] .ulog files to .csv.")
        print("")

    with open(filter, "r") as f:
        data = yaml.safe_load(f)

    if verbose:
        print("Whitelisted topics are:")
        for entry in data["whitelist_messages"]:
            print(f" - {entry}")
        print("")
        print("Blacklisted headers are:")
        for entry in data["blacklist_headers"]:
            print(f" - {entry}")

    processes = []
    for file in ulog_files:
        process = Process(
            target=convert_ulog2csv,
            args=(
                file[0],
                file[1],
                data["whitelist_messages"],
                os.path.join(output_dir, file[0]),
                data["blacklist_headers"],
            ),
        )
        processes.append(process)
        process.start()

    i = 0
    total = len(processes)
    if verbose:
        print("")
        print("Conversion Progress:")
    for process in processes:
        process.join()
        if verbose:
            i += 1
            progress_bar(i / total)

    # = File Merge =#
    if not merge:
        return

    if verbose:
        print("")
        print("")
        print(84 * "-")
        print("Merging .csv files -- Breadcrumbs will be created as merged.csv.")
        print("")

    csv_files = []
    for root, _, files in os.walk(output_dir):
        if len(files) > 0:
            csv_files.append((root, files))

    if verbose:
        print(f"Merging into [{len(csv_files)}] .csv files.")
        print("")

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
    if verbose:
        print("Merging Progress:")
    for process in processes:
        process.join()
        if verbose:
            i += 1
            progress_bar(i / total)

    # = File Unification =#

    merge_files = []
    for root, _, files in os.walk(output_dir):
        if "merged.csv" in files:
            merge_files.append(root)

    if verbose:
        print("")
        print("")
        print(84 * "-")
        print(
            "Unifying all 'merged.csv' files into a single 'unified.csv' -- This may take a while."
        )

    unified_df = pd.concat(
        [pd.read_csv(os.path.join(file, "merged.csv")) for file in merge_files]
    )

    msg_reference = None
    if resample:
        print("")
        print("")
        print(84 * "-")
        print("Resampling `unified.csv`.")
        try:
            msg_reference = pd.read_csv(os.path.join(os.getcwd(), "msg_reference.csv"))
        except FileNotFoundError:
            print("Error: msg_reference.csv not found.")
            print(
                "Case 1 -- Ensure that the msg_reference.csv file is present in the directory this script is being run from."
            )
            print(
                "Case 2 -- Please restore this repository or download this file from the source."
            )
            print(
                "Case 3 -- If resampling is not desired, please remove the resample flag from the command line."
            )
            return

        try:
            _ = data["resample_params"]["target_frequency_hz"]
            _ = data["resample_params"]["num_method"]
            _ = data["resample_params"]["cat_method"]
            _ = data["resample_params"]["interpolate_numerical"]
            _ = data["resample_params"]["interpolate_method"]
        except KeyError:
            print("Warning: Incomplete resampling parameters provided in filter.yaml.")
            print("Using default values.")
            print("")
            data["resample_params"] = {
                "target_frequency_hz": 10,
                "num_method": "mean",
                "cat_method": "ffill",
                "interpolate_numerical": True,
                "interpolate_method": "linear",
            }

        if verbose:
            print("Resampling parameters:")
            print(
                f"-- target_frequency_hz: {data['resample_params']['target_frequency_hz']}"
            )
            print(f"-- num_method: {data['resample_params']['num_method']}")
            print(f"-- cat_method: {data['resample_params']['cat_method']}")
            print(
                f"-- target_frequency_hz: {data['resample_params']['interpolate_numerical']}"
            )
            print(
                f"-- interpolate_method: {data['resample_params']['interpolate_method']}"
            )
            print("")

        unified_df = resample_unified(
            unified_df, msg_reference, data["resample_params"], verbose
        )

    unified_df.to_csv("unified.csv", index=False)

    if clean:
        if verbose:
            print("")
            print(84 * "-")
            print("Cleaning directory and breadcrumbs.")
        shutil.rmtree(output_dir)
