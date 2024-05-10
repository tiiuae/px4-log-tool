#!/usr/bin python3
"""Converts ULog files to CSV, with options for filtering, merging, and creating a unified CSV.

This script provides a streamlined way to process PX4 ULog files. It offers flexibility in converting individual files or merging multiple files, filtering specific messages, and resampling data.

**Arguments:**

  -ulog_dir (str): Path to the directory containing ULog files.
  -filter (str): Path to a YAML filter file specifying message whitelist and header blacklist.
  -o, --output_dir (str): Output directory for converted CSV files (default: 'output_dir').
  -m, --merge: Merge CSV files within each subdirectory into 'merged.csv' files.
  -r, --resample: Resample 'merged.csv' files based on parameters in 'filter.yaml'.
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

1. **File Conversion:** Converts ULog files in the specified directory to individual CSV files, applying filters from the YAML file.
2. **File Merging (Optional):** If the `-m` flag is set, merges CSV files within each subdirectory into a single 'merged.csv'.
3. **File Unification (Optional):** Combines all 'merged.csv' files into a single 'unified.csv' file.
4. **Cleanup (Optional):** If the `-c` flag is set, removes intermediate files and directories, leaving only 'unified.csv'.
"""

import argparse
import numpy as np
from collections import Counter
import yaml
import os
import shutil
import sys
import pandas as pd
from pyulog import ULog
from multiprocessing import Process
from copy import deepcopy
from typing import List, Dict, Any


def convert_ulog2csv(
    directory_address: str,
    ulog_file_name: str,
    messages: List[str] = None,
    output: str = ".",
    blacklist: List[str] = None,
    delimiter: str = ",",
    time_s: float = None,
    time_e: float = None,
    disable_str_exceptions: bool = False,
) -> None:
    """
    Converts a PX4 ULog file to CSV files.

    This function converts a ULog file into multiple CSV files, one for each message type.
    Filtering, field exclusion, and time range extraction are supported.

    Args:
        directory_address: Directory path of the ULog file.
        ulog_file_name: Name of the ULog file to convert.
        messages: List of message names to include (all if None).
        output: Output directory for CSV files (defaults to current directory).
        blacklist: List of field names to exclude.
        delimiter: CSV delimiter (default: ",").
        time_s: Start time (in seconds) for extraction (defaults to log start).
        time_e: End time (in seconds) for extraction (defaults to log end).
        disable_str_exceptions: If True, disables string conversion exceptions.
    """

    ulog_file_name = os.path.join(directory_address, ulog_file_name)
    msg_filter = messages if messages else None

    ulog = ULog(ulog_file_name, msg_filter, disable_str_exceptions)
    data = ulog.data_list

    output_file_prefix = ulog_file_name
    # strip '.ulg' || '.ulog'
    if output_file_prefix.lower().endswith(".ulg"):
        output_file_prefix = output_file_prefix[:-4]
    elif output_file_prefix.lower().endswith(".ulog"):
        output_file_prefix = output_file_prefix[:-5]

    base_name = os.path.basename(output_file_prefix)
    output_file_prefix = os.path.join(output, base_name)

    try:
        os.makedirs(output_file_prefix)
    except FileExistsError:
        pass

    # Mark duplicated
    counts = Counter(
        [d.name.replace("/", "_") for d in data if d.name.replace("/", "_") in messages]
    )
    redundant_msgs = [string for string, count in counts.items() if count > 1]

    for d in data:
        if d.name.replace("/", "_") in redundant_msgs:
            fmt = "{0}/{1}_{2}.csv"
            output_file_name = fmt.format(
                output_file_prefix, d.name.replace("/", "_"), d.multi_id
            )
        else:
            fmt = "{0}/{1}.csv"
            output_file_name = fmt.format(
                output_file_prefix, d.name.replace("/", "_"), d.multi_id
            )
        with open(output_file_name, "w", encoding="utf-8") as csvfile:

            # use same field order as in the log, except for the timestamp
            data_keys = [f.field_name for f in d.field_data]
            data_keys.remove("timestamp")
            # Remove blacklisted data_keys
            for entry in blacklist:
                try:
                    data_keys.remove(entry)
                except ValueError:
                    continue
            data_keys.insert(0, "timestamp")  # we want timestamp at first position

            # write the header
            header_keys = deepcopy(data_keys)
            for i in range(len(header_keys)):
                header_keys[i] = header_keys[i].replace("[", "_")
                header_keys[i] = header_keys[i].replace("]", "")
            csvfile.write(delimiter.join(header_keys) + "\n")

            # get the index for row where timestamp exceeds or equals the required value
            time_s_i = (
                np.where(d.data["timestamp"] >= time_s * 1e6)[0][0] if time_s else 0
            )
            # get the index for row upto the timestamp of the required value
            time_e_i = (
                np.where(d.data["timestamp"] >= time_e * 1e6)[0][0]
                if time_e
                else len(d.data["timestamp"])
            )

            # write the data
            last_elem = len(data_keys) - 1
            for i in range(time_s_i, time_e_i):
                for k in range(len(data_keys)):
                    csvfile.write(str(d.data[data_keys[k]][i]))
                    if k != last_elem:
                        csvfile.write(delimiter)
                csvfile.write("\n")


def resample_data(
    df: pd.DataFrame,
    target_frequency_hz: float,
    num_method: str = "mean",
    cat_method: str = "ffill",
    interpolate_numerical: bool = False,
    interpolate_method: str = "linear",
    num_labels: List[str] = None,
    cat_labels: List[str] = None,
) -> pd.DataFrame:
    """
    Resamples a DataFrame to a specified frequency, handling numerical and categorical data.

    This function resamples the input DataFrame to the given target frequency in Hz. It allows
    different aggregation methods for numerical and categorical columns and provides an option
    for interpolating numerical data after resampling.

    Args:
        df: The DataFrame containing the data to resample. The DataFrame must have a
            'timestamp' column with datetime values.
        target_frequency_hz: The target resampling frequency in Hz.
        num_method: The method to use for downsampling numerical data ('mean', 'median', 'max', 'min', 'sum').
            Defaults to 'mean'.
        cat_method: The method to use for downsampling categorical data ('ffill', 'bfill', 'mode').
            Defaults to 'ffill'.
        interpolate_numerical: Whether to interpolate numerical data after resampling. Defaults to False.
        interpolate_method: The interpolation method to use if interpolation is enabled (e.g., 'linear', 'nearest', 'spline').
            Defaults to 'linear'.
        num_labels: (Optional) List of numerical column labels to resample. If None, all numerical columns are resampled.
        cat_labels: (Optional) List of categorical column labels to resample. If None, all categorical columns are resampled.

    Returns:
        The resampled DataFrame with the 'timestamp' column reset as a regular column.
    """
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")

    # Set the timestamp column as the index if it is not already
    if df.index.name != "timestamp":
        df = df.set_index("timestamp")

    # Convert frequency from Hz to pandas offset
    milliseconds_per_sample = 1000 / target_frequency_hz
    pandas_freq = f"{round(milliseconds_per_sample)}L"

    # Determine column types
    numeric_cols = df.select_dtypes(
        include=[np.number]
    ).columns  # List[str] = [column names]
    categorical_cols = df.select_dtypes(exclude=[np.number]).columns

    # Resample numerical columns
    resampled_num = df[numeric_cols].resample(pandas_freq).agg(num_method)

    # Optionally interpolate numerical data
    if interpolate_numerical:
        resampled_num = resampled_num.interpolate(method=interpolate_method)

    # Resample categorical columns
    if cat_method == "mode":
        # 'mode' is not directly supported in resample.agg, so use a custom function
        resampled_cat = (
            df[categorical_cols]
            .resample(pandas_freq)
            .agg(lambda x: x.mode()[0] if not x.empty else np.nan)
        )
    else:
        resampled_cat = df[categorical_cols].resample(pandas_freq).agg(cat_method)

    # Combine results
    resampled_df = pd.concat([resampled_num, resampled_cat], axis=1)

    # Reset index to return timestamp as a column
    resampled_df.reset_index(inplace=True)
    # resampled_df = resampled_df['timestamp'].astype('int64') // 1000
    return resampled_df


def merge_csv(
    root: str,
    files: List[str],
    msg_reference: pd.DataFrame = None,
    resample: bool = False,
    sampling_params: Dict[str, Any] = None,
) -> None:
    """
    Merges multiple CSV files in a directory, handling column renaming and resampling.

    This function merges CSV files found in the specified directory into a single 'merged.csv' file.
    Column names are intelligently renamed to avoid conflicts, and a 'mission_name' column is added
    to identify the source directory. Optionally, the merged data can be resampled based on 
    parameters provided in `sampling_params`.

    Args:
        root: The directory path containing the CSV files to merge.
        files: A list of filenames within the 'root' directory.
        msg_reference: (Optional) A DataFrame containing column alias mappings for resampling.
        resample: (Optional) If True, resample the merged DataFrame using `sampling_params`. Defaults to False.
        sampling_params: (Optional) A dictionary containing resampling parameters (required if `resample` is True).

    Returns:
        None. The merged and potentially resampled DataFrame is saved as 'merged.csv'
        in the 'root' directory.
    """

    merged_df = pd.DataFrame(data={"timestamp": []})
    for file in files:
        if file == "merged.csv":
            continue

        data_frame: pd.DataFrame = pd.read_csv(os.path.join(root, file))

        prefix_parts = file.split("_")
        capitalised_prefix_parts = [prefix_parts[0].capitalize()] + [
            part.capitalize() for part in prefix_parts[1:]
        ]
        joined_prefix = "".join(capitalised_prefix_parts)
        joined_prefix = joined_prefix.split(".")[0]

        column_names = data_frame.columns
        column_names = ["timestamp"] + [
            f"{joined_prefix}_{name}"
            for name in column_names[column_names != "timestamp"]
        ]

        data_frame.rename(
            columns=dict(zip(data_frame.columns, column_names)), inplace=True
        )

        merged_df = pd.merge(merged_df, data_frame, on="timestamp", how="outer")

    # This gets you the label
    merged_df.sort_values(by="timestamp", inplace=True)

    if resample:
        reference = deepcopy(msg_reference)
        num_labels = []
        cat_labels = []

        for label in merged_df.columns:
            if label == "timestamp" or label == "mission_name":
                continue
            msg, param = label.split("_", maxsplit=1)
            if msg[-1].isdigit():
                msg = msg[:-1]
            label_dc = reference[reference["Alias"] == f"{msg}_{param}"]
            dc = label_dc["Dataclass"].iloc(0)
            if dc == "Numerical":
                num_labels.append(label)
            else:
                cat_labels.append(label)

        merged_df = resample_data(
            merged_df,
            sampling_params["target_frequency_hz"],
            sampling_params["num_method"],
            sampling_params["cat_method"],
            sampling_params["interpolate_numerical"],
            sampling_params["interpolate_method"],
            num_labels,
            cat_labels,
        )

    merged_df["mission_name"] = os.path.basename(root)

    preamble = ["mission_name", "timestamp"]
    body = sorted([col for col in merged_df.columns if col not in preamble])
    merged_df = merged_df[preamble + body]

    merged_df.to_csv(os.path.join(root, "merged.csv"), index=False)


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


def main():
    parser = argparse.ArgumentParser(
        description="Convert ULOG files inside directory to CSV"
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
    merge: bool = args.merge
    clean: bool = args.clean

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

    if verbose:
        print("")
        print("Conversion Progress:")
        total = len(processes)
        i = 0

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
        print(
            "-------------------------------------------------------------------------------------"
        )
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
    msg_reference = None
    if resample:
        try:
            msg_reference = pd.read_csv(os.path.join(os.getcwd(), "msg_reference.csv"))
        except FileNotFoundError:
            print("Error: msg_reference.csv not found.")
            print("Case 1 -- Ensure that the msg_reference.csv file is present in the directory this script is being run from.")
            print("Case 2 -- Please restore this repository or download this file from the source.")
            print("Case 3 -- If resampling is not desired, please remove the resample flag from the command line.")
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
                "interpolate_numerical": False,
                "interpolate_method": "linear",
            }

        if verbose:
            print("Resampling:")
            print(f"-- target_frequency_hz: {data['resample_params']['target_frequency_hz']}")
            print(f"-- num_method: {data['resample_params']['num_method']}")
            print(f"-- cat_method: {data['resample_params']['cat_method']}")
            print(f"-- target_frequency_hz: {data['resample_params']['interpolate_numerical']}")
            print(f"-- interpolate_method: {data['resample_params']['interpolate_method']}")
            print("")

    for file in csv_files:
        process = Process(
            target=merge_csv,
            args=(file[0], file[1], msg_reference, resample, data["resample_params"]),
        )
        processes.append(process)
        process.start()

    if verbose:
        print("Merging Progress:")
        total = len(processes)
        i = 0
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
        print("-------------------------------------------------------------------------------------")
        print("Unifying all 'merged.csv' files into a single 'unified.csv' -- This may take a while.")

    unified_df = pd.concat(
        [pd.read_csv(os.path.join(file, "merged.csv")) for file in merge_files]
    )
    unified_df.to_csv("unified.csv", index=False)

    if clean:
        if verbose:
            print("")
            print("-------------------------------------------------------------------------------------")
            print("Cleaning directory and breadcrumbs.")
        shutil.rmtree(output_dir)


if __name__ == "__main__":
    main()
