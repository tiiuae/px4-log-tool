#!/usr/bin python3
"""
Converts ULog files to CSV, with options for filtering, merging, and creating a unified CSV.

**Usage:**

```bash
python <script_name> -h  # Display help and options
```
```bash
python <script_name> <ulog_dir> <filter.yaml> # Only runs conversion and output into `output_dir`
```
```bash
python <script_name> <ulog_dir> <filter.yaml> -o <custom_output_dir> # Runs conversion and output in custom_output_dir
```
```bash
python <script_name> <ulog_dir> <filter.yaml> -o <output_dir> -m # Merges CSV files into a unified.csv file

**Arguments:**

* **ulog_dir (str):**  Path to the directory containing ULog files.
* **filter.yaml (str):** Path to a YAML filter file specifying:
    * **whitelist_messages (list):**  List of message names to include during conversion 
    * **blacklist_headers (list):** List of headers (field names) to exclude from the output CSV files.
* **-o, --output_dir (str):**  Output directory for converted CSV files.  Mirrors the input directory structure. Defaults to 'output_dir'.
* **-m, --merge:** If specified, merges CSV files within each subdirectory into a 'merged.csv' file.
* **-c, --clean:**  If specified, cleans up intermediate files after merging, leaving only 'unified.csv'.
* **-v, --verbose:**  Enables verbose logging during the process.

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
from typing import List

def convert_ulog2csv(
    directory_address: str,
    ulog_file_name: str,
    messages: List[str],
    output: str,
    blacklist: List[str],
    delimiter=",",
    time_s=None,
    time_e=None,
    disable_str_exceptions=False,
):
    """
    Converts a PX4 ULog file to a set of CSV files. Provides options for filtering messages, excluding fields, and specifying a time range for extraction.
    Function adapted from (https://github.com/PX4/pyulog/blob/main/pyulog/ulog2csv.py)

    :param directory_address: The directory address of the ULog file.
    :param ulog_file_name: The name of the ULog file to convert.
    :param messages: (Optional) List of message names to include. All messages included if None.
    :param output: (Optional) Output directory for CSV files. Defaults to current directory.
    :param blacklist: (Optional) List of field names to exclude from the CSV output.
    :param delimiter: (Optional) CSV delimiter. Defaults to ",".
    :param time_s: (Optional) Start time (seconds) for extraction. Defaults to the log's beginning.
    :param time_e: (Optional) End time (seconds) for extraction. Defaults to the log's end.
    :param disable_str_exceptions: (Optional) If True, disables string conversion exceptions. 

    :return: None
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
    counts = Counter([d.name.replace("/", "_") for d in data if d.name.replace("/", "_") in messages])
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
            for entry in blacklist:
                try:
                    data_keys.remove(entry)
                except ValueError:
                    continue
            data_keys.insert(0, "timestamp")  # we want timestamp at first position

            # write the header
            csvfile.write(delimiter.join(data_keys) + "\n")

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


def merge_csv(root: str, files: List[str]):
    """
    Merges multiple CSV files in a directory, intelligently renaming columns and adding a 'label' column.

    :param root: The root directory containing the CSV files to be merged.
    :param files: A list of filenames within the root directory.

    :return: None. Saves the merged result as 'merged.csv' within the root directory.
    """

    merged_df = pd.DataFrame(data = {'timestamp':[]})
    for file in files:
        if file == "merged.csv":
            continue

        data_frame: pd.DataFrame = pd.read_csv(os.path.join(root, file))

        prefix_parts = file.split('_')
        capitalised_prefix_parts = [prefix_parts[0].capitalize()] + [part.capitalize() for part in prefix_parts[1:]]
        joined_prefix = ''.join(capitalised_prefix_parts)
        joined_prefix = joined_prefix.split('.')[0]

        column_names = data_frame.columns
        column_names = ["timestamp"] + [f"{joined_prefix}_{name}" for name in column_names[column_names != "timestamp"]]

        data_frame.rename(columns=dict(zip(data_frame.columns, column_names)), inplace=True)

        merged_df = pd.merge(merged_df, data_frame, on="timestamp", how="outer")

    # This gets you the label
    merged_df.sort_values(by="timestamp", inplace=True)
    merged_df['mission_name'] = os.path.basename(root)

    preamble = ['mission_name', 'timestamp']
    body = sorted([col for col in merged_df.columns if col not in preamble])
    merged_df = merged_df[preamble + body]

    merged_df.to_csv(os.path.join(root, "merged.csv"), index=False)

def progress_bar(progress):
    """
    Displays a simple command-line progress bar.

    :param progress: A value between 0.0 and 1.0 representing the progress percentage.

    :return: None.  Displays the progress bar directly to the console. 
    """

    bar_length = 50
    progress_length = int(bar_length * progress)
    bar = '[' + '=' * progress_length + ' ' * (bar_length - progress_length) + ']'
    sys.stdout.write('\r' + bar + ' ' + f'{progress * 100:.1f}%')
    sys.stdout.flush()

def main():
    parser = argparse.ArgumentParser(
        description="Convert ULOG files inside directory to CSV"
    )
    parser.add_argument("-m", "--merge", action="count", help="Merge all files into one .csv (leaves breadcrumbs)", default=0)
    parser.add_argument("-c", "--clean", action="count", help="Cleans directory and breadcrumbs leaving only unified.csv", default=0)
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
    merge: bool = args.merge
    clean: bool = args.clean

    #= File Conversion =#

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
            progress_bar(i/total)

    print("")

    #= File Merge =#
    if not merge:
        return

    if verbose:
        print("")
        print("-------------------------------------------------------------------------------------")
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
            args=(file[0], file[1])
        )
        processes.append(process)
        process.start()

    if verbose:
        print("")
        print("Merging Progress:")
        total = len(processes)
        i = 0
    for process in processes:
        process.join()
        if verbose:
            i += 1
            progress_bar(i/total)

    print("")

    #= File Unification =#

    merge_files = []
    for root, _, files in os.walk(output_dir):
        if "merged.csv" in files:
            merge_files.append(root)
    
    if verbose:
        print("")
        print("-------------------------------------------------------------------------------------")
        print("Unifying all 'merged.csv' files into a single 'unified.csv' -- This may take a while.")

    unified_df = pd.concat([pd.read_csv(os.path.join(file, "merged.csv")) for file in merge_files])
    unified_df.to_csv("unified.csv", index=False)

    if clean:
        if verbose:
            print("")
            print("-------------------------------------------------------------------------------------")
            print("Cleaning directory and breadcrumbs.")
        shutil.rmtree(output_dir)

if __name__ == "__main__":
    main()
