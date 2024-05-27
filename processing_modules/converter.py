#!/usr/bin python3

import importlib
import os
from collections import Counter
from copy import deepcopy
from glob import glob
<< << << < HEAD
== == == =
>> >> >> > 3
f2b005c46c415a6032a2e6fedbffd9377098831
from typing import List

import numpy as np
import pandas as pd
import yaml
from pyulog import ULog


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
    if messages != None:
        counts = Counter(
            [d.name.replace("/", "_") for d in data if d.name.replace("/", "_") in messages]
        )
    else:
        counts = Counter(
            [d.name.replace("/", "_") for d in data]
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


# TODO This needs to be refactored
def px4_mcap_to_csv(mcap_dir: str) -> None:
    """
    Convert PX4 MCAP files to CSV format.

    Args:
        mcap_dir (str): Path to the directory containing MCAP directories.

    Raises:
        FileNotFoundError: If no MCAP files are found in the specified directory.
    """
    from mcap_ros2.reader import read_ros2_messages
    from rosidl_runtime_py import message_to_ordereddict

    try:
        mcap_filename = glob(os.path.join(mcap_dir, "*.mcap"))[0]
        metadata = glob(os.path.join(mcap_dir, "*.yaml"))[0]
    except IndexError:
        print(f"No MCAP files found in {mcap_dir}")
        return

    with open(metadata, "r") as file:
        metadata = yaml.safe_load(file)

    msg_df = {}
    msg_rosdict = {}
    msg_csvstr = {}

    for i in range(len(metadata["rosbag2_bagfile_information"]["topics_with_message_count"])):
        msg_addr = metadata["rosbag2_bagfile_information"]["topics_with_message_count"][i]["topic_metadata"]["type"]

        msg_addr = msg_addr.split("/")

        module = importlib.import_module(msg_addr[0])
        message_package = getattr(module, msg_addr[1])
        message = getattr(message_package, msg_addr[2])

        empty_msg_dict = message_to_ordereddict(message())
        msg_rosdict[message().__class__.__name__] = empty_msg_dict

        header = []
        for sub_key in list(empty_msg_dict.keys()):
            if sub_key == 'timestamp':
                header.append(sub_key)
            else:
                try:
                    for j in range(len(empty_msg_dict[sub_key])):
                        header.append(sub_key + f"_{j}")
                except TypeError:
                    header.append(f"{sub_key}")
        header = ",".join(header) + "\n"

        msg_csvstr[message().__class__.__name__] = header

    i = 0
    try:
        for msg in read_ros2_messages(mcap_filename):
            msg_class = msg.ros_msg.__class__.__name__

            try:
                current_line = ",".join([f"{msg.ros_msg.__getattribute__(key)}" for key in list(msg_rosdict[msg_class].keys())]) + "\n"
                current_line = current_line.replace("[", "")
                current_line = current_line.replace("]", "")
                current_line = current_line.replace(", ", ",")
                msg_csvstr[msg_class] += current_line
            except:
                continue

        for key in list(msg_csvstr.keys()):
            dat = [x.split(",") for x in msg_csvstr[key].strip("\n").split("\n")]
            msg_df[key] = pd.DataFrame(dat)
            msg_df[key] = msg_df[key].T.set_index(0, drop=True).T
    except Exception as e:
        print(f"Message versions probably aren't matching. Confirm if message fields are matching: {e}")

    for key in list(msg_df.keys()):
        dumpfile = f"{mcap_dir}{key}.csv"
        msg_df[key].to_csv(dumpfile, index=False)

    return
