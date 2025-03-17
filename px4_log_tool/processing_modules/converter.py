#!/usr/bin python3

import numpy as np
import sqlite3
import csv
import os
import re
import pandas as pd
from collections import Counter
from copy import deepcopy
from pyulog import ULog
from typing import Dict, List
from px4_log_tool.util.logger import log


def convert_ulog2csv(
    directory_address: str,
    ulog_file_name: str,
    messages: List[str] | None = None,
    output: str = ".",
    blacklist: List[str] = [],
    delimiter: str = ",",
    time_s: float | None = None,
    time_e: float | None = None,
    disable_str_exceptions: bool = False,
    verbose: bool = False,
) -> Dict:
    """
    Converts a PX4 ULog file to CSV files.

    This function converts a ULog file into multiple CSV files, one for each message type.
    Filtering, field exclusion, and time range extraction are supported.

    Args:
    - directory_address (str): Directory path of the ULog file.
    - ulog_file_name (str): Name of the ULog file to convert.
    - messages (List[str]): List of message names to include (all if None).
    - output (str): Output directory for CSV files (defaults to current directory).
    - blacklist (List[str]): List of field names to exclude.
    - delimiter (str): CSV delimiter (default: ",").
    - time_s (float): Start time (in seconds) for extraction (defaults to log start).
    - time_e (float): End time (in seconds) for extraction (defaults to log end).
    - disable_str_exceptions (bool): If True, disables string conversion exceptions.
    - verbose (bool): Verbosity of logging.
    """

    ulog_file_name = os.path.join(directory_address, ulog_file_name)
    msg_filter = messages if messages else None

    try:
        ulog = ULog(ulog_file_name, msg_filter, disable_str_exceptions)
        data = ulog.data_list
    except Exception:
        log(
            "Issue with converting file "
            + ulog_file_name
            + ". It is most likely due to its filetype or integrity.",
            verbosity=verbose,
            log_level=1,
        )
        return {}

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
    if messages is not None:
        counts = Counter(
            [
                d.name.replace("/", "_")
                for d in data
                if d.name.replace("/", "_") in messages
            ]
        )
    else:
        counts = Counter([d.name.replace("/", "_") for d in data])
    redundant_msgs = [string for string, count in counts.items() if count > 1]
    data_frame_dict = {}

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
        data_frame_dict[output_file_name.split("/")[-1].split(".")[0]] = pd.read_csv(
            output_file_name
        )
    return data_frame_dict


def convert_csv2ros2bag(
    directory_address: str,
    output_dir: str,
    topic_prefix: str = "/fmu/out",
    capitalise_topics: bool = False,
    verbose: bool = False,
) -> None:
    """
    Converts CSV files to a ROS 2 bag file.

    This function reads CSV files from the specified directory and converts
    their contents into ROS 2 messages, which are then written to a ROS 2 bag
    file. The CSV files should be named according to the message types and
    topics they represent, and the data will be serialized accordingly.

    Args:
    - directory_address (str): Directory path containing the CSV files.
    - topic_prefix (str): Prefix to the topics in the bag file.
    - capitalise_topics (bool): For compatibility with snake and camelcase topics.
    - verbose (bool): Verbosity of logging.
    """
    try:
        import rosbag2_py
        import importlib
        import px4_msgs.msg
        from rclpy.serialization import serialize_message
    except Exception as e:
        if e is not ImportError or e is not ModuleNotFoundError:
            log(
                "Missing required ROS 2 packages. Make sure that the ROS 2 environment is sourced. Skipping conversion to ROS 2 bag.",
                verbosity=verbose,
                log_level=2,
            )
        else:
            log(
                "Missing required ROS 2 px4_msgs library. Make sure that it is sourced. Skipping conversion to ROS 2 bag.",
                verbosity=verbose,
                log_level=2,
            )
        return

    def set_msg_field(msg, field_name, value):
        """
        Set the value of a message field, handling nested fields and array
        indices, and ensuring type compatibility.
        """

        if "_" in field_name and field_name.split("_")[-1].isdigit():
            field_base = "_".join(field_name.split("_")[:-1])
            index = int(field_name.split("_")[-1])
            if hasattr(msg, field_base):
                array_field = getattr(msg, field_base)
                if isinstance(array_field, (list, np.ndarray)):
                    array_field[index] = type(array_field[index])(value)
                else:
                    raise ValueError(
                        f"Field {field_base} is not an array in message type {type(msg).__name__}"
                    )
        else:
            if hasattr(msg, field_name):
                current_type = type(getattr(msg, field_name))
                setattr(msg, field_name, current_type(value))
            else:
                raise ValueError(
                    f"Message type {type(msg).__name__} has no field {field_name}"
                )

    writer = rosbag2_py.SequentialWriter()

    # Catching edge cases where directory_address is a PosixPath
    try:
        bag_name = directory_address.split("/")[-1]
    except AttributeError:
        directory_address = str(directory_address)
        bag_name = directory_address.split("/")[-1]

    storage_options = rosbag2_py._storage.StorageOptions(
        uri=f"{output_dir}/{bag_name}",
        storage_id="sqlite3",
    )
    converter_options = rosbag2_py._storage.ConverterOptions("", "")
    writer.open(storage_options, converter_options)

    csv_files = [f for f in os.listdir(directory_address) if f.endswith(".csv")]
    if len(csv_files) == 0:
        log(
            "Directory does not have any .csv files. Skipping conversion to ROS 2 bag.",
            verbosity=verbose,
            log_level=2,
        )
        return

    topic_dict = {}
    for csv_file in csv_files:
        base_name: str = csv_file.split(".")[0]
        name = base_name
        if capitalise_topics:
            name = "".join([comp.capitalize() for comp in base_name.split("_")])
        if base_name[-1].isdigit():
            topic_name = f"{topic_prefix}/{name[:-2]}/f_{base_name[-1]}"
        else:
            topic_name = f"{topic_prefix}/{name}"
        msg_type = "".join(
            part.capitalize() for part in re.sub(r"_\d+", "", base_name).split("_")
        )
        topic_dict[base_name] = (topic_name, msg_type)
        topic_info = rosbag2_py._storage.TopicMetadata(
            name=topic_name, type=f"px4_msgs/msg/{msg_type}", serialization_format="cdr"
        )
        writer.create_topic(topic_info)

    for base_name, (topic_name, msg_type) in topic_dict.items():
        df = pd.read_csv(os.path.join(directory_address, f"{base_name}.csv"))
        try:
            msg_class = getattr(importlib.import_module("px4_msgs.msg"), msg_type)
        except AttributeError:
            continue

        for _, row in df.iterrows():
            msg = msg_class()
            for field in row.index:
                set_msg_field(msg, field, row[field])
            writer.write(topic_name, serialize_message(msg), msg.timestamp * 1000)


def convert_ros2bag2csv(bag_file_address: str, verbose: bool = False):
    try:
        import px4_msgs.msg
        from rosidl_runtime_py.utilities import get_message
        from rclpy.serialization import deserialize_message
    except Exception as e:
        if e is not ImportError or e is not ModuleNotFoundError:
            log(
                "Missing required ROS 2 packages. Make sure that the ROS 2 environment is sourced. Skipping conversion to ROS 2 bag.",
                verbosity=verbose,
                log_level=2,
            )
        else:
            log(
                "Missing required ROS 2 px4_msgs library. Make sure that it is sourced. Skipping conversion to ROS 2 bag.",
                verbosity=verbose,
                log_level=2,
            )
        return

    files: list[str] = os.listdir(bag_file_address)
    rosbag_db: str | None = None
    for file in files:
        if file.endswith(".db3"):
            rosbag_db = file
    if rosbag_db is None:
        return
    
    conn = sqlite3.connect(os.path.join(bag_file_address, rosbag_db))
    c = conn.cursor()
    topic_names: list[str] = []
    topic_types: list[str] = []
    topic_id = []
    records = c.execute("SELECT * from({})".format("topics")).fetchall()
    for row in records:
        if row[1] == "/rosout" : 
            topic_names.append("")
            topic_types.append("")
            topic_id.append(None)
        else:
            topic_names.append(row[1])
            topic_types.append(row[2])
            topic_id.append(row[0])

    msg_records = c.execute("SELECT * from({})".format("messages")).fetchall()

    for i in range(len(topic_names)):
        if topic_names[i] == "":
            continue
        time = []
        count = 0
        messages = []
        msg_type = get_message(topic_types[i])
        for row in msg_records:
            if topic_id[i] == row[1]:
                count += 1
                time.append(row[2])
                messages.append(row[3])
        if len(messages) < 1:
            continue

        os.makedirs(os.path.join(bag_file_address , "topic_csvs"), exist_ok=True)
        csv_file_name: str = topic_names[i].replace("/",".")[1:]
        with open(f"{bag_file_address }/topic_csvs/{csv_file_name}.csv", "w", newline="") as csvfile:
            # Create a CSV writer
            csv_writer = csv.writer(csvfile)

            # store header and attribute rows
            attributes = []
            header_row = ["ros_timestamp"]
            for key in dir(deserialize_message(messages[0], msg_type)):
                if key[0] != "_" and key.islower() and key != "get_fields_and_field_types":
                    attributes.append(key)
                    try:
                        attr_size = getattr(deserialize_message(messages[0], msg_type), key)
                        if len(attr_size) > 0:
                            for i in range(len(attr_size)):
                                header_row.append(f"{key}_{i}")
                            else:
                                header_row.append(f"{key}_0")
                    except Exception:
                        header_row.append(key)

            csv_writer.writerow(header_row)

            # Write rows
            for timestamp, message in zip(time, messages):
                deserialized_msg = deserialize_message(message, msg_type)
                
                row = []
                row.append(timestamp)
                for key in attributes:
                    try:
                        attr_size = getattr(deserialized_msg, key)
                        if len(attr_size) > 0:
                            for i in range(len(attr_size)):
                                row.append(getattr(deserialized_msg, key)[i])
                            else:
                                row.append(getattr(deserialized_msg, key)[0])
                    except Exception:
                        row.append(getattr(deserialized_msg, key))
                csv_writer.writerow(row)

    conn.close()
            
## TODO: REFACTOR
# import importlib
# import yaml
# from glob import glob
# 
# def px4_mcap_to_csv(mcap_dir: str) -> None:
#     """
#     Convert PX4 MCAP files to CSV format.
#
#     Args:
#         mcap_dir (str): Path to the directory containing MCAP directories.
#     Raises:
#
#         FileNotFoundError: If no MCAP files are found in the specified directory.
#     """
#     import px4_msgs.msg
#     from mcap_ros2.reader import read_ros2_messages
#     from rosidl_runtime_py import message_to_ordereddict
#
#     try:
#         mcap_filename = glob(os.path.join(mcap_dir, "*.mcap"))[0]
#         metadata = glob(os.path.join(mcap_dir, "*.yaml"))[0]
#     except IndexError:
#         print(f"No MCAP files found in {mcap_dir}")
#         return
#
#     with open(metadata, "r") as file:
#         metadata = yaml.safe_load(file)
#
#     msg_df = {}
#     msg_rosdict = {}
#     msg_csvstr = {}
#
#     for i in range(
#         len(metadata["rosbag2_bagfile_information"]["topics_with_message_count"])
#     ):
#         msg_addr = metadata["rosbag2_bagfile_information"]["topics_with_message_count"][
#             i
#         ]["topic_metadata"]["type"]
#
#         msg_addr = msg_addr.split("/")
#
#         module = importlib.import_module(msg_addr[0])
#         message_package = getattr(module, msg_addr[1])
#         message = getattr(message_package, msg_addr[2])
#
#         empty_msg_dict = message_to_ordereddict(message())
#         msg_rosdict[message().__class__.__name__] = empty_msg_dict
#
#         header = []
#         for sub_key in list(empty_msg_dict.keys()):
#             if sub_key == "timestamp":
#                 header.append(sub_key)
#             else:
#                 try:
#                     for j in range(len(empty_msg_dict[sub_key])):
#                         header.append(sub_key + f"_{j}")
#                 except TypeError:
#                     header.append(f"{sub_key}")
#         header = ",".join(header) + "\n"
#
#         msg_csvstr[message().__class__.__name__] = header
#
#     i = 0
#     try:
#         for msg in read_ros2_messages(mcap_filename):
#             msg_class = msg.ros_msg.__class__.__name__
#
#             try:
#                 current_line = (
#                     ",".join(
#                         [
#                             f"{msg.ros_msg.__getattribute__(key)}"
#                             for key in list(msg_rosdict[msg_class].keys())
#                         ]
#                     )
#                     + "\n"
#                 )
#                 current_line = current_line.replace("[", "")
#                 current_line = current_line.replace("]", "")
#                 current_line = current_line.replace(", ", ",")
#                 msg_csvstr[msg_class] += current_line
#             except Exception as _:
#                 continue
#
#         for key in list(msg_csvstr.keys()):
#             dat = [x.split(",") for x in msg_csvstr[key].strip("\n").split("\n")]
#             msg_df[key] = pd.DataFrame(dat)
#             msg_df[key] = msg_df[key].T.set_index(0, drop=True).T
#     except Exception as e:
#         print(
#             f"Message versions probably aren't matching. Confirm if message fields are matching: {e}"
#         )
#
#     for key in list(msg_df.keys()):
#         dumpfile = f"{mcap_dir}{key}.csv"
#         msg_df[key].to_csv(dumpfile, index=False)
#
#     return
