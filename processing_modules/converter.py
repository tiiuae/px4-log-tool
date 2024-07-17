#!/usr/bin python3

import numpy as np
import importlib
import os
import re
import pandas as pd
import yaml
from collections import Counter
from copy import deepcopy
from glob import glob
from pyulog import ULog
from typing import List


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
        directory_address (str): Directory path of the ULog file.
        ulog_file_name (str): Name of the ULog file to convert.
        messages (List[str]): List of message names to include (all if None).
        output (str): Output directory for CSV files (defaults to current directory).
        blacklist (List[str]): List of field names to exclude.
        delimiter (str): CSV delimiter (default: ",").
        time_s (float): Start time (in seconds) for extraction (defaults to log start).
        time_e (float): End time (in seconds) for extraction (defaults to log end).
        disable_str_exceptions (bool): If True, disables string conversion exceptions.
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


def convert_csv2ros2bag(
    directory_address: str,
    topic_prefix: str = "/fmu/out",
    capitalise_topics: bool = False
    ) -> None:
    """
    Converts CSV files to a ROS 2 bag file.

    This function reads CSV files from the specified directory and converts
    their contents into ROS 2 messages, which are then written to a ROS 2 bag
    file. The CSV files should be named according to the message types and
    topics they represent, and the data will be serialized accordingly.

    Args:
        directory_address (str): Directory path containing the CSV files.
        topic_prefix (str): Prefix to the topics in the bag file.

    Raises:
        ValueError: If a field in the CSV file does not match any field in the corresponding ROS message type.
    """
    try:
        import rosbag2_py
        import importlib
        import px4_msgs.msg # pylint: disable=unused-import
        from rclpy.serialization import serialize_message
    except Exception as e:
        if e == ImportError or e == ModuleNotFoundError:
            print("ERROR: Missing required ROS 2 packages. Skipping conversion to ROS 2 bag.")
        else:
            print("ERROR")
        return

    def set_msg_field(msg, field_name, value):
        """
        Set the value of a message field, handling nested fields and array
        indices, and ensuring type compatibility.
        """

        if '_' in field_name and field_name.split('_')[-1].isdigit():
            field_base = '_'.join(field_name.split('_')[:-1])
            index = int(field_name.split('_')[-1])
            if hasattr(msg, field_base):
                array_field = getattr(msg, field_base)
                if isinstance(array_field, (list, np.ndarray)):
                    array_field[index] = type(array_field[index])(value)
                else:
                    raise ValueError(f"Field {field_base} is not an array in message type {type(msg).__name__}")
        else:
            if hasattr(msg, field_name):
                current_type = type(getattr(msg, field_name))
                setattr(msg, field_name, current_type(value))
            else:
                raise ValueError(f"Message type {type(msg).__name__} has no field {field_name}")

    writer = rosbag2_py.SequentialWriter()

    # Catching edge cases where directory_address is a PosixPath
    try:
        bag_name = directory_address.split("/")[-1]
    except AttributeError:
        directory_address = str(directory_address)
        bag_name = directory_address.split("/")[-1]

    storage_options = rosbag2_py._storage.StorageOptions(
        uri=f"{directory_address}/{bag_name}",
        storage_id="sqlite3",
    )
    converter_options = rosbag2_py._storage.ConverterOptions("", "")
    writer.open(storage_options, converter_options)

    csv_files = [f for f in os.listdir(directory_address) if f.endswith(".csv")]
    if len(csv_files) == 0:
        print("WARNING: Directory does not have any .csv files. Skipping conversion to ROS 2 bag.")
        return

    topic_dict = {}
    for csv_file in csv_files:
        base_name: str = csv_file.split(".")[0]
        if capitalise_topics:
            name = base_name.split("_")
            for comp in name:
                comp = comp.capitalize()
            base_name = "".join(name)
        if base_name[-1].isdigit():
            topic_name = f"{topic_prefix}/{base_name[:-2]}/f_{base_name[-1]}"
        else:
            topic_name = f"{topic_prefix}/{base_name}"
        msg_type = ''.join(part.capitalize() for part in re.sub(r'_\d+', '', base_name).split("_"))
        topic_dict[base_name] = (topic_name, msg_type)
        topic_info = rosbag2_py._storage.TopicMetadata(
            name=topic_name,
            type=f"px4_msgs/msg/{msg_type}",
            serialization_format="cdr"
        )
        writer.create_topic(topic_info)

    for base_name, (topic_name, msg_type) in topic_dict.items():
        df = pd.read_csv(os.path.join(directory_address, f"{base_name}.csv"))
        msg_class = getattr(importlib.import_module("px4_msgs.msg"), msg_type)

        for _, row in df.iterrows():
            msg = msg_class()
            for field in row.index:
                set_msg_field(msg, field, row[field])
            writer.write(topic_name, serialize_message(msg), msg.timestamp * 1000)

# TODO This needs to be refactored
def px4_mcap_to_csv(mcap_dir: str) -> None:
    """
    Convert PX4 MCAP files to CSV format.

    Args:
        mcap_dir (str): Path to the directory containing MCAP directories.
    Raises:

        FileNotFoundError: If no MCAP files are found in the specified directory.
    """
    import px4_msgs.msg
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
