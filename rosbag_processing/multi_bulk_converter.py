#!/usr/bin python3

import argparse
import pandas as pd
import os
import px4_msgs.msg
from mcap_ros2.reader import read_ros2_messages
from rosidl_runtime_py import message_to_ordereddict
import importlib
from glob import glob
import yaml

from multiprocessing import Process

def px4_mcap_to_csv(mcap_dir: str) -> None:
    """
    Convert PX4 MCAP files to CSV format.

    Args:
        mcap_dir (str): Path to the directory containing MCAP directories.

    Raises:
        FileNotFoundError: If no MCAP files are found in the specified directory.
    """
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

def multi_thread_initiator(mcap_dir: str):
    dir = os.listdir(mcap_dir)
    processes = []
    for entry in dir:
        is_dir = os.path.isdir(f"{mcap_dir}/{entry}")
        if is_dir:
            print(f"Currently in {mcap_dir}/{entry}/")
            process = Process(target=px4_mcap_to_csv, args=(f"{mcap_dir}/{entry}/",))
            processes.append(process)
            process.start()

    for process in processes:
        process.join()

def main():
    parser = argparse.ArgumentParser(description="Convert PX4 MCAP files to CSV")
    parser.add_argument("directory", help="Path to the directory containing all MCAP folders", default=None)

    args = parser.parse_args()

    mcap_dir = args.directory

    multi_thread_initiator(mcap_dir)

if __name__ == "__main__":
    main()
