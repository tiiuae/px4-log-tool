#!/usr/bin python3
import os
from copy import deepcopy
from multiprocessing import Process
from typing import Any, Dict
from px4_log_tool.util.logger import log
from px4_log_tool.util.tui import progress_bar
from px4_log_tool.processing_modules.converter import convert_csv2ros2bag, convert_ulog2csv
from px4_log_tool.processing_modules.merger import merge_csv
from px4_log_tool.processing_modules.resampler import resample_data, adjust_topic_rate

import pandas as pd
import yaml

def resample_unified(
    unified_df: pd.DataFrame,
    msg_reference: pd.DataFrame,
    resample_params: Dict[str, Any],
    in_place: bool = False,
    verbose: bool = False,
) -> pd.DataFrame | pd.Series:
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

DEFAULT_FILTER_CONFIG = {
    "whitelist_messages": {
        "default": ["sensor_combined", "actuator_outputs"],
        "description": "Whitelisted topics"
    },
    "blacklist_headers": {
        "default": ["timestamp_sample", "device_id", "error_count"],
        "description": "Blacklisted headers"
    },
    "resample_params": {
        "default": {
            "target_frequency_hz": 10,
            "num_method": "mean",
            "cat_method": "ffill",
            "interpolate_numerical": True,
            "interpolate_method": "linear",
        },
        "description": "Resampling parameters"
        # sub_keys_check is removed as the logic now always merges,
        # defaulting only missing sub-keys.
    },
    "bag_params": {
        "default": {
            "topic_prefix": "/fmu/out",
            "topic_max_frequency_hz": 100,
            "capitalise_topics": False,
        },
        "description": "ROS 2 bag parameters"
    }
    # To add a new section, simply add a new entry here:
    # "new_feature_params": {
    #     "default": {"param1": "value1", "enabled": True},
    #     "description": "Parameters for New Feature"
    # }
}

def dump_template_filter(file_path: str, verbose: bool = False):
    """
    Creates a template filter YAML file with default values from the schema.

    Args:
    - file_path (str, optional): Path where the template file will be saved.
                                 Defaults to "filter_template.yml" in the current directory.
    """
    template_data = {}
    for key, config_item in DEFAULT_FILTER_CONFIG.items():
        template_data[key] = config_item["default"]
    
    filter_path = os.path.join(file_path, "filter.yaml")
    try:
        with open(filter_path, "w") as f:
            yaml.dump(template_data, f, sort_keys=False, indent=2, default_flow_style=False)
        log(f"Template filter file created at '{filter_path}'", verbosity=verbose, log_level=0)
    except IOError as e:
        log(f"Error writing template file to {filter_path}: {e}", verbosity=verbose, log_level=1)


def extract_filter(filter_str: str | None, verbose: bool = False):
    """
    Extracts filter parameters from a YAML file, applying defaults from
    DEFAULT_FILTER_CONFIG for missing or incomplete sections.

    Args:
    - filter_file_path (str, optional): Path to the YAML filter file.
                                       If None, all default values are used.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
    - dict: The loaded and defaulted filter configuration.
    """
    user_config = {}
    config_source_name = "the provided configuration" if filter_str else "default configuration (no file provided)"
    
    if isinstance(filter_str, str):
        config_source_name = f"'{filter_str}'"
        try:
            with open(filter_str, "r") as f:
                loaded_yaml = yaml.safe_load(f)
            user_config = loaded_yaml if isinstance(loaded_yaml, dict) else {}
            if loaded_yaml is None: 
                 user_config = {}
        except FileNotFoundError:
            log(f"Warning: Filter file {config_source_name} not found.", verbosity=verbose, log_level=1)
            log("Using default values for all sections.", verbosity=verbose, log_level=1)
            if verbose:
                log("", verbosity=verbose, log_level=1) 
            user_config = {} 
        except yaml.YAMLError as e:
            log(f"Error parsing YAML file {config_source_name}: {e}.", verbosity=verbose, log_level=1)
            log("Using default values for all sections.", verbosity=verbose, log_level=1)
            if verbose:
                log("", verbosity=verbose, log_level=1)
            user_config = {}
    elif filter_str is not None: 
        log(f"Warning: Invalid filter_file_path type ({type(filter_str)}). Expected string or None.", verbosity=verbose, log_level=1)
        log("Using default values for all sections.", verbosity=verbose, log_level=1)
        if verbose:
            log("", verbosity=verbose, log_level=1)
        user_config = {}


    final_filter_config = {}

    for key, schema_item in DEFAULT_FILTER_CONFIG.items():
        default_value_for_key = deepcopy(schema_item["default"])
        description = schema_item["description"]
        
        section_value_to_assign = None

        if key not in user_config:
            log(f"Warning: Missing section '{key}' in {config_source_name}.", verbosity=verbose, log_level=1)
            log("Using default values for this section.", verbosity=verbose, log_level=1)
            log("", verbosity=verbose, log_level=1)
            section_value_to_assign = default_value_for_key
        else:
            user_section_data = user_config[key]
            if isinstance(default_value_for_key, dict):
                if not isinstance(user_section_data, dict):
                    log(f"Warning: Section '{key}' in {config_source_name} is not a dictionary as expected. User provided type: {type(user_section_data)}.", verbosity=verbose, log_level=1)
                    log("Using default values for this entire section.", verbosity=verbose, log_level=1)
                    log("", verbosity=verbose, log_level=1)
                    section_value_to_assign = default_value_for_key
                else:
                    # Both schema default and user input are dictionaries.
                    # Start with the schema's defaults for this section.
                    merged_section = deepcopy(default_value_for_key)
                    
                    for user_sub_key in user_section_data:
                        if user_sub_key not in merged_section: # Check against keys in the default dict
                            log(f"Info: User-provided sub-key '{user_sub_key}' in section '{key}' of {config_source_name} is not defined in the default schema for this section. It will be included.", verbosity=verbose, log_level=0)
                    
                    merged_section.update(user_section_data)
                    
                    defaulted_sub_keys_this_section = []
                    for default_sub_k in default_value_for_key: # Iterate over keys in the original default
                        if default_sub_k not in user_section_data: # Check if user provided it
                            defaulted_sub_keys_this_section.append(default_sub_k)
                    
                    if defaulted_sub_keys_this_section:
                        log(f"Info: For section '{key}' in {config_source_name}, default values were applied for the following missing sub-key(s): {', '.join(defaulted_sub_keys_this_section)}.", verbosity=verbose, log_level=0)
                        log("", verbosity=verbose, log_level=1) 
                             
                    section_value_to_assign = merged_section
            else:
                section_value_to_assign = user_section_data
        
        final_filter_config[key] = section_value_to_assign

        log(f"{description}:", verbosity=verbose, log_level=0, bold=True)
        current_section_data = final_filter_config[key]
        if isinstance(current_section_data, list):
            if not current_section_data and verbose : 
                 log("-- (empty)", verbosity=verbose, log_level=0)
            for entry in current_section_data:
                log(f"-- {entry}", verbosity=verbose, log_level=0)
        elif isinstance(current_section_data, dict):
            if not current_section_data and verbose: 
                 log("-- (empty)", verbosity=verbose, log_level=0)
            for sub_key, sub_value in current_section_data.items():
                log(f"-- {sub_key}: {sub_value}", verbosity=verbose, log_level=0)
        else: 
            log(f"-- {current_section_data}", verbosity=verbose, log_level=0)
        
        if verbose: 
            log("", verbosity=verbose, log_level=0)

    return final_filter_config


def get_ulog_files(ulog_dir: str, verbose: bool = False) -> list[tuple[str,str]]:
    """
    Retrieves a list of `.ulog` files from the specified directory.

    Args:
    - ulog_dir (str): Path to the directory containing `.ulog` files.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Returns:
    - list[str]: A list of tuples, where each tuple contains the file path and filename.
    """
    ulog_files: list[tuple[str,str]] = []
    for root, _, files in os.walk(ulog_dir):
        for file in files:
            if file.split(".")[-1] == "ulg" or file.split(".")[-1] == "ulog":
                ulog_files.append((root, file))

    log(msg=f"Converting [{len(ulog_files)}] .ulog files.", verbosity=verbose, log_level=0)
    return ulog_files


def get_csv_dirs(csv_dir: str, verbose: bool = False) -> list[str]:
    csv_dirs: list[str] = []
    for root, subdirs, files in os.walk(csv_dir):
        if not subdirs:
            if all(file.endswith(".csv") for file in files):
                csv_dirs.append(root)
    log(msg=f"Converting [{len(csv_dirs)}] .csv directories.", verbosity=verbose, log_level=0)
    return csv_dirs

def convert_dir_ulog_csv(ulog_files: list[tuple[str,str]], output_dir: str, filter: dict, verbose: bool = False):
    """
    Converts a list of `.ulog` files to `.csv` files in parallel.

    Args:
    - ulog_files (list[str]): A list of tuples, where each tuple contains the file path and filename.
    - output_dir (str): The output directory for the converted `.csv` files.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.
    """

    processes: list[Process] = []
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


def convert_dir_csv_db3(csv_dirs: list[str], output_dir: str, topic_prefix: str, capitalise_topics: bool, verbose: bool = False):

    processes = []
    for dir in csv_dirs:
        process = Process(
            target=convert_csv2ros2bag,
            args=(
                dir,
                os.path.join(output_dir, dir),
                topic_prefix,
                capitalise_topics,
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


def adjust_topics(directory_address:str, filter:dict, verbose: bool = False):

    adjust_frequency: float = filter["bag_params"]["topic_max_frequency_hz"]
    csv_dirs: list[str] = get_csv_dirs(csv_dir = directory_address, verbose = verbose)
    
    processes: list[Process] = []
    for dir in csv_dirs:
        for filename in os.listdir(dir):
            if not filename.endswith(".csv"):
                continue
            filepath = os.path.join(dir, filename)

            process = Process(
                target=adjust_topic_rate,
                args=(filepath, adjust_frequency, verbose),
            )
            processes.append(process)
            process.start()
    i = 0
    total = len(processes)
    log("Topic Rate Adjustment Progress:", verbosity=verbose, log_level=0, bold=True)
    for process in processes:
        process.join()
        i += 1
        progress_bar(i / total, verbose)
    log("", verbosity=verbose, log_level=0, color=False, timestamped=False)

    return
