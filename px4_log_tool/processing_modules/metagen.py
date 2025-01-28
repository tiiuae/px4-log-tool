import pandas as pd
import numpy as np
from px4_log_tool.processing_modules.converter import convert_ulog2csv


def _calc_min_altitude(dataframe: pd.DataFrame) -> float:
    min_altitude: float = 0.0
    min_altitude = -1 * dataframe["z"].min()
    return min_altitude


def _calc_max_altitude(dataframe: pd.DataFrame) -> float:
    max_altitude: float = 0.0
    max_altitude = -1 * dataframe["z"].max()
    return max_altitude


def _calc_average_altitude(dataframe: pd.DataFrame) -> float:
    average_altitude: float = 0.0
    average_altitude = -1 * dataframe["z"].mean()
    return average_altitude


def _calc_min_speed(dataframe: pd.DataFrame) -> float:
    min_speed: float = 0.0
    magnitudes = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    min_speed = magnitudes.min()
    return min_speed


def _calc_max_speed(dataframe: pd.DataFrame) -> float:
    max_speed: float = 0.0
    magnitudes = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    max_speed = magnitudes.max()
    return max_speed


def _calc_average_speed(dataframe: pd.DataFrame) -> float:
    average_speed: float = 0.0
    magnitudes = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    average_speed = magnitudes.mean()
    return average_speed


def _calc_yaw_lock(dataframe: pd.DataFrame) -> bool:
    yaw_lock: bool = False
    max_yaw = dataframe["heading"].max()
    min_yaw = dataframe["heading"].min()
    diff_yaw = float(max_yaw) - float(min_yaw)
    diff_yaw_degree: float = diff_yaw * 180 / np.pi
    yaw_lock = diff_yaw_degree <= 5
    return yaw_lock


eval_metadata = {
    "min_altitude": _calc_min_altitude,
    "max_altitude": _calc_max_altitude,
    "average_altitude": _calc_average_altitude,
    "min_speed": _calc_min_speed,
    "max_speed": _calc_max_speed,
    "average_speed": _calc_average_speed,
    "yaw_lock": _calc_yaw_lock,
}


def get_file_metadata(metadata_fields: list, directory_address: str, ulog_file_name: str):
    data_frame_dict = convert_ulog2csv(
        directory_address,
        ulog_file_name,
        messages=["vehicle_local_position"],
        output=f"./.cache/{ulog_file_name}",
    )
    metadata = {}
    for field in metadata_fields:
        metadata[field] = eval_metadata[field](
            data_frame_dict["vehicle_local_position"]
        )
    metadata["duration"] = (
        data_frame_dict["vehicle_local_position"]["timestamp"].max()
        - data_frame_dict["vehicle_local_position"]["timestamp"].min()
    ) / 1e6
    return metadata
