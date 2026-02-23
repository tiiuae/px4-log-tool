import pandas as pd
import numpy as np
from typing import Any, Callable, Dict, List
from px4_log_tool.processing_modules.converter import convert_ulog2csv


def _calc_min_altitude(dataframe: pd.DataFrame) -> float:
    min_altitude: float = -1 * dataframe["z"].min()
    return min_altitude


def _calc_max_altitude(dataframe: pd.DataFrame) -> float:
    max_altitude: float = -1 * dataframe["z"].max()
    return max_altitude


def _calc_average_altitude(dataframe: pd.DataFrame) -> float:
    average_altitude: float = -1 * dataframe["z"].mean()
    return average_altitude


def _calc_min_speed(dataframe: pd.DataFrame) -> float:
    magnitudes: pd.Series = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    min_speed: float = magnitudes.min()
    return min_speed


def _calc_max_speed(dataframe: pd.DataFrame) -> float:
    magnitudes: pd.Series = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    max_speed: float = magnitudes.max()
    return max_speed


def _calc_average_speed(dataframe: pd.DataFrame) -> float:
    magnitudes: pd.Series = np.sqrt(
        dataframe["vx"] ** 2 + dataframe["vy"] ** 2 + dataframe["vz"] ** 2
    )
    average_speed: float = magnitudes.mean()
    return average_speed


def _calc_yaw_lock(dataframe: pd.DataFrame) -> bool:
    max_yaw: float = dataframe["heading"].max()
    min_yaw: float = dataframe["heading"].min()
    diff_yaw: float = float(max_yaw) - float(min_yaw)
    diff_yaw_degree: float = diff_yaw * 180 / np.pi
    yaw_lock: bool = diff_yaw_degree <= 5
    return yaw_lock


eval_metadata: Dict[str, Callable[[pd.DataFrame], Any]] = {
    "min_altitude": _calc_min_altitude,
    "max_altitude": _calc_max_altitude,
    "average_altitude": _calc_average_altitude,
    "min_speed": _calc_min_speed,
    "max_speed": _calc_max_speed,
    "average_speed": _calc_average_speed,
    "yaw_lock": _calc_yaw_lock,
}


def get_file_metadata(
    metadata_fields: List[str],
    directory_address: str,
    ulog_file_name: str,
) -> Dict[str, Any]:
    data_frame_dict: Dict[str, pd.DataFrame] = convert_ulog2csv(
        directory_address,
        ulog_file_name,
        messages=["vehicle_local_position"],
        output=f"./.cache/{ulog_file_name}",
    )
    metadata: Dict[str, Any] = {}
    for field in metadata_fields:
        metadata[field] = eval_metadata[field](
            data_frame_dict["vehicle_local_position"]
        )
    metadata["duration"] = (
        data_frame_dict["vehicle_local_position"]["timestamp"].max()
        - data_frame_dict["vehicle_local_position"]["timestamp"].min()
    ) / 1e6
    return metadata
