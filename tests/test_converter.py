"""
Tests for px4_log_tool/processing_modules/converter.py

Mocks pyulog and file I/O so no real .ulog files are needed.
"""

import os
import sys
import pytest
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch, mock_open

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_ulog_mock(message_names=("sensor_combined",), n_rows=5):
    """Build a minimal ULog mock whose data_list looks realistic."""
    data_list = []
    for name in message_names:
        d = MagicMock()
        d.name = name
        d.multi_id = 0
        # field_data: list of objects with .field_name
        ts_field = MagicMock()
        ts_field.field_name = "timestamp"
        val_field = MagicMock()
        val_field.field_name = "gyro_rad_0"
        d.field_data = [ts_field, val_field]
        # data dict: numpy arrays
        timestamps = np.arange(n_rows, dtype=np.uint64) * 1_000_000
        d.data = {
            "timestamp": timestamps,
            "gyro_rad_0": np.random.rand(n_rows).astype(np.float32),
        }
        data_list.append(d)
    ulog = MagicMock()
    ulog.data_list = data_list
    return ulog


# ---------------------------------------------------------------------------
# convert_ulog2csv
# ---------------------------------------------------------------------------


class TestConvertUlog2Csv:
    """Unit tests for convert_ulog2csv."""

    def test_returns_dict_on_success(self, tmp_path):
        """Should return a non-empty dict mapping topic name -> DataFrame."""
        from px4_log_tool.processing_modules.converter import convert_ulog2csv

        ulog_mock = _make_ulog_mock()
        with patch(
            "px4_log_tool.processing_modules.converter.ULog", return_value=ulog_mock
        ):
            result = convert_ulog2csv(
                directory_address=str(tmp_path),
                ulog_file_name="test.ulog",
                messages=["sensor_combined"],
                output=str(tmp_path),
                verbose=False,
            )
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_returns_empty_dict_on_ulog_exception(self, tmp_path):
        """If ULog constructor raises, an empty dict must be returned."""
        from px4_log_tool.processing_modules.converter import convert_ulog2csv

        with patch(
            "px4_log_tool.processing_modules.converter.ULog",
            side_effect=Exception("corrupt file"),
        ):
            result = convert_ulog2csv(
                directory_address=str(tmp_path),
                ulog_file_name="bad.ulog",
                verbose=False,
            )
        assert result == {}

    def test_blacklist_removes_fields(self, tmp_path):
        """Fields in the blacklist must not appear in the CSV output."""
        from px4_log_tool.processing_modules.converter import convert_ulog2csv

        ulog_mock = _make_ulog_mock()
        with patch(
            "px4_log_tool.processing_modules.converter.ULog", return_value=ulog_mock
        ):
            result = convert_ulog2csv(
                directory_address=str(tmp_path),
                ulog_file_name="test.ulog",
                messages=["sensor_combined"],
                output=str(tmp_path),
                blacklist=["gyro_rad_0"],
                verbose=False,
            )
        # If a key exists, the column should not contain 'gyro_rad'
        for df in result.values():
            assert "gyro_rad_0" not in df.columns

    def test_output_csv_files_created(self, tmp_path):
        """At least one .csv file should be written inside output_file_prefix dir."""
        from px4_log_tool.processing_modules.converter import convert_ulog2csv

        ulog_mock = _make_ulog_mock()
        with patch(
            "px4_log_tool.processing_modules.converter.ULog", return_value=ulog_mock
        ):
            convert_ulog2csv(
                directory_address=str(tmp_path),
                ulog_file_name="test.ulog",
                messages=["sensor_combined"],
                output=str(tmp_path),
                verbose=False,
            )
        csv_files = list(tmp_path.rglob("*.csv"))
        assert len(csv_files) >= 1
