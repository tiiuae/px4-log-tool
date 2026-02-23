"""
Tests for px4_log_tool/processing_modules/metagen.py

pyulog is mocked so no real .ulog files are needed.
"""

import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vlp_df(n: int = 10) -> pd.DataFrame:
    """Return a mock vehicle_local_position-like DataFrame."""
    t = np.arange(n, dtype=np.float64) * 1e6  # µs
    return pd.DataFrame(
        {
            "timestamp": t,
            "x": np.ones(n),
            "y": np.ones(n),
            "z": -np.linspace(10, 50, n),  # negative z == altitude above ground
            "vx": np.ones(n) * 5,
            "vy": np.zeros(n),
            "vz": np.zeros(n),
            "heading": np.full(n, 1.5),  # constant → yaw_lock=True
        }
    )


# ---------------------------------------------------------------------------
# Private helper functions
# ---------------------------------------------------------------------------


class TestPrivateHelpers:
    def test_calc_max_altitude(self):
        from px4_log_tool.processing_modules.metagen import _calc_max_altitude

        df = _make_vlp_df()
        # z ranges from -10 to -50 (negative convention).
        # _calc_max_altitude = -1 * z.max() = -1 * (-10) = 10
        assert _calc_max_altitude(df) == pytest.approx(10.0)

    def test_calc_min_altitude(self):
        from px4_log_tool.processing_modules.metagen import _calc_min_altitude

        df = _make_vlp_df()
        # _calc_min_altitude = -1 * z.min() = -1 * (-50) = 50
        assert _calc_min_altitude(df) == pytest.approx(50.0)

    def test_calc_average_altitude(self):
        from px4_log_tool.processing_modules.metagen import _calc_average_altitude

        df = _make_vlp_df()
        expected = np.linspace(10, 50, 10).mean()
        assert _calc_average_altitude(df) == pytest.approx(expected)

    def test_calc_min_speed(self):
        from px4_log_tool.processing_modules.metagen import _calc_min_speed

        df = _make_vlp_df()
        # vx=5, vy=0, vz=0 → speed = 5 for all rows
        assert _calc_min_speed(df) == pytest.approx(5.0)

    def test_calc_max_speed(self):
        from px4_log_tool.processing_modules.metagen import _calc_max_speed

        df = _make_vlp_df()
        assert _calc_max_speed(df) == pytest.approx(5.0)

    def test_calc_average_speed(self):
        from px4_log_tool.processing_modules.metagen import _calc_average_speed

        df = _make_vlp_df()
        assert _calc_average_speed(df) == pytest.approx(5.0)

    def test_calc_yaw_lock_constant_heading(self):
        from px4_log_tool.processing_modules.metagen import _calc_yaw_lock

        df = _make_vlp_df()  # heading is constant => yaw_lock=True
        assert _calc_yaw_lock(df) is True

    def test_calc_yaw_lock_varying_heading(self):
        from px4_log_tool.processing_modules.metagen import _calc_yaw_lock

        df = _make_vlp_df()
        # Spread heading over a large range
        df["heading"] = np.linspace(0, np.pi, len(df))  # 180° swing
        assert _calc_yaw_lock(df) is False


# ---------------------------------------------------------------------------
# get_file_metadata
# ---------------------------------------------------------------------------


class TestGetFileMetadata:
    def test_returns_all_requested_fields(self, tmp_path):
        """get_file_metadata should return a dict containing every requested field."""
        from px4_log_tool.processing_modules.metagen import get_file_metadata

        vlp_df = _make_vlp_df()

        with patch(
            "px4_log_tool.processing_modules.metagen.convert_ulog2csv",
            return_value={"vehicle_local_position": vlp_df},
        ):
            metadata = get_file_metadata(
                metadata_fields=["max_altitude", "min_speed"],
                directory_address=str(tmp_path),
                ulog_file_name="flight.ulog",
            )

        assert "max_altitude" in metadata
        assert "min_speed" in metadata
        assert "duration" in metadata

    def test_duration_is_positive(self, tmp_path):
        from px4_log_tool.processing_modules.metagen import get_file_metadata

        vlp_df = _make_vlp_df(n=10)

        with patch(
            "px4_log_tool.processing_modules.metagen.convert_ulog2csv",
            return_value={"vehicle_local_position": vlp_df},
        ):
            metadata = get_file_metadata(
                metadata_fields=[],
                directory_address=str(tmp_path),
                ulog_file_name="flight.ulog",
            )

        assert metadata["duration"] > 0
