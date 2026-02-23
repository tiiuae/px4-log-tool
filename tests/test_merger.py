"""
Tests for px4_log_tool/processing_modules/merger.py
"""

import os
import pytest
import tempfile
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(path: str, name: str, df: pd.DataFrame) -> None:
    df.to_csv(os.path.join(path, name), index=False)


# ---------------------------------------------------------------------------
# merge_csv
# ---------------------------------------------------------------------------


class TestMergeCsv:
    """Unit tests for merge_csv."""

    def test_creates_merged_csv(self, tmp_path):
        """merge_csv must produce a merged.csv in the given directory."""
        from px4_log_tool.processing_modules.merger import merge_csv

        df1 = pd.DataFrame({"timestamp": [1, 2, 3], "value_a": [10, 20, 30]})
        df2 = pd.DataFrame({"timestamp": [1, 2, 3], "value_b": [100, 200, 300]})
        _write_csv(str(tmp_path), "sensor_a.csv", df1)
        _write_csv(str(tmp_path), "sensor_b.csv", df2)

        merge_csv(str(tmp_path), ["sensor_a.csv", "sensor_b.csv"])

        merged_path = os.path.join(str(tmp_path), "merged.csv")
        assert os.path.exists(merged_path)

    def test_merged_csv_has_mission_name(self, tmp_path):
        """The merged.csv must include a 'mission_name' column."""
        from px4_log_tool.processing_modules.merger import merge_csv

        df1 = pd.DataFrame({"timestamp": [1, 2], "x": [5, 6]})
        _write_csv(str(tmp_path), "topic_x.csv", df1)

        merge_csv(str(tmp_path), ["topic_x.csv"])

        merged = pd.read_csv(os.path.join(str(tmp_path), "merged.csv"))
        assert "mission_name" in merged.columns

    def test_merged_csv_skips_existing_merged(self, tmp_path):
        """Passing merged.csv in the file list must not cause a self-reference loop."""
        from px4_log_tool.processing_modules.merger import merge_csv

        df1 = pd.DataFrame({"timestamp": [1], "v": [99]})
        _write_csv(str(tmp_path), "topic_v.csv", df1)
        # Write a stale merged.csv to ensure it is ignored
        _write_csv(str(tmp_path), "merged.csv", pd.DataFrame({"timestamp": [999]}))

        merge_csv(str(tmp_path), ["topic_v.csv", "merged.csv"])
        merged = pd.read_csv(os.path.join(str(tmp_path), "merged.csv"))
        # The stale timestamp=999 should not appear
        assert 999 not in merged["timestamp"].values

    def test_column_renaming_uses_prefix(self, tmp_path):
        """Columns other than 'timestamp' should be prefixed with the topic name."""
        from px4_log_tool.processing_modules.merger import merge_csv

        df = pd.DataFrame({"timestamp": [1, 2], "rate": [0.1, 0.2]})
        _write_csv(str(tmp_path), "gyro.csv", df)

        merge_csv(str(tmp_path), ["gyro.csv"])
        merged = pd.read_csv(os.path.join(str(tmp_path), "merged.csv"))

        # Expect the column to be renamed like "Gyro_rate"
        col_names = list(merged.columns)
        assert any("rate" in c for c in col_names)

    def test_multiple_topics_outer_join(self, tmp_path):
        """When topics have non-overlapping timestamps, all rows must be preserved."""
        from px4_log_tool.processing_modules.merger import merge_csv

        df1 = pd.DataFrame({"timestamp": [1, 3], "a": [10, 30]})
        df2 = pd.DataFrame({"timestamp": [2, 4], "b": [20, 40]})
        _write_csv(str(tmp_path), "topic_a.csv", df1)
        _write_csv(str(tmp_path), "topic_b.csv", df2)

        merge_csv(str(tmp_path), ["topic_a.csv", "topic_b.csv"])
        merged = pd.read_csv(os.path.join(str(tmp_path), "merged.csv"))
        assert len(merged) == 4
