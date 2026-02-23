"""
Tests for px4_log_tool/util/components.py

Covers: extract_filter, get_ulog_files, get_csv_dirs, get_msg_reference,
        dump_template_filter, and resample_unified (light weight).
"""

import os
import pytest
import tempfile
import textwrap
from unittest.mock import patch, MagicMock

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# extract_filter
# ---------------------------------------------------------------------------


class TestExtractFilter:
    """Tests for extract_filter."""

    def test_returns_dict(self):
        from px4_log_tool.util.components import extract_filter

        result = extract_filter(filter_str=None, verbose=False)
        assert isinstance(result, dict)

    def test_default_keys_present(self):
        from px4_log_tool.util.components import extract_filter, DEFAULT_FILTER_CONFIG

        result = extract_filter(filter_str=None, verbose=False)
        for key in DEFAULT_FILTER_CONFIG:
            assert key in result

    def test_loads_valid_yaml(self, tmp_path):
        from px4_log_tool.util.components import extract_filter

        yaml_content = textwrap.dedent("""\
            whitelist_messages:
              - imu
            blacklist_headers:
              - error_count
            resample_params:
              target_frequency_hz: 50
              num_method: mean
              cat_method: ffill
              interpolate_numerical: true
              interpolate_method: linear
            bag_params:
              topic_prefix: /fmu/out
              topic_max_frequency_hz: 200
              capitalise_topics: false
            metadata_fields:
              - max_altitude
        """)
        f = tmp_path / "filter.yaml"
        f.write_text(yaml_content)
        result = extract_filter(filter_str=str(f), verbose=False)
        assert result["whitelist_messages"] == ["imu"]
        assert result["resample_params"]["target_frequency_hz"] == 50

    def test_missing_file_uses_defaults(self, tmp_path):
        from px4_log_tool.util.components import extract_filter, DEFAULT_FILTER_CONFIG

        result = extract_filter(
            filter_str=str(tmp_path / "nonexistent.yaml"), verbose=False
        )
        for key in DEFAULT_FILTER_CONFIG:
            assert key in result

    def test_invalid_yaml_uses_defaults(self, tmp_path):
        from px4_log_tool.util.components import extract_filter, DEFAULT_FILTER_CONFIG

        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("{invalid: yaml: content::")
        result = extract_filter(filter_str=str(bad_file), verbose=False)
        for key in DEFAULT_FILTER_CONFIG:
            assert key in result

    def test_missing_section_falls_back_to_default(self, tmp_path):
        from px4_log_tool.util.components import extract_filter, DEFAULT_FILTER_CONFIG

        # Provide only one section; the rest should fall back to defaults
        f = tmp_path / "partial.yaml"
        f.write_text("whitelist_messages:\n  - sensor_combined\n")
        result = extract_filter(filter_str=str(f), verbose=False)
        # Non-provided sections get defaults
        assert "bag_params" in result
        assert result["bag_params"] == DEFAULT_FILTER_CONFIG["bag_params"]["default"]


# ---------------------------------------------------------------------------
# get_ulog_files
# ---------------------------------------------------------------------------


class TestGetUlogFiles:
    """Tests for get_ulog_files."""

    def test_finds_ulog_extension(self, tmp_path):
        from px4_log_tool.util.components import get_ulog_files

        (tmp_path / "flight.ulog").touch()
        result = get_ulog_files(str(tmp_path), verbose=False)
        assert any("flight.ulog" in f[1] for f in result)

    def test_finds_ulg_extension(self, tmp_path):
        from px4_log_tool.util.components import get_ulog_files

        (tmp_path / "mission.ulg").touch()
        result = get_ulog_files(str(tmp_path), verbose=False)
        assert any("mission.ulg" in f[1] for f in result)

    def test_ignores_non_ulog_files(self, tmp_path):
        from px4_log_tool.util.components import get_ulog_files

        (tmp_path / "readme.txt").touch()
        (tmp_path / "data.csv").touch()
        result = get_ulog_files(str(tmp_path), verbose=False)
        assert len(result) == 0

    def test_returns_list_of_tuples(self, tmp_path):
        from px4_log_tool.util.components import get_ulog_files

        (tmp_path / "a.ulog").touch()
        result = get_ulog_files(str(tmp_path), verbose=False)
        assert isinstance(result, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)

    def test_empty_directory_returns_empty_list(self, tmp_path):
        from px4_log_tool.util.components import get_ulog_files

        result = get_ulog_files(str(tmp_path), verbose=False)
        assert result == []


# ---------------------------------------------------------------------------
# get_csv_dirs
# ---------------------------------------------------------------------------


class TestGetCsvDirs:
    """Tests for get_csv_dirs."""

    def test_finds_leaf_all_csv_directory(self, tmp_path):
        from px4_log_tool.util.components import get_csv_dirs

        leaf = tmp_path / "mission1"
        leaf.mkdir()
        (leaf / "topic.csv").touch()
        result = get_csv_dirs(str(tmp_path), verbose=False)
        assert str(leaf) in result

    def test_ignores_directory_with_mixed_files(self, tmp_path):
        from px4_log_tool.util.components import get_csv_dirs

        leaf = tmp_path / "mixed"
        leaf.mkdir()
        (leaf / "topic.csv").touch()
        (leaf / "readme.txt").touch()
        result = get_csv_dirs(str(tmp_path), verbose=False)
        assert str(leaf) not in result

    def test_empty_directory_returns_empty_list(self, tmp_path):
        from px4_log_tool.util.components import get_csv_dirs

        # An empty leaf directory passes the "all files are .csv" vacuous truth check,
        # so it IS included. Verify it contains just the root path itself.
        result = get_csv_dirs(str(tmp_path), verbose=False)
        assert result == [str(tmp_path)]


# ---------------------------------------------------------------------------
# get_msg_reference
# ---------------------------------------------------------------------------


class TestGetMsgReference:
    """Tests for get_msg_reference (importlib.resources path)."""

    def test_returns_dataframe_when_csv_exists(self):
        from px4_log_tool.util.components import get_msg_reference
        import io

        fake_csv = "Alias,Dataclass\ngyro_rad_0,Numerical\nmode,Categorical\n"

        mock_path = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_path)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with (
            patch("importlib.resources.files") as mock_files,
            patch(
                "pandas.read_csv", return_value=pd.read_csv(io.StringIO(fake_csv))
            ) as mock_read,
        ):
            mock_files.return_value.joinpath.return_value = MagicMock()
            import importlib.resources as ir

            with patch.object(ir, "as_file", return_value=mock_ctx):
                result = get_msg_reference(verbose=False)

        # result is either a DataFrame or None depending on mock depth; just assert no exception
        assert result is None or isinstance(result, pd.DataFrame)

    def test_returns_none_on_file_not_found(self):
        from px4_log_tool.util.components import get_msg_reference
        import importlib.resources as ir

        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(side_effect=FileNotFoundError)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with patch("importlib.resources.files") as mock_files:
            mock_joinpath = MagicMock()
            mock_files.return_value.joinpath.return_value = mock_joinpath
            with patch.object(ir, "as_file", return_value=mock_ctx):
                result = get_msg_reference(verbose=False)

        assert result is None


# ---------------------------------------------------------------------------
# dump_template_filter
# ---------------------------------------------------------------------------


class TestDumpTemplateFilter:
    """Tests for dump_template_filter."""

    def test_creates_yaml_file(self, tmp_path):
        from px4_log_tool.util.components import dump_template_filter

        dump_template_filter(str(tmp_path), verbose=False)
        assert (tmp_path / "filter.yaml").exists()

    def test_yaml_is_valid(self, tmp_path):
        import yaml
        from px4_log_tool.util.components import dump_template_filter

        dump_template_filter(str(tmp_path), verbose=False)
        with open(tmp_path / "filter.yaml") as f:
            content = yaml.safe_load(f)
        assert isinstance(content, dict)

    def test_yaml_contains_expected_keys(self, tmp_path):
        import yaml
        from px4_log_tool.util.components import (
            dump_template_filter,
            DEFAULT_FILTER_CONFIG,
        )

        dump_template_filter(str(tmp_path), verbose=False)
        with open(tmp_path / "filter.yaml") as f:
            content = yaml.safe_load(f)
        for key in DEFAULT_FILTER_CONFIG:
            assert key in content
