"""
Tests for px4_log_tool/processing_modules/resampler.py
"""

import pytest
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df(n: int = 20, freq_us: int = 100_000) -> pd.DataFrame:
    """Return a small DataFrame with a 'timestamp' column in microseconds."""
    timestamps = np.arange(n) * freq_us  # 10 Hz -> 100_000 µs period
    return pd.DataFrame(
        {
            "timestamp": timestamps.astype(np.int64),
            "num_col": np.random.rand(n),
            "cat_col": np.random.choice(["a", "b"], size=n),
        }
    )


# ---------------------------------------------------------------------------
# resample_data
# ---------------------------------------------------------------------------


class TestResampleData:
    """Unit tests for resample_data."""

    def test_raises_without_column_lists(self):
        """Passing None for num/cat columns must raise ValueError."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df()
        with pytest.raises(ValueError, match="num_columns and cat_columns"):
            resample_data(df, target_frequency_hz=5.0)

    def test_raises_with_unidentified_columns(self):
        """A column not in num_columns or cat_columns must raise ValueError."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df()
        with pytest.raises(ValueError, match="neither numerical nor categorical"):
            resample_data(
                df,
                target_frequency_hz=5.0,
                num_columns=["num_col"],
                cat_columns=[],  # cat_col is missing -> unidentified
            )

    def test_output_has_timestamp_column(self):
        """The returned DataFrame must have a 'timestamp' column."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df()
        result = resample_data(
            df.copy(),
            target_frequency_hz=5.0,
            num_columns=["num_col"],
            cat_columns=["cat_col"],
        )
        assert "timestamp" in result.columns

    def test_output_is_dataframe(self):
        """resample_data must return a pandas DataFrame."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df()
        result = resample_data(
            df.copy(),
            target_frequency_hz=5.0,
            num_columns=["num_col"],
            cat_columns=["cat_col"],
        )
        assert isinstance(result, pd.DataFrame)

    def test_numerical_columns_preserved(self):
        """Numerical columns should survive resampling."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df()
        result = resample_data(
            df.copy(),
            target_frequency_hz=5.0,
            num_columns=["num_col"],
            cat_columns=["cat_col"],
        )
        assert "num_col" in result.columns

    def test_interpolation_no_nan_in_num(self):
        """After interpolation, the numerical column should have no NaN values."""
        from px4_log_tool.processing_modules.resampler import resample_data

        df = _make_df(n=30)
        result = resample_data(
            df.copy(),
            target_frequency_hz=5.0,
            num_columns=["num_col"],
            cat_columns=["cat_col"],
            interpolate_numerical=True,
            interpolate_method="linear",
        )
        assert result["num_col"].isna().sum() == 0


# ---------------------------------------------------------------------------
# adjust_topic_rate
# ---------------------------------------------------------------------------


class TestAdjustTopicRate:
    """Unit tests for adjust_topic_rate."""

    def test_downsamples_high_frequency_csv(self, tmp_path):
        """A 200 Hz CSV file must be downsampled to <=100 Hz."""
        from px4_log_tool.processing_modules.resampler import adjust_topic_rate

        n = 200
        freq_us = 5_000  # 200 Hz in µs
        timestamps = np.arange(n) * freq_us
        df = pd.DataFrame({"timestamp": timestamps, "value": np.random.rand(n)})
        csv_path = str(tmp_path / "topic.csv")
        df.to_csv(csv_path, index=False)

        adjust_topic_rate(csv_path, max_frequency=100.0)

        result = pd.read_csv(csv_path)
        assert len(result) <= n  # must have fewer rows
        assert len(result) >= 2  # must keep at least a couple

    def test_does_not_alter_low_frequency_csv(self, tmp_path):
        """A 10 Hz file should be left unchanged when max_frequency=100."""
        from px4_log_tool.processing_modules.resampler import adjust_topic_rate

        n = 20
        freq_us = 100_000  # 10 Hz
        timestamps = np.arange(n) * freq_us
        df = pd.DataFrame({"timestamp": timestamps, "value": np.ones(n)})
        csv_path = str(tmp_path / "slow.csv")
        df.to_csv(csv_path, index=False)
        original_len = len(df)

        adjust_topic_rate(csv_path, max_frequency=100.0)

        result = pd.read_csv(csv_path)
        assert len(result) == original_len

    def test_skips_file_with_single_row(self, tmp_path):
        """A file with fewer than 2 rows must not raise and must be skipped."""
        from px4_log_tool.processing_modules.resampler import adjust_topic_rate

        df = pd.DataFrame({"timestamp": [1_000_000], "value": [1.0]})
        csv_path = str(tmp_path / "single.csv")
        df.to_csv(csv_path, index=False)

        # Should not raise
        adjust_topic_rate(csv_path, max_frequency=100.0)

        result = pd.read_csv(csv_path)
        assert len(result) == 1
