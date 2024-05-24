import pandas as pd
from typing import List

def resample_data(
        df: pd.DataFrame,
        target_frequency_hz: float,
        num_method: str = "mean",
        cat_method: str = "last",
        interpolate_numerical: bool = True,
        interpolate_method: str = "linear",
        num_columns: List[str] = None,
        cat_columns: List[str] = None,
        verbose: bool = False,
) -> pd.DataFrame:
    """
    Resamples a DataFrame to a specified frequency, handling numerical and categorical data.

    This function resamples the input DataFrame to the given target frequency in Hz. It allows
    different aggregation methods for numerical and categorical columns and provides an option
    for interpolating numerical data after resampling.

    Args:
        df: The DataFrame containing the data to resample. The DataFrame must have a
            'timestamp' column with datetime values.
        target_frequency_hz: The target resampling frequency in Hz.
        num_method: The method to use for downsampling numerical data ('mean', 'median', 'max', 'min', 'sum'). This is only applied if `interpolate_numerical = False`
            Defaults to 'mean'.
        cat_method: The method to use for downsampling categorical data ('ffill', 'bfill', 'mode').
            Defaults to 'ffill'.
        interpolate_numerical: Whether to interpolate numerical data after resampling. Defaults to False.
        interpolate_method: The interpolation method to use if interpolation is enabled (e.g., 'linear', 'nearest', 'spline').
            Defaults to 'linear'.
        num_columns: (Optional) List of numerical column labels to resample. If None, all numerical columns are resampled.
        cat_columns: (Optional) List of categorical column labels to resample. If None, all categorical columns are resampled.
        verbose: (Optional) Set to True if verbose output is desired in sampling.

    Returns:
        The resampled DataFrame with the 'timestamp' column reset as a regular column.
    """
    # Validate that num_columns and cat_columns are provided
    if num_columns is None or cat_columns is None:
        raise ValueError("Both num_columns and cat_columns must be provided and cannot be None.")

    # Find any columns that are neither in num_columns nor in cat_columns, including 'timestamp' as a recognized column
    all_labels = set(num_columns + cat_columns + ['timestamp'])
    unidentified_cols = [col for col in df.columns if col not in all_labels]

    # If there are unidentified columns, raise an error and print them
    if unidentified_cols:
        raise ValueError(f"There are columns in the dataframe which are flagged as neither numerical nor categorical: {unidentified_cols}")

    # Convert the target frequency from Hz to a pandas frequency string
    milliseconds_per_sample = 1000 / target_frequency_hz
    pandas_freq = f"{round(milliseconds_per_sample)}L"

    # Set the timestamp column as the index if it is not already
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
    if df.index.name != "timestamp":
        df = df.set_index("timestamp")

    # TODO: check verbose true
    if verbose:
        print_column_frequencies(df)

    # Resample the entire DataFrame using a simple aggregation initially
    df_resampled = df.resample(pandas_freq).last()  # Use 'last' to initially preserve data structure

    # Apply specific methods for numerical data
    df_num = df_resampled[num_columns]
    if interpolate_numerical:
        df_num = df_num.interpolate(method=interpolate_method)
    else:
        df_num = df_num.apply(lambda x: x.resample(pandas_freq).agg(num_method))

    # Apply specific methods for categorical data
    df_cat = df_resampled[cat_columns]
    df_cat = df_cat.fillna(method=cat_method)

    # Concatenate numerical and categorical data back together
    df_final = pd.concat([df_num, df_cat], axis=1)
    df_final = df_final.ffill().bfill()  # ensure that no new NaNs are introduced

    # Ensure df_final has the timestamp in a column
    df_final.reset_index(inplace=True)

    return df_final


def print_column_frequencies(df):
    # Ensure the 'timestamp' column is sorted
    if not df.index.is_monotonic_increasing:
        raise ValueError("Timestamp column must be sorted in ascending order")

    # Initialize a dictionary to store frequencies of each column
    frequency_dict = {}
    overall_mean_frequencies = []

    for column in df.columns:
        # Filter out NaN values
        valid_df = df[[column]].dropna()

        # Skip empty columns
        if valid_df.empty:
            continue

        # Calculate time intervals between valid timestamps
        intervals = valid_df.index.to_series().diff().dt.total_seconds().dropna()

        # Calculate mean interval and frequency
        mean_interval = intervals.mean()
        if mean_interval == 0:
            frequency_dict[column] = float('inf')  # Infinite frequency if intervals are zero
        else:
            frequency_dict[column] = 1 / mean_interval

        # Collect mean frequencies for overall calculation
        overall_mean_frequencies.append(1 / mean_interval)

    # Print frequencies of each column
    for col, freq in frequency_dict.items():
        print(f"Frequency of {col}: {freq} Hz")

    # Identify the column with the highest and lowest frequency
    max_freq_col = max(frequency_dict, key=frequency_dict.get)
    min_freq_col = min(frequency_dict, key=frequency_dict.get)
    print(f"Highest frequency column: {max_freq_col} with {frequency_dict[max_freq_col]} Hz")
    print(f"Lowest frequency column: {min_freq_col} with {frequency_dict[min_freq_col]} Hz")

    # Calculate and print overall mean frequency
    overall_mean_frequency = sum(overall_mean_frequencies) / len(overall_mean_frequencies) if overall_mean_frequencies else 0
    print(f"Overall mean frequency of DataFrame: {overall_mean_frequency} Hz")

    return frequency_dict


