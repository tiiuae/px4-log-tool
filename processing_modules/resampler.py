import warnings
from typing import List

import pandas as pd


def merge_csv(
        root: str,
        files: List[str],
) -> None:
    """
    Merges multiple CSV files in a directory, handling column renaming and resampling.

    This function merges CSV files found in the specified directory into a single 'merged.csv' file.
    Column names are intelligently renamed to avoid conflicts, and a 'mission_name' column is added
    to identify the source directory. Optionally, the merged data can be resampled based on
    parameters provided in `sampling_params`.

    Args:
<<<<<<< HEAD
        root: The directory path containing the CSV files to merge.
        files: A list of filenames within the 'root' directory.
=======
        df: The DataFrame containing the data to resample. The DataFrame must have a
            'timestamp' column with datetime values.
        target_frequency_hz: The target resampling frequency in Hz.
        num_method: The method to use for downsampling numerical data ('mean', 'median', 'max', 'min', 'sum'). This is only applied if `interpolate_numerical = False`
            Defaults to 'mean'.
        cat_method: The method to use for downsampling categorical data ('ffill', 'bfill', 'mode').
            Defaults to 'last'.
        interpolate_numerical: Whether to interpolate numerical data after resampling. Defaults to False.
        interpolate_method: The interpolation method to use if interpolation is enabled (e.g., 'linear', 'nearest', 'spline').
            Defaults to 'linear'.
        num_columns: (Optional) List of numerical column labels to resample. If None, all numerical columns are resampled.
        cat_columns: (Optional) List of categorical column labels to resample. If None, all categorical columns are resampled.
        verbose: (Optional) Set to True if verbose output is desired in sampling.
>>>>>>> 3f2b005c46c415a6032a2e6fedbffd9377098831

    Returns:
        None. The merged and potentially resampled DataFrame is saved as 'merged.csv' in the 'root' directory.
    """

<< << << < HEAD
merged_df = pd.DataFrame(data={"timestamp": []})
for file in files:
    if file == "merged.csv":
== == == =
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
with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning)
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

>> >> >> > 3
f2b005c46c415a6032a2e6fedbffd9377098831
continue

data_frame: pd.DataFrame = pd.read_csv(os.path.join(root, file))

prefix_parts = file.split("_")
capitalised_prefix_parts = [prefix_parts[0].capitalize()] + [
    part.capitalize() for part in prefix_parts[1:]
]
joined_prefix = "".join(capitalised_prefix_parts)
joined_prefix = joined_prefix.split(".")[0]

column_names = data_frame.columns
column_names = ["timestamp"] + [
    f"{joined_prefix}_{name}"
    for name in column_names[column_names != "timestamp"]
]

data_frame.rename(
    columns=dict(zip(data_frame.columns, column_names)), inplace=True
)

merged_df = pd.merge(merged_df, data_frame, on="timestamp", how="outer")

# This gets you the label
merged_df.sort_values(by="timestamp", inplace=True)

merged_df["mission_name"] = os.path.basename(root)

preamble = ["mission_name", "timestamp"]
body = sorted([col for col in merged_df.columns if col not in preamble])
merged_df = merged_df[preamble + body]

merged_df.to_csv(os.path.join(root, "merged.csv"), index=False)

# blacklist = ["timestamp_sample", "device_id"]

# def merge_csv_files(directory, silent_prefix=False, output="merged.csv"):
#     global blacklist
#
#     csv_files = [file for file in os.listdir(directory) if file.endswith(".csv")]
#
#     if not csv_files:
#         print(f"No CSV files found in {directory}")
#         return
#
#     dfs = []
#     for csv_file in csv_files:
#
#         file_path = os.path.join(directory, csv_file)
#         # Read CSV file
#         df = pd.read_csv(file_path)
#         for black in blacklist:
#             try:
#                 df.pop(black)
#             except KeyError:
#                 pass
#
#         if not silent_prefix:
#             prefix = f"{csv_file[:-4]}_"
#             new_headers = [prefix + col for col in df.columns if col != "timestamp"]
#             new_headers = ["timestamp"] + new_headers
#             df.rename(columns=dict(zip(df.columns, new_headers)), inplace=True)
#         dfs.append(df)
#
#     if not dfs:
#         print(f"No valid CSV files found in {directory}")
#         return
#
#     # Merge dataframes using left join to preserve all timestamps
#     merged_df = dfs[0]
#     for df in dfs[1:]:
#         merged_df = pd.merge(merged_df, df, on="timestamp", how="outer")
#
#     merged_df.sort_values(by="timestamp", inplace=True)
#
#     merged_file_path = os.path.join(directory, output)
#     merged_df.to_csv(merged_file_path, index=False)
#     print(f"Merged and sorted CSV file saved to {merged_file_path}")
