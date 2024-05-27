import pandas as pd
import os
from typing import List

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
        root: The directory path containing the CSV files to merge.
        files: A list of filenames within the 'root' directory.

    Returns:
        None. The merged and potentially resampled DataFrame is saved as 'merged.csv' in the 'root' directory.
    """

    merged_df = pd.DataFrame(data={"timestamp": []})
    for file in files:
        if file == "merged.csv":
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