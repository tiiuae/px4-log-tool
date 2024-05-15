import argparse
import os
import pandas as pd
import yaml

blacklist = ["timestamp_sample", "device_id"]


def merge_csv_files(directory, silent_prefix=False, output="merged.csv"):
    global blacklist

    csv_files = [file for file in os.listdir(directory) if file.endswith(".csv")]

    if not csv_files:
        print(f"No CSV files found in {directory}")
        return

    dfs = []
    for csv_file in csv_files:

        file_path = os.path.join(directory, csv_file)
        # Read CSV file
        df = pd.read_csv(file_path)
        for black in blacklist:
            try:
                df.pop(black)
            except KeyError:
                pass

        if not silent_prefix:
            prefix = f"{csv_file[:-4]}_"
            new_headers = [prefix + col for col in df.columns if col != "timestamp"]
            new_headers = ["timestamp"] + new_headers
            df.rename(columns=dict(zip(df.columns, new_headers)), inplace=True)
        dfs.append(df)

    if not dfs:
        print(f"No valid CSV files found in {directory}")
        return

    # Merge dataframes using left join to preserve all timestamps
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on="timestamp", how="outer")

    merged_df.sort_values(by="timestamp", inplace=True)

    merged_file_path = os.path.join(directory, output)
    merged_df.to_csv(merged_file_path, index=False)
    print(f"Merged and sorted CSV file saved to {merged_file_path}")

def main():
    global blacklist

    parser = argparse.ArgumentParser(
        description="Merge all CSV files inside provided directory"
    )
    parser.add_argument(
        "-d",
        "--directory",
        help="Path to the directory containing CSV files to be merged",
        required=True,
        default=None,
    )
    parser.add_argument(
        "-b",
        "--blacklist",
        help=".yml file containing a list of columns to be removed",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-s",
        "--silent",
        help="Do not add prefixes to column names",
        required=False,
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Name of output CSV file (default: merged.csv)",
        required=False,
        default="merged.csv",
    )

    args = parser.parse_args()
    directory = args.directory

    if args.blacklist is not None:
        with open(args.blacklist, "r") as f:
            blacklist = yaml.safe_load(f)["blacklist"]

    merge_csv_files(directory, args.silent, args.output)

if __name__ == "__main__":
    main()
