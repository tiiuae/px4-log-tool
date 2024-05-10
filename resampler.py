import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_battery_status(df, timestamp_column, battery_status_column):
    df = df.copy()
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='us')
    df.set_index(timestamp_column, inplace=True)

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(df.index, df[battery_status_column], marker='.', linestyle='-')
    plt.title('Battery Status Over Time')
    plt.xlabel('Time (seconds from start)')
    plt.ylabel('Battery Status Remaining (%)')
    plt.grid(True)
    plt.show()


def filter_first_ten_percent(df, timestamp_column):
    df = df.copy()  # Work with a copy to avoid SettingWithCopyWarning
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='us')

    df.set_index(timestamp_column, inplace=True)

    total_duration = df.index.max() - df.index.min()  # Correct total duration calculation
    end_time_first_10 = df.index.min() + 0.05 * total_duration  # Calculate the 10% endpoint

    # Filter the dataframe to the first 10% of the total time
    filtered_df = df[df.index <= end_time_first_10].reset_index()

    print("Total duration (seconds):", total_duration)
    print("End time for first 10% (seconds):", end_time_first_10)
    print("Number of rows before filtering:", len(df))
    print("Number of rows after filtering:", len(filtered_df))

    return filtered_df


def resample_data(df, target_frequency_hz, num_method='mean', cat_method='ffill', interpolate_numerical=False, interpolate_method='linear'):
    """
    Resamples the given DataFrame to the specified frequency in Hz, handling both numerical and categorical data,
    and allows for interpolation during downsampling and upsampling.

    Parameters:
    df (pd.DataFrame): DataFrame containing the data to resample.
    timestamp_column (str): Column name containing the timestamp data.
    target_frequency_hz (float): The target frequency for resampling in Hz.
    num_method (str, optional): The method to use for downsampling numerical data ('mean', 'median', 'max', 'min', 'sum'). Default is 'mean'.
    cat_method (str, optional): The method to use for downsampling categorical data ('ffill', 'bfill', 'mode'). Default is 'ffill'.
    interpolate_numerical (bool, optional): Whether to apply interpolation to numerical data after resampling. Default is False.
    interpolate_method (str, optional): The interpolation method to use if interpolation is enabled. Includes 'linear', 'nearest', 'spline', etc.

    Returns:
    pd.DataFrame: A DataFrame resampled to the target frequency.
    """
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')

    # Set the timestamp column as the index if it is not already
    if df.index.name != 'timestamp':
        df = df.set_index('timestamp')

    # Convert frequency from Hz to pandas offset
    milliseconds_per_sample = 1000 / target_frequency_hz
    pandas_freq = f'{round(milliseconds_per_sample)}L'

    # Determine column types
    numeric_cols = df.select_dtypes(include=[np.number]).columns # List[str] = [column names]
    categorical_cols = df.select_dtypes(exclude=[np.number]).columns

    # Resample numerical columns
    resampled_num = df[numeric_cols].resample(pandas_freq).agg(num_method)

    # Optionally interpolate numerical data
    if interpolate_numerical:
        resampled_num = resampled_num.interpolate(method=interpolate_method)

    # Resample categorical columns
    if cat_method == 'mode':
        # 'mode' is not directly supported in resample.agg, so use a custom function
        resampled_cat = df[categorical_cols].resample(pandas_freq).agg(lambda x: x.mode()[0] if not x.empty else np.nan)
    else:
        resampled_cat = df[categorical_cols].resample(pandas_freq).agg(cat_method)

    # Combine results
    resampled_df = pd.concat([resampled_num, resampled_cat], axis=1)

    # Reset index to return timestamp as a column
    resampled_df.reset_index(inplace=True)

    return resampled_df


# Load data
df = pd.read_csv('data.csv')

# Filter data for a specific mission
filtered_df = df[df['mission_name'] == 'different_terrains_4'].copy()

# Normalize timestamps to start from zero
min_time = filtered_df['timestamp'].min()
filtered_df['timestamp'] = filtered_df['timestamp'] - min_time

plot_battery_status(filtered_df, 'timestamp', 'BatteryStatus0_voltage_v')
# filtered_df = filter_first_ten_percent(filtered_df, 'timestamp')
# plot_battery_status(filtered_df, 'timestamp', 'BatteryStatus0_voltage_v')


resampled_df = resample_data(filtered_df, target_frequency_hz=1, num_method='mean', cat_method='ffill', interpolate_numerical=True)

plot_battery_status(resampled_df, 'timestamp', 'BatteryStatus0_voltage_v')
