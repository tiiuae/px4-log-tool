---
title: Drone Flight Dataset
description: Includes real flights and SITL flight. Normal and anomalous.
---

<!--toc:start-->
- [Folder Structure](#folder-structure)
- [Data Conversion and Pre-Processing](#data-conversion-and-pre-processing)
  - [`filter.yaml`](#filteryaml)
  - [`.ulog` --> `.csv`(`/.db3``) -- `ulog_converter.py`](#ulog-csvdb3-ulogconverterpy)
    - [Usage](#usage)
    - [Arguments](#arguments)
    - [Workflow](#workflow)
    - [Resampling Functionality](#resampling-functionality)
<!--toc:end-->

# Folder Structure

📂 Data  
├─ 📂 hardware  
│  ├─ 📂 normal  
│  │  ├─ 📂 autonomous                                                                         
│  │  │  ├─ 📂 different_paths  
│  │  │  │  ├─ ❓ README.md  
│  │  │  │  ├─ 💾 `different_paths_0.ulog`  
│  │  │  │  └─ 💾 `<...>.ulog`    
│  │  │  │    
│  │  │  ├─ 📂 different_terrains  
│  │  │  │  ├─ ❓ README.md   
│  │  │  │  ├─ 💾 `different_terrains_0.ulog`  
│  │  │  │  └─ 💾 `<...>.ulog`    
│  │  │  │    
│  │  │  └─ 📂 linear_paths  
│  │  │     ├─ ❓ README.md   
│  │  │     ├─ 💾 `linear_paths_0.ulog`  
│  │  │     └─ 💾 `<...>.ulog`    
│  │  │  
│  │  ├─ 📂 gimbal_camera_video  
│  │  │  ├─ 📂 natsbags  
│  │  │  │  ├─ 💾 `different_paths_0.gz`  
│  │  │  │  └─ 💾 `<...>.gz`    
│  │  │  │    
│  │  │  └─ 📂 videos  
│  │  │     ├─ 💾 `different_paths_0.mp4`  
│  │  │     └─ 💾 `<...>.mp4`    
│  │  │  
│  │  └─ 📂 manual  
│  │     ├─ 📂 fast_movements  
│  │     │    ├─ ❓ README.md   
│  │     │    ├─ 💾 `fast_movements_0.ulog`  
│  │     │    └─ 💾 `<...>.ulog`    
│  │     │  
│  │     └─ 📂 takeoff_land  
│  │        ├─ ❓ README.md  
│  │        ├─ 💾 `takeoff_land_0.ulog`  
│  │        └─ 💾 `<...>.ulog`  
│  │  
│  └─ 📂 non_normal  
│  
├─ 📂 simulation  
│  ├─ 📂 faulty  
│  └─ 📂 normal  
│     
├─ `filter.yaml`  
├─ ❓ `README.md`  
└─ 🐍 `ulog_converter.py`  

# Data Conversion and Pre-Processing

## `filter.yaml`

Contains two lists for `whitelist_messages` and `blacklist headers`.

Add uorb/ros2 topics of interest into the `whitelist_messages` list.

Add headers that are redundant or not required in the `blacklist_headers` list.

`resample_params` contains parameters for resampling the data after it is merged. More on this is explained in [Resampling Functionality](#resampling-functionality). Provide the target sampling frequency in Hertz at `target_frequency_hz`.

For the other parameters, refer to the following documentation links of the `pandas` library:
* [`pandas.DataFrame.resample`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html)
* [`pandas.DataFrame.agg`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.agg.html)
* [`pandas.DataFrame.interpolate`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html)

```yaml
whitelist_messages:
    - sensor_combined
    - vehicle_attitude
    - ... (other message names)
blacklist_headers:
    - timestamp
    - ... (other field names)
resample_params:
    target_frequency_hz: 10
    num_method: "mean"
    cat_method: "ffill"
    interpolate_numerical: True
    interpolate_method: "linear"
```

## `.ulog` --> `.csv`(`/.db3``) -- `ulog_converter.py`

This script provides a streamlined way to process PX4 ULog files. It offers flexibility in converting individual files or merging multiple files, filtering specific messages, and resampling data.

### Usage

```bash
python <script_name> -h  # Display help and options
```
```bash
python <script_name> <ulog_dir> <filter.yaml> # Only runs conversion and output into `output_dir`
```
```bash
python <script_name> <ulog_dir> <filter.yaml> -b # After conversion, convert folders into bags .db3

```bash
python <script_name> <ulog_dir> <filter.yaml> -o <custom_output_dir> # Runs conversion and output in custom_output_dir
```
```bash
python <script_name> <ulog_dir> <filter.yaml> -o <output_dir> -m # Merges CSV files into a unified.csv file
```

### Arguments

* -ulog_dir (str): Path to the directory containing ULog files.
* -filter (str): Path to a YAML filter file specifying message whitelist and header blacklist.
* -o, --output_dir (str): Output directory for converted CSV files (default: 'output_dir').
* -m, --merge: Merge CSV files within each subdirectory into 'merged.csv' files.
* -r, --resample: Resample 'merged.csv' files based on parameters in 'filter.yaml'.
* -b, --rosbag: Convert each mission into a ROS 2 bag (sqlite / .db).
* -c, --clean: Clean up intermediate files, leaving only 'unified.csv'.
* -v, --verbose: Enable verbose logging.

### Workflow

1. **File Conversion:** Converts ULog files in the specified directory to individual CSV files and .db3 ROS 2 Bag files, applying filters from the YAML file.
2. **File Merging (Optional):** If the `-m` flag is set, merges CSV files within each subdirectory into a single 'merged.csv'.
3. **File Unification (Optional):** Combines all 'merged.csv' files into a single 'unified.csv' file.
4. **Cleanup (Optional):** If the `-c` flag is set, removes intermediate files and directories, leaving only 'unified.csv'.

### Resampling Functionality

As seen in [Arguments](README#Arguments) it is possible to resample the output 'unified.csv' to a predefined target frequency.

This is done by providing the `-r` flag:

```bash
python <script_name> <ulog_dir> <filter.yaml> -o <output_dir> -m -r # Resamples data according to provided filter params
```

Note that you NEED to have a 'filter.yaml' file with the provided params or else it will default to the parameters defined above.

An example of resampling is shown below for the 'SensorCombined' topic for the gyroscope readings, with default resampling params.


![Unsampled](./assets/unsampled.png)
![Sampled at 10Hz](./assets/sampled_10hz.png)
