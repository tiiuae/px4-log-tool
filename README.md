---
title: Drone Flight Dataset
description: Includes real flights and SITL flight. Normal and anomalous.
---

<!--toc:start-->
- [Folder Structure](#folder-structure)
- [Data Conversion and Pre-Processing](#data-conversion-and-pre-processing)
  - [`filter.yaml`](#filteryaml)
  - [`.ulog` --> `.csv` -- `ulog_converter.py`](#ulog-csv-ulogconverterpy)
    - [Usage](#usage)
    - [Arguments](#arguments)
    - [Workflow](#workflow)
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
    interpolate_numerical: False
    interpolate_method: "linear"
```

## `.ulog` --> `.csv` -- `ulog_converter.py`

This script provides a streamlined way to process PX4 ULog files. It offers flexibility in converting individual files or merging multiple files, filtering specific messages, and resampling data.

### Usage

```bash
python <script_name> -h  # Display help and options
```
```bash
python <script_name> <ulog_dir> <filter.yaml> # Only runs conversion and output into `output_dir`
```
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
* -c, --clean: Clean up intermediate files, leaving only 'unified.csv'.
* -v, --verbose: Enable verbose logging.

### Workflow

1. **File Conversion:** Converts ULog files in the specified directory to individual CSV files, applying filters from the YAML file.
2. **File Merging (Optional):** If the `-m` flag is set, merges CSV files within each subdirectory into a single 'merged.csv'.
3. **File Unification (Optional):** Combines all 'merged.csv' files into a single 'unified.csv' file.
4. **Cleanup (Optional):** If the `-c` flag is set, removes intermediate files and directories, leaving only 'unified.csv'.





