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
│  ├─ 📂 autonomous  
│  │  ├─ 📂 different_paths  
│  │  │  ├─ ❓ [README.md](./hardware/autonomous/different_paths/README.md)  
│  │  │  ├─ 💾 `different_paths_0.ulog`  
│  │  │  └─ 💾 `<...>.ulog`    
│  │  │    
│  │  ├─ 📂 different_terrains  
│  │  │  ├─ ❓ [README.md](./hardware/autonomous/different_terrains/README.md)   
│  │  │  ├─ 💾 `different_terrains_0.ulog`  
│  │  │  └─ 💾 `<...>.ulog`    
│  │  │    
│  │  ├─ 📂 linear_paths  
│  │  │  ├─ ❓ [README.md](./hardware/autonomous/linear_paths/README.md)   
│  │  │  ├─ 💾 `linear_paths_0.ulog`  
│  │  │  └─ 💾 `<...>.ulog`    
│  │  │  
│  │  └─ 📂 takeoff_land  
│  │     ├─ ❓ [README.md](./hardware/autonomous/takeoff_land/README.md)  
│  │     ├─ 💾 `takeoff_land_0.ulog`  
│  │     └─ 💾 `<...>.ulog`    
│  │  
│  ├─ 📂 gimbal_camera_video  
│  │  ├─ 📂 natsbags  
│  │  │  ├─ 💾 `different_paths_0.gz`  
│  │  │  └─ 💾 `<...>.gz`    
│  │  │    
│  │  └─ 📂 videos  
│  │     ├─ 💾 `different_paths_0.mp4`  
│  │     └─ 💾 `<...>.mp4`    
│  │  
│  └─ 📂 manual  
│     └─ 📂 fast_movements  
│        ├─ ❓ [README.md](./hardware/manual/fast_movements/README.md)   
│        ├─ 💾 `fast_movements_0.ulog`  
│        └─ 💾 `<...>.ulog`    
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
  - "battery_status"
  - "vehicle_global_position"
  - "vehicle_local_position"
  - "vehicle_attitude"
  - "vehicle_status"
  - "actuator_outputs"
  - "sensor_baro"
blacklist_headers:
  - "timestamp_sample"
  - "device_id"
  - "error_count"
```

## `.ulog` --> `.csv` -- `ulog_converter.py`

Converts ULog files to CSV, with options for filtering, merging, and creating a unified CSV.

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

* **ulog_dir (str):**  Path to the directory containing ULog files.
* **filter.yaml (str):** Path to a YAML filter file specifying:
    * **whitelist_messages (list):**  List of message names to include during conversion 
    * **blacklist_headers (list):** List of headers (field names) to exclude from the output CSV files.
* **-o, --output_dir (str):**  Output directory for converted CSV files.  Mirrors the input directory structure. Defaults to 'output_dir'.
* **-m, --merge:** If specified, merges CSV files within each subdirectory into a 'merged.csv' file.
* **-c, --clean:**  If specified, cleans up intermediate files after merging, leaving only 'unified.csv'.
* **-v, --verbose:**  Enables verbose logging during the process.

### Workflow

1. **File Conversion:** Converts ULog files in the specified directory to individual CSV files, applying filters from the YAML file.
2. **File Merging (Optional):** If the `-m` flag is set, merges CSV files within each subdirectory into a single 'merged.csv'.
3. **File Unification (Optional):** Combines all 'merged.csv' files into a single 'unified.csv' file.
4. **Cleanup (Optional):** If the `-c` flag is set, removes intermediate files and directories, leaving only 'unified.csv'.





