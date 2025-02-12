---
title: px4-log-tool
description: The All-in-One tool to work with PX4 log files.
---

# Installation

## Regular Installation

```bash
 pip install git+https://github.com/tiiuae/px4-log-tool.git#egg=px4-log-tool
```

## Developer Mode

```bash
git clone https://www.github.com/tiiuae/px4-log-tool.git
cd px4-log-tool
pip install -e .
```

## Shell Tab-Completions

Currently only Bash, Zsh and Fish shells are supported. After installing the CLI tool:

```bash
_PX4_LOG_TOOL_COMPLETE=bash_source px4-log-tool > ~/.px4_log_tool_completion.bash
```
```bash
_PX4_LOG_TOOL_COMPLETE=zsh_source px4-log-tool > ~/.px4_log_tool_completion.zsh
```
```bash
_PX4_LOG_TOOL_COMPLETE=fish_source px4-log-tool > ~/.px4_log_tool_completion.fish
```

Then source the generated completion shell file into you `.bashrc`, `.zshrc` or `config.fish`. Example:

```bash
echo "source ~/.px4_log_tool_completion.bash" >> ~/.bashrc && source ~/.bashrc
```

# Usage

The CLI tool should have a rich help feature.

```bash
px4-log-tool --help
px4-log-tool subcommands --help
```

Every subcommand is documented and can be queried with the `--help` flag.

By default, the logging is set to only flag "ERROR" that cause the program to fail.

```bash
px4-log-tool --verbose subcommands #shows ERROR
```

To see general INFO and WARN messages, adjust the environment variable `PRINT_LEVEL` to 2 or 1.

```bash
PRINT_LEVEL=1 px4-log-tool --verbose subcommands #shows WARN, ERROR
PRINT_LEVEL=2 px4-log-tool --verbose subcommands #shows INFO, WARN, ERROR
```

# Data Conversion and Pre-Processing

## `filter.yaml`

This file is necessary for most operations with the CLI tool. This is populated with pertinent information 

### `.ulog` -> `.csv`

Contains two lists for `whitelist_messages` and `blacklist headers`.

Add uorb/ros2 topics of interest into the `whitelist_messages` list.

Add headers that are redundant or not required in the `blacklist_headers` list.

`resample_params` contains parameters for resampling the data after it is merged. More on this is explained in [Resampling Functionality](#resampling-functionality). Provide
the target sampling frequency in Hertz at `target_frequency_hz`.

For the other parameters, refer to the following documentation links of the `pandas` library:

* [`pandas.DataFrame.resample`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html)
* [`pandas.DataFrame.agg`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.agg.html)
* [`pandas.DataFrame.interpolate`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html)

### `.csv` -> `.db3`

Under `bag_params` contains two parameters `topic_prefix` and `captitalise_topics`.
- `topic_prefix`: Namespace/prefix for the `px4_msgs` ROS 2 topics. Defaults to `/fmu/out`
- `topic_max_frequency_hz`: The maximum frequency of topics of ROS 2 bags when converting from `.ulog`. Defaults to 100Hz.
- `capitalise_topics`: Depending on the PX4-Autopilot version, the topic names are either CamelCase (`capitalise_topics: True`) or snake_case (`capitalise_topics: False`). Defaults to `False`.

### Metadata Generation (Only for `.ulog` files)

The field `metadata_fields` should be populated with the list of metadata properties to be extracted from the `.ulog` files.

These fields can be expanded upon through Feature Requests on "Issues". As of now, the possible metadata properties are:
- "max_altitude"
- "min_altitude"
- "average_altitude"
- "max_speed"
- "min_speed"
- "average_speed"
- "yaw_lock"

### Example
```yaml
whitelist_messages:
  - sensor_combined
  - vehicle_attitude
  - ... (other message names. If this list is empty it will convert all messages)
blacklist_headers:
  - timestamp
  - ... (other field names)
resample_params:
  target_frequency_hz: 10
  num_method: "mean"
  cat_method: "ffill"
  interpolate_numerical: True
  interpolate_method: "linear"
metadata_fields:
  - "max_altitude"
  - "min_altitude"
  - "average_altitude"
  - "max_speed"
  - "min_speed"
  - "average_speed"
  - "yaw_lock"
bag_params:
  topic_prefix: "/fmu/out"
  topic_max_frequency_hz: 100
  capitalise_topics: False
```

## Convert `.ulog` to `.csv`: `ulog2csv`

Convert a provided directory containing `.ulog` files into `.csv`. The directory can be ordered in any way, and can contain subdirectories as well.

```bash
px4-log-tool ulog2csv DIRECTORY_ADDRESS -f FILTER [-o OUTPUT_DIRECTORY -m -r -c]
```

Documentation for usage of this command can be obtained through the `-h` or `--help` flag:

```bash
px4-log-tool ulog2csv --help
```

## Generate `metadata.json` for `.ulog` files: `generate-metadata`

Generate `metadata.json` for `.ulog` files in DIRECTORY_ADDRESS with metadata fields in FILTER. This operation is in place, so the `.json` files will be added into the provided directory.

```bash
px4-log-tool generate-metadata DIRECTORY_ADDRESS -f FILTER
```

## Convert `.csv` to `.db3`: `csv2db3`

> [!IMPORTANT]
> Need to have ROS 2 framework and the `px4_msgs` ROS 2 packages installed and sourced.

Convert a provided directory containing folders of `.csv` files into ROS 2 bag files in the `.db3` format. It is important the the final directory containing `.csv` files are formatted correctly. Best is to use the [`ulog2csv`](#convert-ulog-to-csv-ulog2csv) first and target the output.

Operation can be in-place or the bag files can be generated into a specified directory.

```bash
px4-log-tool csv2db3 DIRECTORY_ADDRESS -f FILTER [-o OUTPUT_DIRECTORY]
```

Documentation for usage of this command can be obtained through the `-h` or `--help` flag:

```bash
px4-log-tool csv2db3 --help
```

## Convert `.ulog` to `.db3`: `ulog2db3`

> [!IMPORTANT]
> Need to have ROS 2 framework and the `px4_msgs` ROS 2 packages installed and sourced.

Convert a provided directory containing folders of `.ulog` files into ROS 2 bag files in the `.db3` format. This will also perform topic adjustment. For instance, it will reduce the rate of the topics from the `.ulog` file to 100Hz before converting to `.db3` ROS 2 bags. This can be changed by modifying the `topic_max_frequency_hz`, parameter in the `filter.yaml` file provided when running the CLI tool.

This operation will create a mirror output folder of `.csv` files with the corresponding `.db3` bag files inside them.

```bash
px4-log-tool ulog2db3 DIRECTORY_ADDRESS -f FILTER [-o OUTPUT_DIRECTORY]
```

Documentation for usage of this command can be obtained through the `-h` or `--help` flag:

```bash
px4-log-tool ulog2db3 --help
```

