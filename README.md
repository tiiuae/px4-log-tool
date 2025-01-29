---
title: px4-log-tool
description: The All-in-One tool to work with PX4 log files.
---
# Data Conversion and Pre-Processing

## `filter.yaml`

This file is necessary for most operations with the CLI tool. This is populated with pertinent information 

Contains two lists for `whitelist_messages` and `blacklist headers`.

Add uorb/ros2 topics of interest into the `whitelist_messages` list.

Add headers that are redundant or not required in the `blacklist_headers` list.

`resample_params` contains parameters for resampling the data after it is merged. More on this is explained in [Resampling Functionality](#resampling-functionality). Provide
the target sampling frequency in Hertz at `target_frequency_hz`.

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
