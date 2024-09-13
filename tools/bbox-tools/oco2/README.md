# Bounding Box Utilities for OCO-2 Data

## Introduction

When generating site-specific output products, the software needs to be provided bounding boxes for each Target-mode
region it will encounter. 

[//]: # (A list of target IDs and their centers can be downloaded from the [project website]&#40;https://oco3car.jpl.nasa.gov/api/report/clasp&#41;. )

There is no fixed cap on the distance a Target-mode sounding can be made from an associated target's center, thus
these tools were created to help extract, visualize, aggregate and analyze the Target-mode data to help the 
user decide what bounding box sizes they wish to use, evaluate its performance, and, ultimately, produce a correctly
formatted file to run the software.

## Setup

### Data

You'll need to download data from the [`OCO2_L2_Lite_FP v11.1r`](https://disc.gsfc.nasa.gov/datasets/OCO2_L2_Lite_FP_11.1r/summary?keywords=OCO2_L2_Lite_FP_11.1r) dataset. Any amount of data can be used, but we recommend
downloading as much as you are able, if not the whole dataset. You'll need an [Earthdata login](https://urs.earthdata.nasa.gov/)
to download the data.

While no comprehensive list of targets and their centers is publicly available, one has been constructed 
[here](oco2-targets-pub.csv) which should be up-to-date.

### Environment

Follow the environment section for the OCO-3 bounding box utilities in the parent directory.

## The Utilities

### 1: granulesToTargets.py - Extract Target data regions over a list of granules

This tool iterates over all OCO-2 data files (granules) in a provided directory, searching for regions of Target-mode
data and outputting them into a CSV file.

Usage:
```
usage: granulesToTargets.py [-h] -d INPUT_DIR -t TARGETS [-m {all_deg,all_km,cent_deg,cent_km}] [-o OUTPUT] [--overwrite]

options:
  -h, --help            show this help message and exit
  -d INPUT_DIR, --dir INPUT_DIR
                        Directory containing input files to process
  -t TARGETS, --targets TARGETS
                        Path to target file as provided in oco2-targets-pub.csv
  -m {all_deg,all_km,cent_deg,cent_km}, --method {all_deg,all_km,cent_deg,cent_km}
                        Method to infer the target for each region of target data. Default: all_km . 
                        Accepted values: 
                        all_deg: Compute degree distance for all points and choose most common minimum (slow) 
                        all_km: Compute geodesic distance for all points and choose most common minimum (very very slow) 
                        cent_deg: Compute degree distance for centroid of points and choose minimum 
                        cent_km: Compute geodesic distance for centroid of points and choose minimum
  -o OUTPUT, --output OUTPUT
                        Output file path. Defaults to oco2-target-bounding-boxes.csv in the current working directory.
  --overwrite           Overwrite output file if it exists.
  --outlier OUTLIER     Maximum distance from target center (based on evaluation defined by -m) over which a region will be excluded. Omit to include all regions.
```

### 2: targetsToBounds.py - Aggregate extracted bounding boxes into encompassing bounding boxes

This tool takes the output of `granulesToTargets.py` and produces a CSV file containing a bounding box for each target ID
that will encompass all the data for that ID in the files found by the `granulesToTargets.py` tool.

It will also try to produce a GeoPackage of these bounds that can be viewed in a tool like [QGIS](https://www.qgis.org/).

<p align="center">
    <img src="../doc/images/agg_bbox_qgis_oco2.png" alt="Target capture over El Reno, Oklahoma (elRenoOK) visualized in QGIS over the aggregate bounding box for all El Reno data." />
    <br>
    Target capture over El Reno, Oklahoma (elRenoOK) visualized in QGIS over the aggregate bounding box for all El Reno data.
</p>

Usage:
```
usage: targetsToBounds.py [-h] -i INPUT [-o OUTPUT] [--overwrite] [--no-geo]

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Input .csv file. Should be the output of the granulesToTargets.py script
  -o OUTPUT, --output OUTPUT
                        Output file path. Defaults to oco2-targets-aggregated-bounding-boxes.csv in the current working directory.
  --overwrite           Overwrite output file if it exists.
  --no-geo              Do not try to produce a GeoPackage of the computed bounding boxes
```

### 3: distanceStats.py - Compute distances between soundings and target centers

This tool will iterate over all OCO-2 data files (granules) in a provided directory and extract all soundings in the 
regions found by `granulesToTargets.py`. It will then compute all the distances between the sounding centers and the centers 
of their associated targets in both kilometers and degrees. 

There are two ways this tool computes distance, set by the `--diff` option: the default (`latlon`) uses the maximum difference 
in latitude or longitude (from the diagram below: $`\max (x, y)`$); (`abs`) will use the absolute distance between the sounding 
center and target center ($d$ from the diagram below).

<p align="center">
    <img src="../doc/images/distance_tool_diagram.png" alt="Distance computation diagram" />
</p>

Usage:
```
usage: distanceStats.py [-h] -d INPUT_DIR -e EXTRACTED_TARGETS -t TARGET_FILE [-o OUTPUT] [--overwrite] [--diff {abs,latlon}]

options:
  -h, --help            show this help message and exit
  -d INPUT_DIR, --dir INPUT_DIR
                        Directory containing input files to process
  -e EXTRACTED_TARGETS, --extracted-data EXTRACTED_TARGETS
                        Path to file containing SAM and Target captures (generated by granulesToTargets.py)
  -t TARGET_FILE, --targets TARGET_FILE
                        Path to file containing target definitions. (generated by oco2Targets.py)
  -o OUTPUT, --output OUTPUT
                        Output file path. Defaults to oco2-target-distances.json in the current working directory.
  --overwrite           Overwrite output file if it exists.
  --diff {abs,latlon}   Method to compute differences between sounding and target centroid. latlon (default) is the max 
                        abs difference in lat and lon (max(|lon_c - lon_s|, |lat_c - lat_s|). abs is the absolute distance between the centroids.
```

### 4: stats.ipynb - Compute and plot statistics on sounding-center distances

This is a Jupyter notebook that will load the distance data produced by `distanceStats.py` and produce histograms for each
target ID for Target-mode distances from center. It will also include statistics like maxima and minima, mean, 
standard deviation, and 90thm 95th and 99th percentiles. It can also aggregate for all data.

<p align="center">
    <img src="plots/priv/all data.png" alt="Sample plot: all data" />
    <br>
    Sample plot: all data
</p>

To run, simply start Jupyter and navigate to the notebook.

### 5: targetsToJson.py - Produce target configs based on offsets from target centers

Once you've decided what dimensions you want for your bounding boxes, you can use this tool to produce the config JSON
file. This tool currently produces bounding boxes from the centers given in the CSV file linked in the introduction and adds
a user-provided offset in either degrees or kilometers in all four directions. Like `targetsToBounds.py`, this will also try
to produce a GeoPackage for visualization.

<p align="center">
    <img src="../doc/images/produced_bbox_qgis_oco2.png" alt="Target capture over El Reno, Oklahoma (elRenoOK) visualized in QGIS over a sample 1x1 deg bounding box for all El Reno data." />
    <br>
    Target capture over El Reno, Oklahoma (elRenoOK) visualized in QGIS over a sample 1x1 deg (<code>0.5 --unit deg</code>>) bounding box for all El Reno data.
</p>

Usage:
```
usage: targetsToJson.py [-h] [--unit {km,deg}] [-o OUTPUT] [--overwrite] [--no-geo] target_file offset

positional arguments:
  target_file           Path to file containing target definitions. (provided in oco2-targets-pub.csv)
  offset                Offset from centroid for bboxes. Must be > 0.

options:
  -h, --help            show this help message and exit
  --unit {km,deg}       Units of the value of --offset. Boxes are computed by target centroid +/- offset in --units. Default: km
  -o OUTPUT, --output OUTPUT
                        Output file path. Defaults to targets.json in the current working directory.
  --overwrite           Overwrite output file if it exists.
  --no-geo              Do not try to produce a GeoPackage of the computed bounding boxes
```

### 6: targetsStats.py - Evaluate performance of target config over a list of granules

This tool will iterate over all OCO-2 data files (granules) in a provided directory and extract all soundings in the 
regions found by `granulesToTargets.py`. It will then compute how many of those soundings lie fully within, partially within,
or completely outside the provided target configuration (as produced by `targetsToJson.py`). By default, this will produce
an Excel spreadsheet of the data, with additional sheets aggregating the data by target ID and target type with an additional
aggregation for all data. The user can alternatively specify a `.csv` output, in which case the per-granule, per-Target-region
data is written alone in CSV format.

Usage:
```
usage: targetsStats.py [-h] -d INPUT_DIR -e EXTRACTED_TARGETS -t TARGET_FILE [-o OUTPUT] [--overwrite]

options:
  -h, --help            show this help message and exit
  -d INPUT_DIR, --dir INPUT_DIR
                        Directory containing input files to process
  -e EXTRACTED_TARGETS, --extracted-data EXTRACTED_TARGETS
                        Path to file containing SAM and Target captures (generated by granulesToTargets.py)
  -t TARGET_FILE, --targets TARGET_FILE
                        Target bbox JSON file (generated by targetsToJson.py)
  -o OUTPUT, --output OUTPUT
                        Output file path. Defaults to oco2-target-stats.xlsx in the current working directory. If a .csv extension is given, a CSV file will be produced.
  --overwrite           Overwrite output file if it exists.
```
