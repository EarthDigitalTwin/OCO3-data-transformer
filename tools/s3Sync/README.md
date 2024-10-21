# Local Filesystem to S3 Sync Utility

## Description

Directly writing output products to S3, while supported, is very non-performant and, thus, discouraged. This utility
was written to sync outputs to the local filesystem to locations in S3. 

## Configuration

This script is primarily configured through environment variables as follows:

| Variable           | Description                                                                                                                      |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `LOCAL_ROOT`       | Root directory of Zarr outputs.                                                                                                  |
| `PRE_QF_NAME`      | Name of pre_qf Zarr output directory.                                                                                            |
| `POST_QF_NAME`     | Name of post_qf Zarr output directory.                                                                                           |
| `S3_BUCKET`        | S3 bucket to sync objects to.                                                                                                    |
| `S3_ROOT_PREFIX`   | Root path in S3 to sync objects to.                                                                                              |
| `COG_DIR`          | OPTIONAL: Directory of CoG outputs.                                                                                              |
| `COG_DIR_S3`       | OPTIONAL: S3 subdirectory under `S3_ROOT_PREFIX` for CoG outputs.<br/> Required if `COG_DIR` is set.                             |
| `DRYRUN`           | If set, do not perform S3 operations, but compute files to sync as normal.                                                       |
| `VERBOSE`          | Set `DEBUG` log level                                                                                                            |
| `RC_FILE_OVERRIDE` | Override the values of `LOCAL_ROOT`, `PRE_QF_NAME`, `POST_QF_NAME`, and `COG_DIR` with values defined in a run config YAML file. |

## To Run

To run, ensure you are using either the same conda environment that the generation code is using or set up and activate
a virtual environment with

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```


Then with your environment variables set, simply run:

```shell
python sync.py
```
