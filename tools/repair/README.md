# Zarr Backup Restore Utility

## Description

This script is used when product generation fails during Zarr write. When product generation begins, a file is created
specifying the paths of backups for each dataset. The directory this file will be created in defaults to
`/tmp/oco_pipeline_zarr_state` but can be configured by setting the `ZARR_BACKUP_DIR` environment variable for the 
product generation execution. The file will always be named `ZARR_WRITE.json`.

In the event the product generation code fails, you can determine if the output data stores are potentially corrupted - 
thus needing this restore script - by checking if this file exists. If it does, you should run this script.

## To Run

To run, ensure you are using the same conda environment that the generation code is using. If the `ZARR_BACKUP_DIR` 
variable was set for the failed product generation, set it to the same value. Then simply run:

```shell
python repair.py --rc <path to runconfig>
```

where the provided RunConfig path is the same one used in the failed generation run.
