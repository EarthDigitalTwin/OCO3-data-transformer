
/*

EDL

*/

variable "edl_username" {
  description = "NASA Earthdata Login username"
  type        = string
  sensitive   = false
}

variable "edl_password" {
  description = "NASA Earthdata Login password"
  type        = string
  sensitive   = true
}

/*

IAM

*/

variable "iam_lambda_role" {
  description = "IAM role name for Lambda"
  type        = string
}

variable "iam_task_role" {
  description = "IAM role name for Batch job executions (actual compute permissions)"
  type        = string
}

variable "iam_task_execution_role" {
  description = "IAM role name for Batch job container management (KMS fetch etc)"
  type        = string
}

variable "iam_step_role" {
  description = "IAM role name for Step Function state machine executions"
  type        = string
}

variable "iam_events_role" {
  description = "IAM role name for EventBridge Scheduler invocations"
  type        = string
}


/*

VPC

*/

variable "subnet" {
  description = "Subnet ID to use for EFS & compute"
  type        = string
}

variable "compute_sg" {
  description = "Compute security group. Needs outbound rules for HTTP/S (0.0.0.0/0) and NFS to at least the EFS security group"
  type        = string
}

variable "efs_sg" {
  description = "EFS security group. Must allow inbound NFS traffic from compute SG"
  type        = string
}

/*

EFS

*/

variable "efs_fs_id" {
  description = "EFS Filesystem ID"
  type        = string
}

variable "efs_ap_id" {
  description = "EFS Access Point ID to use"
  type        = string
}

/*

S3

*/

variable "s3_bucket" {
  description = "S3 bucket to store outputs and config data"
  type        = string
}

variable "s3_rc_template_key" {
  description = "Key for the RC template (YAML file) object in S3"
  type        = string
  default     = "deploy/run-config.yaml"
}

variable "s3_outputs_prefix" {
  description = "Key prefix for output products"
  type        = string
  default     = "outputs"
}

/*

Batch

*/

variable "batch_ce_name" {
  description = "Name of Batch compute environment to use"
  type        = string
}

/*

CloudWatch Logs

*/

variable "log_retention_time" {
  description = "Specifies the number of days you want to retain log events in the specified log group. Possible values are: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653, and 0. If you select 0, the events in the log group are always retained and never expire."
  type        = number
  default     = 0

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653, 0], tonumber(var.log_retention_time))
    error_message = "Invalid log retention time. Possible values are: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653, and 0"
  }
}

/*

EventBridge

*/

variable "schedule_frequency" {
  description = "EventBridge schedule frequency. Values: 15, 30, 60 for 4, 2 and 1 invocations per hour"
  type        = number
  default     = 15

  validation {
    condition     = contains([15, 30, 60], tonumber(var.schedule_frequency))
    error_message = "Invalid schedule frequency"
  }
}

/*

Config

*/

variable "output_ds_name" {
  description = "Root name of output datasets."
  type        = string
}

variable "output_chunk_lat" {
  description = "Output Zarr chunk latitude length. Use integers"
  type        = number

  validation {
    condition     = var.output_chunk_lat > 0 && var.output_chunk_lat == floor(var.output_chunk_lat)
    error_message = "Chunk sizes must be an integer > 0"
  }
}

variable "output_chunk_lon" {
  description = "Output Zarr chunk longitude length. Use integers"
  type        = number

  validation {
    condition     = var.output_chunk_lon > 0 && var.output_chunk_lon == floor(var.output_chunk_lon)
    error_message = "Chunk sizes must be an integer > 0"
  }
}

variable "output_chunk_time" {
  description = "Output Zarr chunk time length. Use integers"
  type        = number

  validation {
    condition     = var.output_chunk_time > 0 && var.output_chunk_time == floor(var.output_chunk_time)
    error_message = "Chunk sizes must be an integer > 0"
  }
}

variable "output_grid_lat_resolution" {
  description = "Size of the output dataset's latitude dimension. The longitude dimension will be set to 2x this value. Must be an integer >= 1800."
  type        = number
  default     = 18000

  validation {
    condition     = floor(var.output_grid_lat_resolution) == var.output_grid_lat_resolution
    error_message = "Output grid res must be an integer!"
  }

  validation {
    condition     = var.output_grid_lat_resolution >= 1800
    error_message = "Output grid res must be >= 1800"
  }
}

variable "output_interpolation_method" {
  description = "Interpolation method used to grid output data. Must be one of 'nearest' (not recommended), 'linear' (recommended), or 'cubic'"
  type        = string
  default     = "linear"

  validation {
    condition     = contains(["nearest", "linear", "cubic"], var.output_interpolation_method)
    error_message = "Interpolation method must be one of 'nearest' (not recommended), 'linear' (recommended), or 'cubic'"
  }
}

variable "input_granule_limit" {
  description = "Max number of input granules to process for each execution."
  type        = number

  validation {
    condition     = var.input_granule_limit >= 1 && var.input_granule_limit <= 32 && var.input_granule_limit == floor(var.input_granule_limit)
    error_message = "Granule limit should be an integer between 1 and 32"
  }
}

variable "mprof_interval" {
  type        = number
  description = "Number of seconds between log reports of memory consumption in the processing stage. Must be between 10 and 3600 (-1 to disable)"
  default     = -1

  validation {
    condition     = var.mprof_interval == -1 || (var.mprof_interval >= 10 && var.mprof_interval <= 3600)
    error_message = "mprof_interval must be between 10 and 3600 (-1 to disable)"
  }
  validation {
    condition     = var.mprof_interval == floor(var.mprof_interval)
    error_message = "mprof_interval must be an integer"
  }
}

variable "interpolate_max_parallel" {
  type        = number
  description = "Maximum number of threads that can interpolate SAM data to grid in parallel. Must be between 1 and 4 (-1 to use process image default)"
  default     = -1

  validation {
    condition     = var.interpolate_max_parallel == -1 || (var.interpolate_max_parallel >= 1 && var.interpolate_max_parallel <= 4)
    error_message = "interpolate_max_parallel must be between 1 and 4 (-1 to use process image default)"
  }
  validation {
    condition     = var.interpolate_max_parallel == floor(var.interpolate_max_parallel)
    error_message = "interpolate_max_parallel must be an integer"
  }
}

variable "disable_schedule" {
  type        = bool
  description = "Create the schedule in disabled state"
  default     = false
}

variable "image" {
  type        = string
  description = "Docker image to use for data processing, sync and restore. Format: <registry>/<image_name>. Example: rileykkjpl/oco-sam-l3"

  validation {
    condition     = can(regex("^[a-z0-9-_]+/[a-z0-9-_]+$", var.image))
    error_message = "Invalid docker image name"
  }
}

variable "image_tag" {
  type        = string
  description = "Docker image tag version to use for data processing, sync and restore. Format: <tag>. <tag> should be formatted as \"YYYY.MM.DD[-[a-z0-9-_]+]\". Minimum version: 2024.01.25"

  validation {
    condition     = can(regex("^\\d{4}\\.\\d{2}\\.\\d{2}(-[a-z0-9-_]+)?$", var.image_tag))
    error_message = "Invalid docker image tag"
  }

  validation {
    condition     = element(sort([substr(var.image_tag, 0, 10), "2024.03.12"]), 0) == "2024.03.12"
    error_message = "Docker image tag is too old"
  }
}

variable "verbose" {
  type        = bool
  description = "Process step logging verbosity"
  default     = false
}

variable "testing" {
  description = "Is this plan/apply for testing?"
  type        = bool
  default     = false
}
