# Copyright 2025 California Institute of Technology (Caltech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


locals {
  workspace_name = terraform.workspace == "default" ? "" : "${terraform.workspace}-"
  name_root      = var.testing ? "oco3-tftest-${local.workspace_name}" : "oco3-${local.workspace_name}"

  schedule_name = "${local.name_root}transform-schedule"
  sfn_name      = "${local.name_root}transform"

  metrics_namespace_root = var.global_product ? "OCO3/GLOBAL${var.testing ? "-TFTEST" : ""}" : "OCO3/TARGET_FOCUSED${var.testing ? "-TFTEST" : ""}"
  metrics_namespace      = "${local.metrics_namespace_root}${terraform.workspace == "default" ? "-DEV" : "-PROD"}"

  image = "${var.image}:${var.image_tag}"

  chunk_config = "${var.output_chunk_time}_${var.output_chunk_lat}_${var.output_chunk_lon}"

  sns_subject_ws_id = terraform.workspace == "default" ? "" : " [${upper(terraform.workspace)}]"

  invoke_frequency = {
    quarter-hourly = "cron(*/15 * * * ? *)"
    half-hourly    = "cron(*/30 * * * ? *)"
    hourly         = "cron(0 * * * ? *)"
    daily          = "cron(0 0 * * ? *)"
    "15"           = "cron(*/15 * * * ? *)"
    "30"           = "cron(*/30 * * * ? *)"
    "60"           = "cron(*/60 * * * ? *)"
  }

  log_levels = {
    INFO  = []
    DEBUG = ["-v"]
    TRACE = ["-vv"]
  }

  rc_template = yamlencode({
    output = {
      local = "/var/outputs"
      naming = {
        pre_qf  = var.global_product ? "${var.output_ds_name}_pre_qf.zarr" : "${var.output_ds_name}_pre_qf"
        post_qf = var.global_product ? "${var.output_ds_name}_post_qf.zarr" : "${var.output_ds_name}_post_qf"
      }
      title = {
        pre_qf  = upper("${var.output_ds_name}_pre_qf")
        post_qf = upper("${var.output_ds_name}_post_qf")
      }
      global     = var.global_product
      drop-empty = var.drop-empty
      cog = var.global_product ? null : {
        efs = true,
        output = {
          local = "/var/outputs/${var.output_ds_name}_cog/"
        }
        options = var.cog_options
      }
    }
    input = {
      files = []
    }
    variables = var.variables != null ? var.variables : {}
    grid = {
      latitude        = var.output_grid_lat_resolution
      longitude       = var.output_grid_lat_resolution * (var.global_product ? 2 : 1)
      method          = var.output_interpolation_method
      resolution_attr = var.output_grid_lat_resolution == 18000 ? "1km" : var.output_grid_lat_resolution == 1800 ? "10km" : null
    }
    chunking = {
      latitude  = var.output_chunk_lat
      longitude = var.output_chunk_lon
      time      = var.output_chunk_time
    }
    max-workers  = var.global_product ? 4 : 16
    mask-scaling = 1.00
    target-file : var.global_product ? null : { oco3 = "/var/targets.json", oco2 = "/var/targets_oco2.json" }
  })

  batch_definition_containerprops_search = jsonencode({
    image            = "ghcr.io/unity-sds/unity-data-services:5.3.2"
    command          = ["SEARCH"]
    jobRoleArn       = data.aws_iam_role.iam_task_role.arn
    executionRoleArn = data.aws_iam_role.iam_task_execution_role.arn
    volumes = [
      {
        name = "oco-efs"
        efsVolumeConfiguration = {
          fileSystemId      = local.fs_dep_id
          transitEncryption = "ENABLED"
          authorizationConfig = {
            accessPointId = local.ap_dep_id
          }
        }
      }
    ]
    environment = [
      {
        name  = "LIMITS",
        value = "-1"
      },
      {
        name  = "FILTER_ONLY_ASSETS",
        value = "TRUE"
      },
      {
        name  = "DATE_FROM",
        value = "1970-01-01T00:00:00Z"
      },
      {
        name  = "GRANULES_SEARCH_DOMAIN",
        value = "CMR"
      },
      {
        name  = "OUTPUT_FILE",
        value = "/tmp/cmr-results.json"
      },
      {
        name  = "COLLECTION_ID",
        value = "C2910086168-GES_DISC"
      },
      {
        name  = "CMR_BASE_URL",
        value = "https://cmr.earthdata.nasa.gov"
      },
      {
        name  = "VERIFY_SSL",
        value = "FALSE"
      },
      {
        name  = "LOG_LEVEL",
        value = "10"
      },
      {
        name  = "DATE_TO",
        value = "2999-12-31T00:00:00Z"
      }
    ]
    mountPoints = [
      {
        containerPath = "/tmp"
        readOnly      = false
        sourceVolume  = "oco-efs"
      }
    ]
    ulimits = []
    resourceRequirements = [
      {
        value = "1.0"
        type  = "VCPU"
      },
      {
        value = "2048"
        type  = "MEMORY"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.log_group.name
      }
      secretOptions = []
    }
    secrets = []
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    runtimePlatform = {
      operatingSystemFamily = "LINUX"
      cpuArchitecture       = "X86_64"
    }
  })

  batch_definition_containerprops_stage = jsonencode({
    image            = "ghcr.io/unity-sds/unity-data-services:5.3.2"
    command          = ["DOWNLOAD"]
    jobRoleArn       = data.aws_iam_role.iam_task_role.arn
    executionRoleArn = data.aws_iam_role.iam_task_execution_role.arn
    volumes = [
      {
        name = "oco-efs"
        efsVolumeConfiguration = {
          fileSystemId      = local.fs_dep_id
          transitEncryption = "ENABLED"

          authorizationConfig = {
            accessPointId = local.ap_dep_id
          }
        }
      }
    ]
    environment = [
      {
        name  = "GRANULES_DOWNLOAD_TYPE",
        value = "DAAC"
      },
      {
        name  = "PARALLEL_COUNT",
        value = "24"
      },
      {
        name  = "EDL_BASE_URL",
        value = "https://urs.earthdata.nasa.gov"
      },
      {
        name  = "PYTHONUNBUFFERED",
        value = "1"
      },
      {
        name  = "DOWNLOAD_RETRY_TIMES",
        value = "5"
      },
      {
        name  = "EDL_PASSWORD_TYPE",
        value = "PLAIN"
      },
      {
        name  = "DOWNLOAD_DIR",
        value = "/etc/granules/inputs"
      },
      {
        name  = "EDL_USERNAME",
        value = var.edl_username
      },
      {
        name  = "DOWNLOAD_RETRY_WAIT_TIME",
        value = "30"
      },
      {
        name  = "LOG_LEVEL",
        value = contains(["DEBUG", "TRACE"], var.log_level) ? "10" : "20"
      },
      {
        name  = "STAC_JSON",
        value = "/etc/granules/cmr-results.json"
      }
    ]
    mountPoints = [
      {
        containerPath = "/etc/granules"
        readOnly      = false
        sourceVolume  = "oco-efs"
      }
    ]
    ulimits = []
    resourceRequirements = [
      {
        value = "2.0"
        type  = "VCPU"
      },
      {
        value = "4096"
        type  = "MEMORY"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.log_group.name
      }
      secretOptions = []
    }
    secrets = [
      {
        name      = "EDL_PASSWORD"
        valueFrom = aws_secretsmanager_secret.edl_secret.arn
      }
    ]
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    runtimePlatform = {
      operatingSystemFamily = "LINUX"
      cpuArchitecture       = "X86_64"
    }
  })

  batch_definition_containerprops_process = jsonencode({
    image = local.image
    command = concat(
      ["python", "/sam_extract/main.py", "-i", "/var/pipeline-rc.yaml"],
      local.log_levels[var.log_level]
    )
    jobRoleArn       = data.aws_iam_role.iam_task_role.arn
    executionRoleArn = data.aws_iam_role.iam_task_execution_role.arn
    volumes = [
      {
        name = "oco-efs"
        efsVolumeConfiguration = {
          fileSystemId      = local.fs_dep_id
          transitEncryption = "ENABLED"
          authorizationConfig = {
            accessPointId = local.ap_dep_id
          }
        }
      }
    ]
    environment = concat(
      [
        {
          name  = "USING_EFS"
          value = "true"
        },
        {
          name  = "ZARR_BACKUP_DIR"
          value = "/var/oco_pipeline_zarr_state"
        }
      ],
      var.metrics_profile_rate != -1 ? [
        {
          name  = "METRICS_PROFILE_RATE"
          value = tostring(var.metrics_profile_rate)
        }
      ] : [],
      var.interpolate_max_parallel_global != -1 ? [
        {
          name  = "INTERP_MAX_PARALLEL"
          value = tostring(var.interpolate_max_parallel_global)
        }
      ] : []
    )
    mountPoints = [
      {
        containerPath = "/var"
        readOnly      = false
        sourceVolume  = "oco-efs"
      }
    ]
    ulimits = []
    resourceRequirements = [
      {
        value = "16.0"
        type  = "VCPU"
      },
      {
        value = "122880"
        type  = "MEMORY"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.log_group.name
      }
      secretOptions = []
    }
    secrets = []
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    ephemeralStorage = {
      sizeInGiB = 50
    }
    runtimePlatform = {
      operatingSystemFamily = "LINUX"
      cpuArchitecture       = "X86_64"
    }
  })

  batch_definition_containerprops_push = jsonencode({
    image            = local.image
    command          = ["python", "/tools/s3Sync/sync.py", "--method", "awscli"]
    jobRoleArn       = data.aws_iam_role.iam_task_role.arn
    executionRoleArn = data.aws_iam_role.iam_task_execution_role.arn
    volumes = [
      {
        name = "oco-efs"
        efsVolumeConfiguration = {
          fileSystemId      = local.fs_dep_id
          transitEncryption = "ENABLED"
          authorizationConfig = {
            accessPointId = local.ap_dep_id
          }
        }
      }
    ]
    environment = [
      {
        name  = "S3_BUCKET",
        value = var.s3_bucket
      },
      {
        name  = "S3_ROOT_PREFIX",
        value = var.s3_outputs_prefix
      },
      {
        name  = "RC_FILE_OVERRIDE",
        value = "/var/pipeline-rc.yaml"
      },
      {
        name  = "COG_DIR_S3",
        value = var.output_ds_name
      }
    ]
    mountPoints = [
      {
        containerPath = "/var"
        readOnly      = false
        sourceVolume  = "oco-efs"
      }
    ]
    ulimits = []
    resourceRequirements = [
      {
        value = "4.0"
        type  = "VCPU"
      },
      {
        value = "16384"
        type  = "MEMORY"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.log_group.name
      }
      secretOptions = []
    }
    secrets = []
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    runtimePlatform = {
      operatingSystemFamily = "LINUX"
      cpuArchitecture       = "X86_64"
    }
  })

  batch_definition_containerprops_restore = jsonencode({
    image            = local.image
    command          = ["python", "/tools/repair/repair.py", "--rc", "/var/pipeline-rc.yaml"]
    jobRoleArn       = data.aws_iam_role.iam_task_role.arn
    executionRoleArn = data.aws_iam_role.iam_task_execution_role.arn
    volumes = [
      {
        name = "oco-efs"
        efsVolumeConfiguration = {
          fileSystemId      = local.fs_dep_id
          transitEncryption = "ENABLED"
          authorizationConfig = {
            accessPointId = local.ap_dep_id
          }
        }
      }
    ]
    environment = [
      {
        name  = "ZARR_BACKUP_DIR",
        value = "/var/oco_pipeline_zarr_state"
      }
    ]
    mountPoints = [
      {
        containerPath = "/var"
        readOnly      = false
        sourceVolume  = "oco-efs"
      }
    ]
    ulimits = []
    resourceRequirements = [
      {
        value = "4.0"
        type  = "VCPU"
      },
      {
        value = "16384"
        type  = "MEMORY"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group = aws_cloudwatch_log_group.log_group.name
      }
      secretOptions = []
    }
    secrets = []
    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
    ephemeralStorage = {
      sizeInGiB = 35
    }
    runtimePlatform = {
      operatingSystemFamily = "LINUX"
      cpuArchitecture       = "X86_64"
    }
  })
}
