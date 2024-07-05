# Copyright 2024 California Institute of Technology (Caltech)
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
  name_root = var.testing ? "oco3-tftest-" : "oco3-"

  schedule_name = "${local.name_root}transform-schedule"
  sfn_name      = "${local.name_root}transform"

  image = "${var.image}:${var.image_tag}"

  chunk_config = "${var.output_chunk_time}_${var.output_chunk_lat}_${var.output_chunk_lon}"

  invoke_frequency = {
    quarter-hourly = "cron(*/15 * * * ? *)"
    half-hourly    = "cron(*/30 * * * ? *)"
    hourly         = "cron(0 * * * ? *)"
    daily          = "cron(0 0 * * ? *)"
    weekly         = "cron(0 0 ? * 1 *)"
    monthly        = "cron(0 0 1 * ? *)"
    "15"           = "cron(*/15 * * * ? *)"
    "30"           = "cron(*/30 * * * ? *)"
    "60"           = "cron(*/60 * * * ? *)"
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
      global = var.global_product
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
    drop-dims = [
      {
        group = "/Meteorology"
        name  = "psurf_apriori_sco2"
      },
      {
        group = "/Meteorology"
        name  = "psurf_apriori_wco2"
      },
      {
        group = "/Meteorology"
        name  = "windspeed_u_met"
      },
      {
        group = "/Meteorology"
        name  = "windspeed_v_met"
      },
      {
        group = "/Preprocessors"
        name  = "xco2_weak_idp"
      },
      {
        group = "/Preprocessors"
        name  = "xco2_strong_idp"
      },
      {
        group = "/Preprocessors"
        name  = "max_declocking_o2a"
      },
      {
        group = "/Preprocessors"
        name  = "csstd_ratio_wco2"
      },
      {
        group = "/Preprocessors"
        name  = "dp_abp"
      },
      {
        group = "/Preprocessors"
        name  = "co2_ratio"
      },
      {
        group = "/Retrieval"
        name  = "surface_type"
      },
      {
        group = "/Retrieval"
        name  = "psurf"
      },
      {
        group = "/Retrieval"
        name  = "SigmaB"
      },
      {
        group = "/Retrieval"
        name  = "windspeed"
      },
      {
        group = "/Retrieval"
        name  = "windspeed_apriori"
      },
      {
        group = "/Retrieval"
        name  = "psurf_apriori"
      },
      {
        group = "/Retrieval"
        name  = "t700"
      },
      {
        group = "/Retrieval"
        name  = "fs"
      },
      {
        group = "/Retrieval"
        name  = "fs_rel"
      },
      {
        group = "/Retrieval"
        name  = "tcwv"
      },
      {
        group = "/Retrieval"
        name  = "tcwv_apriori"
      },
      {
        group = "/Retrieval"
        name  = "tcwv_uncertainty"
      },
      {
        group = "/Retrieval"
        name  = "dp"
      },
      {
        group = "/Retrieval"
        name  = "dp_o2a"
      },
      {
        group = "/Retrieval"
        name  = "dp_sco2"
      },
      {
        group = "/Retrieval"
        name  = "dpfrac"
      },
      {
        group = "/Retrieval"
        name  = "s31"
      },
      {
        group = "/Retrieval"
        name  = "s32"
      },
      {
        group = "/Retrieval"
        name  = "co2_grad_del"
      },
      {
        group = "/Retrieval"
        name  = "dws"
      },
      {
        group = "/Retrieval"
        name  = "aod_fine"
      },
      {
        group = "/Retrieval"
        name  = "eof2_2_rel"
      },
      {
        group = "/Retrieval"
        name  = "aod_dust"
      },
      {
        group = "/Retrieval"
        name  = "aod_bc"
      },
      {
        group = "/Retrieval"
        name  = "aod_oc"
      },
      {
        group = "/Retrieval"
        name  = "aod_seasalt"
      },
      {
        group = "/Retrieval"
        name  = "aod_sulfate"
      },
      {
        group = "/Retrieval"
        name  = "aod_strataer"
      },
      {
        group = "/Retrieval"
        name  = "aod_water"
      },
      {
        group = "/Retrieval"
        name  = "aod_ice"
      },
      {
        group = "/Retrieval"
        name  = "aod_total"
      },
      {
        group = "/Retrieval"
        name  = "ice_height"
      },
      {
        group = "/Retrieval"
        name  = "dust_height"
      },
      {
        group = "/Retrieval"
        name  = "h2o_scale"
      },
      {
        group = "/Retrieval"
        name  = "deltaT"
      },
      {
        group = "/Retrieval"
        name  = "albedo_o2a"
      },
      {
        group = "/Retrieval"
        name  = "albedo_wco2"
      },
      {
        group = "/Retrieval"
        name  = "albedo_sco2"
      },
      {
        group = "/Retrieval"
        name  = "albedo_slope_o2a"
      },
      {
        group = "/Retrieval"
        name  = "albedo_slope_wco2"
      },
      {
        group = "/Retrieval"
        name  = "albedo_slope_sco2"
      },
      {
        group = "/Retrieval"
        name  = "chi2_o2a"
      },
      {
        group = "/Retrieval"
        name  = "chi2_wco2"
      },
      {
        group = "/Retrieval"
        name  = "chi2_sco2"
      },
      {
        group = "/Retrieval"
        name  = "rms_rel_o2a"
      },
      {
        group = "/Retrieval"
        name  = "rms_rel_wco2"
      },
      {
        group = "/Retrieval"
        name  = "rms_rel_sco2"
      },
      {
        group = "/Retrieval"
        name  = "iterations"
      },
      {
        group = "/Retrieval"
        name  = "diverging_steps"
      },
      {
        group = "/Retrieval"
        name  = "dof_co2"
      },
      {
        group = "/Retrieval"
        name  = "xco2_zlo_bias"
      },
      {
        group = "/Sounding"
        name  = "solar_azimuth_angle"
      },
      {
        group = "/Sounding"
        name  = "sensor_azimuth_angle"
      },
      {
        group = "/Sounding"
        name  = "pma_elevation_angle"
      },
      {
        group = "/Sounding"
        name  = "pma_azimuth_angle"
      },
      {
        group = "/Sounding"
        name  = "polarization_angle"
      },
      {
        group = "/Sounding"
        name  = "att_data_source"
      },
      {
        group = "/Sounding"
        name  = "land_fraction"
      },
      {
        group = "/Sounding"
        name  = "glint_angle"
      },
      {
        group = "/Sounding"
        name  = "airmass"
      },
      {
        group = "/Sounding"
        name  = "snr_o2a"
      },
      {
        group = "/Sounding"
        name  = "snr_wco2"
      },
      {
        group = "/Sounding"
        name  = "snr_sco2"
      },
      {
        group = "/Sounding"
        name  = "footprint"
      },
      {
        group = "/Sounding"
        name  = "land_water_indicator"
      },
      {
        group = "/Sounding"
        name  = "altitude"
      },
      {
        group = "/Sounding"
        name  = "altitude_stddev"
      },
      {
        group = "/Sounding"
        name  = "zlo_wco2"
      }
    ]
    exclude-groups = [
      "/Preprocessors",
      "/Meteorology",
      "/Sounding",
      "/Retrieval"
    ]
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
    target-file : var.global_product ? null : "/var/targets.json"
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
        value = "C2237732007-GES_DISC"
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
        value = var.verbose ? "10" : "20"
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
      var.verbose ? ["-v"] : []
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
      var.mprof_interval != -1 ? [
        {
          name  = "DEBUG_MPROF"
          value = tostring(var.mprof_interval)
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
    command          = ["python", "/tools/s3Sync/sync.py"]
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

  sfn_definition = <<EOF
{
  "StartAt": "check_executions",
  "States": {
    "check_executions": {
      "Type": "Task",
      "Next": "concurrency_check",
      "Parameters": {
        "StateMachineArn": "arn:${data.aws_partition.current.id}:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.sfn_name}",
        "StatusFilter": "RUNNING"
      },
      "Resource": "arn:aws:states:::aws-sdk:sfn:listExecutions",
      "Comment": "List all running executions of this state machine",
      "ResultSelector": {
        "runningExecutionsCount.$": "States.ArrayLength($.Executions)",
        "firstExecution.$": "$.Executions[-1:]"
      },
      "ResultPath": "$.TaskResult"
    },
    "concurrency_check": {
      "Type": "Choice",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.TaskResult.firstExecution[0].Name",
              "StringEqualsPath": "$$.Execution.Name"
            }
          ],
          "Next": "init",
          "Comment": "We're good. This is the first/only current execution."
        }
      ],
      "Default": "SKIP_CONCURRENT_EXECUTION",
      "Comment": "Check that this is the first running execution"
    },
    "SKIP_CONCURRENT_EXECUTION": {
      "Type": "Succeed",
      "Comment": "We don't want this state machine to be run concurrently, so just exit this execution here"
    },
    "init": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.init_lambda.arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "Comment": "This really shouldn't ever fail (but has). Retry a couple times"
        }
      ],
      "Next": "cmr_searches",
      "Comment": "Copies RC template from S3 to EFS\nPurges granule stage dir and other temp files in EFS",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "cmr_searches",
          "Comment": "If this fails, it's likely not a very big deal, try to proceed without"
        }
      ]
    },
    "cmr_searches": {
      "Type": "Parallel",
      "Next": "stac_filter",
      "Branches": [
        {
          "StartAt": "cmr_search_oco2",
          "States": {
            "cmr_search_oco2": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Parameters": {
                "JobName": "cmr_search",
                "JobDefinition": "${aws_batch_job_definition.batch_job_def_search.arn}",
                "JobQueue": "${aws_batch_job_queue.batch_queue.arn}",
                "ContainerOverrides": {
                  "Environment": [
                    {
                      "Name": "OUTPUT_FILE",
                      "Value": "/tmp/cmr-results-oco2.json"
                    },
                    {
                      "Name": "COLLECTION_ID",
                      "Value": "C2716248872-GES_DISC"
                    }
                  ]
                }
              },
              "Comment": "Queries CMR for granules in collection",
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "oco2_search_fail",
                  "Comment": "General error catch"
                }
              ],
              "Next": "err_check_0_oco2"
            },
            "err_check_0_oco2": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Container.ExitCode",
                  "NumericEquals": 0,
                  "Next": "oco2_search_success"
                }
              ],
              "Default": "oco2_search_fail"
            },
            "oco2_search_success": {
              "Type": "Succeed"
            },
            "oco2_search_fail": {
              "Type": "Fail"
            }
          }
        },
        {
          "StartAt": "cmr_search_oco3",
          "States": {
            "cmr_search_oco3": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Parameters": {
                "JobName": "cmr_search",
                "JobDefinition": "${aws_batch_job_definition.batch_job_def_search.arn}",
                "JobQueue": "${aws_batch_job_queue.batch_queue.arn}"
              },
              "Comment": "Queries CMR for granules in collection",
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "oco3_search_fail",
                  "Comment": "General error catch"
                }
              ],
              "Next": "err_check_0_oco3"
            },
            "err_check_0_oco3": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Container.ExitCode",
                  "NumericEquals": 0,
                  "Next": "oco3_search_success"
                }
              ],
              "Default": "oco3_search_fail"
            },
            "oco3_search_success": {
              "Type": "Succeed"
            },
            "oco3_search_fail": {
              "Type": "Fail"
            }
          }
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "fail_msg_search"
        }
      ]
    },
    "fail_msg_search": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "CMR search failed"
      },
      "Comment": "SNS message formatting"
    },
    "stac_filter": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
        "Payload": {
          "phase": 1
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "err_check_1",
      "Comment": "Filter out granules that have already been processed according to state file on EFS. Also truncate if desired to limit batch size."
    },
    "err_check_1": {
      "Type": "Choice",
      "Default": "fail_msg_filter",
      "Choices": [
        {
          "Variable": "$.ExitCode",
          "NumericEquals": 0,
          "Next": "stage_granules",
          "Comment": "Completed ok?"
        },
        {
          "Variable": "$.ExitCode",
          "NumericEquals": 255,
          "Next": "NO_NEW_DATA",
          "Comment": "Nothing new, so we're done here"
        }
      ],
      "Comment": "Check if previous step completed successfully"
    },
    "fail_msg_filter": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Unable to filter granules"
      },
      "Comment": "SNS message formatting"
    },
    "notify_fail_general": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.oco3-transform-notify.arn}",
        "Message.$": "$.Message",
        "Subject": "OCO-3 Processing failed"
      },
      "Next": "FAIL_GENERAL",
      "Comment": "Publish a message to SNS that we've failed"
    },
    "FAIL_GENERAL": {
      "Type": "Fail",
      "Error": "FAIL_GENERAL",
      "Cause": "Any non-critical failure (ie, failing outside of a final Zarr write)",
      "Comment": "General failure"
    },
    "stage_granules": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {
        "JobName": "granule_stage",
        "JobDefinition": "${aws_batch_job_definition.batch_job_def_stage.arn}",
        "JobQueue": "${aws_batch_job_queue.batch_queue.arn}"
      },
      "Next": "err_check_2",
      "Comment": "Download the granules to be processed to EFS",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "fail_msg_stage",
          "Comment": "General error catch"
        }
      ]
    },
    "err_check_2": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Container.ExitCode",
          "NumericEquals": 0,
          "Next": "rc_prep"
        }
      ],
      "Default": "fail_msg_stage",
      "Comment": "Check if previous step completed successfully"
    },
    "fail_msg_stage": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Granule download failed"
      },
      "Comment": "SNS message formatting"
    },
    "rc_prep": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
        "Payload": {
          "phase": 2
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "err_check_3",
      "Comment": "Produce job config file for data process container"
    },
    "err_check_3": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.ExitCode",
          "NumericEquals": 0,
          "Next": "process_data"
        }
      ],
      "Default": "fail_msg_prep",
      "Comment": "Check if previous step completed successfully"
    },
    "fail_msg_prep": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Failed to prepare run config file"
      },
      "Comment": "SNS message formatting"
    },
    "process_data": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {
        "JobDefinition": "${aws_batch_job_definition.batch_job_def_process.arn}",
        "JobQueue": "${aws_batch_job_queue.batch_queue.arn}",
        "JobName": "process_pipeline"
      },
      "Next": "err_check_4",
      "Comment": "Process input data and write to S3",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "pipeline_fail_eval",
          "Comment": "General error catch"
        }
      ]
    },
    "err_check_4": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Container.ExitCode",
          "NumericEquals": 0,
          "Next": "s3_sync",
          "Comment": "Completed ok?"
        }
      ],
      "Default": "pipeline_fail_eval",
      "Comment": "Check if previous step completed successfully"
    },
    "s3_sync": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {
        "JobName": "s3_sync",
        "JobDefinition": "${aws_batch_job_definition.batch_job_def_push.arn}",
        "JobQueue": "${aws_batch_job_queue.batch_queue.arn}"
      },
      "Next": "cleanup",
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "BackoffRate": 2,
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "Comment": "Keep attempting to push to S3 if something goes wrong."
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Comment": "Cannot push to S3 for some reason...",
          "Next": "cleanup_failed_bad_push"
        }
      ],
      "Comment": "If the process job wrote to EFS as an intermediate store, sync the EFS output to S3"
    },
    "cleanup_failed_bad_push": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload": {
          "phase": 4
        },
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "fail_msg_bad_push",
      "Comment": "Was unable to sync. Cleanup but do not update state. (TODO: This is probably the wrong course of action as the next run will add a duplicate section to the data in EFS. Try to come up with a better course of action for sync failure.)"
    },
    "fail_msg_bad_push": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Failed to push data to S3"
      },
      "Comment": "SNS message formatting"
    },
    "pipeline_fail_eval": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${aws_lambda_function.eval_lambda.arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "fail_type_check",
      "Comment": "Checks if zarr backup info file exists. If it does, this indicates a critical failure during final result write and the data is most likely corrupted and a backup will need to be restored."
    },
    "fail_type_check": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.FailureType",
          "StringEquals": "CRITICAL",
          "Next": "restore_backup",
          "Comment": "Failed critically"
        }
      ],
      "Default": "cleanup_failed",
      "Comment": "Branch based on failure type"
    },
    "cleanup_failed": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
        "Payload": {
          "phase": 4
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "fail_msg_pipeline",
      "Comment": "PIpeline failed, clean up but do not update state"
    },
    "restore_backup": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Parameters": {
        "JobName": "backup_restore",
        "JobDefinition": "${aws_batch_job_definition.batch_job_def_restore.arn}",
        "JobQueue": "${aws_batch_job_queue.batch_queue.arn}"
      },
      "Next": "err_check_5",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "get_trigger_schedule",
          "Comment": "General error catch"
        }
      ],
      "Comment": "Attempts to replace corrupted zarr stores with backups in S3 at the locations given in the file on EFS"
    },
    "err_check_5": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Container.ExitCode",
          "NumericEquals": 0,
          "Next": "cleanup_failed_post_crit",
          "Comment": "Restored ok?"
        }
      ],
      "Default": "get_trigger_schedule",
      "Comment": "Was the backup restore successful?"
    },
    "cleanup_failed_post_crit": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
        "Payload": {
          "phase": 4
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "fail_msg_backup",
      "Comment": "The data is ok, but this run still failed, clean up files but do not update state file"
    },
    "fail_msg_backup": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Data processing failed during critical phase. Output data was restored from backup successfully."
      },
      "Comment": "SNS message formatting"
    },
    "get_trigger_schedule": {
      "Type": "Task",
      "Next": "disable_trigger_schedule",
      "Parameters": {
        "Name": "${local.schedule_name}"
      },
      "Resource": "arn:aws:states:::aws-sdk:scheduler:getSchedule",
      "Comment": "Needed to fill the required params in the update step",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "notify_fail_critical_no_disable"
        }
      ]
    },
    "disable_trigger_schedule": {
      "Type": "Task",
      "Next": "notify_fail_critical",
      "Parameters": {
        "ActionAfterCompletion.$": "$.ActionAfterCompletion",
        "Description.$": "$.Description",
        "FlexibleTimeWindow": {
          "Mode.$": "$.FlexibleTimeWindow.Mode"
        },
        "GroupName.$": "$.GroupName",
        "Name.$": "$.Name",
        "ScheduleExpression.$": "$.ScheduleExpression",
        "ScheduleExpressionTimezone.$": "$.ScheduleExpressionTimezone",
        "Target": {
          "Arn.$": "$.Target.Arn",
          "RoleArn.$": "$.Target.RoleArn",
          "RetryPolicy": {
            "MaximumEventAgeInSeconds.$": "$.Target.RetryPolicy.MaximumEventAgeInSeconds",
            "MaximumRetryAttempts.$": "$.Target.RetryPolicy.MaximumRetryAttempts"
          }
        },
        "State": "DISABLED"
      },
      "Resource": "arn:aws:states:::aws-sdk:scheduler:updateSchedule",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "notify_fail_critical_no_disable",
          "Comment": "Couldn't update"
        }
      ],
      "Comment": "Disable EventBridge schedule that triggers this state machine."
    },
    "notify_fail_critical_no_disable": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.oco3-transform-notify.arn}",
        "Message": "OCO-3 pipeline failed during final result write. This very likely caused the output data to be corrupted. Tried and failed to restore from backup. No further data will be processed until this is manually evaluated.\nWas unable to disable automatic trigger; will only get worse. Intervention is REQUIRED.",
        "Subject": "[CRITICAL] OCO-3 Processing failed in result write - Could not disable"
      },
      "Next": "FAIL_CRITICAL",
      "Comment": "Publish a message to SNS that we've failed at a really really bad time AND failed to prevent making it worse"
    },
    "notify_fail_critical": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sns:publish",
      "Parameters": {
        "TopicArn": "${aws_sns_topic.oco3-transform-notify.arn}",
        "Message": "OCO-3 pipeline failed during final result write. This very likely caused the output data to be corrupted. Tried and failed to restore from backup. No further data will be processed until this is manually evaluated.",
        "Subject": "[CRITICAL] OCO-3 Processing failed in result write"
      },
      "Next": "FAIL_CRITICAL",
      "Comment": "Publish a message to SNS that we've failed at a really really bad time"
    },
    "FAIL_CRITICAL": {
      "Type": "Fail",
      "Error": "FAIL_CRITICAL",
      "Cause": "Failed during final Zarr write but could not restore backup. The output data is corrupted or gone.",
      "Comment": "Critical failure. Really do not want to be here!"
    },
    "fail_msg_pipeline": {
      "Type": "Pass",
      "Next": "notify_fail_general",
      "Result": {
        "Message": "Data processing failed at a non-critical point"
      },
      "Comment": "SNS message formatting"
    },
    "cleanup": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
        "Payload": {
          "phase": 3
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "SUCCESS",
      "Comment": "Delete staged granules, and temp files from EFS\nUpdate state JSON with granules that were processed"
    },
    "SUCCESS": {
      "Type": "Succeed",
      "Comment": "We're done, successfully! "
    },
    "NO_NEW_DATA": {
      "Type": "Succeed",
      "Comment": "There's nothing new to process..."
    }
  },
  "Comment": "Queries NASA Earthdata CMR for OCO-3 data granules, filters out a list of granules to be processed based on a state file in EFS and an optional, configurable limit, download the files to EFS, then process them into gridded SAM data, which is written as Zarr to S3. Downloaded data and temp files are then cleaned up. If a stage fails, a notice message is published to SNS."
}
EOF
}
