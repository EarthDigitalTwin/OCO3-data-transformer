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

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.34.0"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-west-2"
  default_tags {
    tags = {
      DeploymentName = "OCO3 Transformer"
      TestDeploy     = tostring(var.testing)
    }
  }
}

check "deployment_or_default" {
  assert {
    condition     = terraform.workspace == "default" || var.deployment_name == terraform.workspace
    error_message = "Configured deployment name for non-default workspace disagrees with workspace name"
  }
}

check "default" {
  assert {
    condition     = terraform.workspace != "default"
    error_message = "You are about to deploy using the default workspace. This should only be used for development purposes"
  }
}

/*

AWS

*/

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

/*

Secrets Manager

*/

resource "aws_secretsmanager_secret" "edl_secret" {
  name                    = "${local.name_root}secret"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "edl_secret" {
  secret_id     = aws_secretsmanager_secret.edl_secret.id
  secret_string = var.edl_password
}

/*

IAM

*/

data "aws_iam_role" "iam_lambda_role" {
  name = var.iam_lambda_role
}

data "aws_iam_role" "iam_task_role" {
  name = var.iam_task_role
}

data "aws_iam_role" "iam_task_execution_role" {
  name = var.iam_task_execution_role
}

data "aws_iam_role" "iam_step_role" {
  name = var.iam_step_role
}

data "aws_iam_role" "iam_events_role" {
  name = var.iam_events_role
}

/*

Networking

*/

data "aws_security_group" "compute_sg" {
  id = var.compute_sg
}

data "aws_security_group" "efs_sg" {
  id = var.efs_sg
}

data "aws_subnet" "subnet" {
  id = var.subnet
}

/*

S3

*/

data "aws_s3_bucket" "s3_bucket" {
  bucket = var.s3_bucket
}

data "local_file" "oco3_target_file" {
  filename = var.s3_oco3_target_config_source
}

data "local_file" "oco2_target_file" {
  filename = var.s3_oco2_target_config_source

  count = fileexists(var.s3_oco2_target_config_source) ? 1 : 0
}

data "local_file" "data_gaps" {
  filename = var.s3_data_gap_config_source

  count = var.s3_data_gap_config_source != null ? 1 : 0
}

resource "aws_s3_object" "rct" {
  bucket  = data.aws_s3_bucket.s3_bucket.id
  key     = var.s3_rc_template_key
  content = local.rc_template
}

resource "aws_s3_object" "oco3_target_json" {
  bucket  = data.aws_s3_bucket.s3_bucket.id
  key     = var.s3_oco3_target_config_key
  content = data.local_file.oco3_target_file.content

  count = var.global_product ? 0 : 1
}

resource "aws_s3_object" "oco2_target_json" {
  bucket  = data.aws_s3_bucket.s3_bucket.id
  key     = var.s3_oco2_target_config_key
  content = data.local_file.oco2_target_file[0].content

  count = (var.global_product || !fileexists(var.s3_oco2_target_config_source)) ? 0 : 1
}

resource "aws_s3_object" "data_gaps" {
  bucket  = data.aws_s3_bucket.s3_bucket.id
  key     = var.s3_data_gap_config_key
  content = data.local_file.data_gaps[0].content

  count = var.s3_data_gap_config_source != null ? 1 : 0
}

/*

CloudWatch Logs

*/

resource "aws_cloudwatch_log_group" "log_group" {
  name              = "${local.name_root}transform-logs"
  retention_in_days = var.log_retention_time
  skip_destroy      = false

  lifecycle {
    ignore_changes = [
      tags,
      #      retention_in_days
    ]
  }
}

/*

CloudWatch Alarm

*/

# TODO: Make threshold configurable?
resource "aws_cloudwatch_metric_alarm" "efs_burst_alarm" {
  alarm_name          = "${local.name_root}fs-burst-alarm"
  alarm_description   = "Alarm if FS doesn't have enough burst credits to comfortably process ~1yr of OCO data"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  period              = 60
  statistic           = "Average"
  datapoints_to_alarm = 1
  treat_missing_data  = "notBreaching"
  actions_enabled     = false
  namespace           = "AWS/EFS"
  metric_name         = "BurstCreditBalance"
  unit                = "Bytes"
  threshold           = 100000000000 # Margin of 100 GB
  dimensions = {
    FileSystemId = var.efs_fs_id
  }
}

/*

SNS

*/

resource "aws_sns_topic" "oco3-transform-notify" {
  name = "${local.name_root}transform-notify"
}

resource "aws_sns_topic_policy" "oco3-transform-notify-policy" {
  arn    = aws_sns_topic.oco3-transform-notify.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

data "aws_iam_policy_document" "sns_topic_policy" {
  policy_id = "__default_policy_ID"
  version   = "2008-10-17"

  statement {
    actions = [
      "SNS:GetTopicAttributes",
      "SNS:SetTopicAttributes",
      "SNS:AddPermission",
      "SNS:RemovePermission",
      "SNS:DeleteTopic",
      "SNS:Subscribe",
      "SNS:ListSubscriptionsByTopic",
      "SNS:Publish"
    ]

    condition {
      test = "StringEquals"
      values = [
        data.aws_caller_identity.current.account_id
      ]
      variable = "AWS:SourceOwner"
    }

    effect = "Allow"

    principals {
      identifiers = ["*"]
      type        = "AWS"
    }

    resources = [
      aws_sns_topic.oco3-transform-notify.arn
    ]

    sid = "__default_statement_ID"
  }
}

/*

EFS

*/

/*
resource "aws_efs_file_system" "efs_filesystem" {
  tags = {
    Name = "${local.name_root}transform-efs"
  }
  encrypted        = true
  performance_mode = "maxIO"

  protection {
    replication_overwrite = "ENABLED"
  }
}

resource "aws_efs_mount_target" "mnt_target" {
  file_system_id  = aws_efs_file_system.efs_filesystem.id
  subnet_id       = data.aws_subnet.subnet.id
  security_groups = [data.aws_security_group.efs_sg.id]
}

resource "aws_efs_access_point" "compute_access_point" {
  file_system_id = aws_efs_file_system.efs_filesystem.id

  root_directory {
    path = "/transform"
    creation_info {
      owner_gid   = 1000
      owner_uid   = 1000
      permissions = "755"
    }
  }

  posix_user {
    gid = 1000
    uid = 1000
  }
}

locals {
  fs_dep          = aws_efs_file_system.efs_filesystem
  fs_dep_id       = aws_efs_file_system.efs_filesystem.id
  mount_point_dep = aws_efs_mount_target.mnt_target
  ap_dep          = aws_efs_access_point.compute_access_point
  ap_dep_arn      = aws_efs_access_point.compute_access_point.arn
  ap_dep_id       = aws_efs_access_point.compute_access_point.id
}

*/

data "aws_efs_file_system" "efs_filesystem" {
  file_system_id = var.efs_fs_id
}

data "aws_efs_access_point" "compute_access_point" {
  access_point_id = var.efs_ap_id
}

data "aws_efs_mount_target" "mnt_target" {
  access_point_id = data.aws_efs_access_point.compute_access_point.id
}

locals {
  fs_dep          = data.aws_efs_file_system.efs_filesystem
  fs_dep_id       = data.aws_efs_file_system.efs_filesystem.id
  mount_point_dep = data.aws_efs_mount_target.mnt_target
  ap_dep          = data.aws_efs_access_point.compute_access_point
  ap_dep_arn      = data.aws_efs_access_point.compute_access_point.arn
  ap_dep_id       = data.aws_efs_access_point.compute_access_point.id
}

/*

Lambda

*/

data "local_file" "init_package" {
  filename = "../lambdas/init/package.zip"
}

data "local_file" "eval_package" {
  filename = "../lambdas/pipeline-fail-eval/package.zip"
}

data "local_file" "transform_package" {
  filename = "../lambda_deployment_package.zip"
}

data "local_file" "reset_package" {
  filename = "../lambdas/reset/package.zip"
}

resource "aws_lambda_function" "init_lambda" {
  function_name    = "${local.name_root}transform-copy-config-files"
  role             = data.aws_iam_role.iam_lambda_role.arn
  architectures    = ["x86_64"]
  filename         = data.local_file.init_package.filename
  source_code_hash = data.local_file.init_package.content_base64sha256
  handler          = "lambda_function.lambda_handler"
  memory_size      = 128
  runtime          = "python3.11"
  timeout          = 300
  depends_on       = [local.mount_point_dep, aws_s3_object.rct]

  environment {
    variables = {
      BUCKET           = var.s3_bucket
      RCT_KEY          = var.s3_rc_template_key
      TARGET_KEY       = var.global_product ? null : var.s3_oco3_target_config_key
      TARGET_KEY_OCO2  = var.global_product ? null : var.s3_oco2_target_config_key
      ZARR_BACKUP_FILE = "/mnt/transform/oco_pipeline_zarr_state/ZARR_WRITE.json"
      GAP_FILE         = var.s3_data_gap_config_source != null ? var.s3_data_gap_config_key : null
    }
  }

  file_system_config {
    arn              = local.ap_dep_arn
    local_mount_path = "/mnt/transform"
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.log_group.name
  }

  vpc_config {
    security_group_ids = [data.aws_security_group.compute_sg.id]
    subnet_ids         = [data.aws_subnet.subnet.id]
  }
}

resource "aws_lambda_function" "eval_lambda" {
  function_name    = "${local.name_root}transform-pipeline-fail-eval"
  role             = data.aws_iam_role.iam_lambda_role.arn
  architectures    = ["x86_64"]
  filename         = data.local_file.eval_package.filename
  source_code_hash = data.local_file.eval_package.content_base64sha256
  handler          = "lambda_function.lambda_handler"
  memory_size      = 128
  runtime          = "python3.11"
  timeout          = 30
  depends_on       = [local.mount_point_dep]

  environment {
    variables = {
      ZARR_BACKUP_FILE = "/mnt/transform/oco_pipeline_zarr_state/ZARR_WRITE.json"
    }
  }

  file_system_config {
    arn              = local.ap_dep_arn
    local_mount_path = "/mnt/transform"
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.log_group.name
  }

  vpc_config {
    security_group_ids = [data.aws_security_group.compute_sg.id]
    subnet_ids         = [data.aws_subnet.subnet.id]
  }
}

resource "aws_lambda_function" "transform_lambda" {
  function_name    = "${local.name_root}transform-lambda"
  role             = data.aws_iam_role.iam_lambda_role.arn
  architectures    = ["x86_64"]
  filename         = data.local_file.transform_package.filename
  source_code_hash = data.local_file.transform_package.content_base64sha256
  handler          = "run.lambda_handler"
  memory_size      = 128
  runtime          = "python3.11"
  timeout          = 300
  depends_on       = [local.mount_point_dep]

  environment {
    variables = {
      LAMBDA_GRANULE_LIMIT    = var.input_granule_limit
      LAMBDA_MOUNT_DIR        = "/mnt/transform/"
      LAMBDA_PHASE            = 1
      LAMBDA_RC_TEMPLATE      = "/mnt/transform/run-config.yaml"
      LAMBDA_STAC_PATH        = "/mnt/transform/"
      LAMBDA_STAGE_DIR        = "/mnt/transform/inputs/"
      LAMBDA_STATE            = var.global_product ? "/mnt/transform/state_global.json" : "/mnt/transform/state.json"
      LAMBDA_GAP_FILE         = var.s3_data_gap_config_source != null ? "/mnt/transform/gaps.json" : null
      LAMBDA_TARGET_FILE      = "/mnt/transform/targets.json"
      LAMBDA_TARGET_FILE_OCO2 = "/mnt/transform/targets_oco2.json"
      LAMBDA_SKIP             = join(",", var.skip_datasets)
    }
  }

  file_system_config {
    arn              = local.ap_dep_arn
    local_mount_path = "/mnt/transform"
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.log_group.name
  }

  vpc_config {
    security_group_ids = [data.aws_security_group.compute_sg.id]
    subnet_ids         = [data.aws_subnet.subnet.id]
  }
}

// Removes ALL files, will restart processing. To be triggered manually
resource "aws_lambda_function" "reset_lambda" {
  function_name    = "${local.name_root}reset"
  role             = data.aws_iam_role.iam_lambda_role.arn
  architectures    = ["x86_64"]
  filename         = data.local_file.reset_package.filename
  source_code_hash = data.local_file.reset_package.content_base64sha256
  handler          = "lambda_function.lambda_handler"
  memory_size      = 128
  runtime          = "python3.11"
  timeout          = 300
  depends_on       = [local.mount_point_dep, aws_s3_object.rct]

  environment {
    variables = {
      GLOBAL = var.global_product ? "true" : "false"
    }
  }

  file_system_config {
    arn              = local.ap_dep_arn
    local_mount_path = "/mnt/transform"
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.log_group.name
  }

  vpc_config {
    security_group_ids = [data.aws_security_group.compute_sg.id]
    subnet_ids         = [data.aws_subnet.subnet.id]
  }
}


/*

Batch

*/

data "aws_batch_compute_environment" "batch_ce" {
  compute_environment_name = var.batch_ce_name
}

resource "aws_batch_job_queue" "batch_queue" {
  compute_environments = [data.aws_batch_compute_environment.batch_ce.arn]
  name                 = "${local.name_root}transform-queue"
  priority             = 1
  state                = "ENABLED"
}

resource "aws_batch_job_definition" "batch_job_def_search" {
  name = "${local.name_root}transform-cmr-search"
  type = "container"

  platform_capabilities = [
    "FARGATE"
  ]

  depends_on = [local.mount_point_dep]

  container_properties = local.batch_definition_containerprops_search

  propagate_tags = true
}

resource "aws_batch_job_definition" "batch_job_def_stage" {
  name = "${local.name_root}transform-download-data"
  type = "container"

  platform_capabilities = [
    "FARGATE"
  ]

  depends_on = [local.mount_point_dep]

  container_properties = local.batch_definition_containerprops_stage

  propagate_tags = true
}

resource "aws_batch_job_definition" "batch_job_def_process" {
  name = "${local.name_root}transform-process-data"
  type = "container"

  platform_capabilities = [
    "FARGATE"
  ]

  depends_on = [local.mount_point_dep]

  container_properties = local.batch_definition_containerprops_process

  propagate_tags = true
}

resource "aws_batch_job_definition" "batch_job_def_push" {
  name = "${local.name_root}transform-s3-sync"
  type = "container"

  platform_capabilities = [
    "FARGATE"
  ]

  depends_on = [local.mount_point_dep]

  container_properties = local.batch_definition_containerprops_push

  propagate_tags = true
}

resource "aws_batch_job_definition" "batch_job_def_restore" {
  name = "${local.name_root}transform-restore-backup"
  type = "container"

  platform_capabilities = [
    "FARGATE"
  ]

  depends_on = [local.mount_point_dep]

  container_properties = local.batch_definition_containerprops_restore

  propagate_tags = true
}

/*

Step Functions

*/

resource "aws_sfn_state_machine" "state_machine" {
  name     = local.sfn_name
  role_arn = data.aws_iam_role.iam_step_role.arn
  type     = "STANDARD"
  logging_configuration {
    include_execution_data = false
    log_destination        = "${aws_cloudwatch_log_group.log_group.arn}:*"
    level                  = "ALL"
  }

  depends_on = [
    aws_batch_job_definition.batch_job_def_process,
    aws_batch_job_definition.batch_job_def_push,
    aws_batch_job_definition.batch_job_def_restore,
    aws_batch_job_definition.batch_job_def_search,
    aws_batch_job_definition.batch_job_def_stage,
    aws_batch_job_queue.batch_queue,
    aws_lambda_function.eval_lambda,
    aws_lambda_function.init_lambda,
    aws_lambda_function.transform_lambda
  ]

  definition = local.sfn_definition
}

/*

EventBridge

*/

resource "aws_scheduler_schedule" "schedule" {
  name                         = local.schedule_name
  description                  = "Schedule to regularly invoke process State Machine"
  schedule_expression          = local.invoke_frequency[var.schedule_frequency]
  schedule_expression_timezone = "America/Los_Angeles"
  state                        = !var.disable_schedule ? "ENABLED" : "DISABLED"

  target {
    arn      = aws_sfn_state_machine.state_machine.arn
    role_arn = data.aws_iam_role.iam_events_role.arn

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 0
    }
  }

  flexible_time_window {
    mode = "OFF"
  }
}
