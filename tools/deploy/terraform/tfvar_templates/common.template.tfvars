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

// NASA Earthdata login
edl_username = ""
edl_password = ""

// The deployed resources should be marked as testing (affects their names)
testing = false

// Image and image tag of the OCO transform software & utils to deploy
image     = ""
image_tag = ""

// IAM roles for various functions
iam_lambda_role         = ""
iam_task_role           = ""
iam_task_execution_role = ""
iam_step_role           = ""
iam_events_role         = ""

// AWS VPC & Networking resource names
subnet     = "subnet-xxxxxxxxxxxxxxxxx"
compute_sg = "sg-xxxxxxxxxxxxxxxxx"
efs_sg     = "sg-xxxxxxxxxxxxxxxxx"

// Additional AWS resource names
s3_bucket     = ""
batch_ce_name = ""
efs_fs_id     = "fs-xxxxxxxxxxxxxxxxx"

// Configuration for data gaps
s3_data_gap_config_source = ""

// Logging configurations
mprof_interval     = 10
log_retention_time = 7
verbose            = false

// Desired output variables. Sample provided
variables = {
  oco3 = [
    {
      group = "/"
      name  = "xco2"
    }
  ]
  oco2 = [
    {
      group = "/"
      name  = "xco2"
    }
  ]
  oco3_sif = [
    {
      group = "/"
      name  = "Daily_SIF_757nm"
    }
  ]
}

// Sources for target definitions on local FS
// Only used for target-focused deploys but they are required so need to be defined regardless of mode
s3_oco3_target_config_source = ""
s3_oco2_target_config_source = ""
