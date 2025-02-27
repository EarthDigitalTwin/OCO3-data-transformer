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

//// DO NOT CHANGE THESE ////
deployment_name = "global" //
global_product  = true     //
/////////////////////////////

// Filesystem access config
efs_ap_id = "fsap-xxxxxxxxxxxxxxxxx"

// Prefixes and paths for S3 outputs and configuration files
s3_outputs_prefix      = ""
s3_rc_template_key     = ""
s3_data_gap_config_key = ""

// Granule process limit
input_granule_limit = 150

// Output config
output_ds_name              = ""
output_interpolation_method = "linear"
output_chunk_time           = 14
output_chunk_lat            = 500
output_chunk_lon            = 500
output_grid_lat_resolution  = 1800

// Scheduling
disable_schedule   = false
schedule_frequency = "quarter-hourly"

// Parallelism config (will possibly be deprecated - used for memory footprint control)
interpolate_max_parallel_global = 4
