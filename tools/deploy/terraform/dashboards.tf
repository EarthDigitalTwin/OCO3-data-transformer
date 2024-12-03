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
  global_dashboard_definition = <<EOF
{
    "start": "-PT15M",
    "widgets": [
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Total Process Counts", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - Total",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO3_SIF", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO3 SIF",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 6,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO3", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO3",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO2", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO2",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 6,
            "width": 4,
            "y": 14,
            "x": 14,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "AWS/EFS", "BurstCreditBalance", "FileSystemId", "${local.fs_dep_id}" ]
                ],
                "region": "${data.aws_region.current.name}",
                "title": "EFS Filesystem Write Capacity",
                "period": 300,
                "start": "-PT24H",
                "end": "P0D"
            }
        },
        {
            "height": 2,
            "width": 2,
            "y": 18,
            "x": 12,
            "type": "alarm",
            "properties": {
                "title": "EFS Filesystem Capacity Alarm",
                "alarms": [
                    "${aws_cloudwatch_metric_alarm.efs_burst_alarm.arn}"
                ]
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 6,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Completion", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "title": "Completion - %",
                "period": 5,
                "stat": "Maximum",
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100,
                        "label": ""
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process CPU Utilization", { "yAxis": "left", "label": "CPU Utilization" } ],
                    [ ".", "Process Memory Utilization", "Memory", "Physical Memory", { "label": "Physical Memory", "yAxis": "right" } ],
                    [ "...", "Virtual Memory", { "label": "Virtual Memory", "yAxis": "right" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "yAxis": {
                    "left": {
                        "min": 0,
                        "label": "CPU Utilization"
                    },
                    "right": {
                        "label": "Memory Usage"
                    }
                },
                "stat": "Average",
                "period": 5,
                "title": "Process Task Performance",
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 18,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "${local.metrics_namespace}", "Execution time" ]
                ],
                "region": "${data.aws_region.current.name}",
                "period": 300,
                "start": "-PT24H",
                "end": "P0D",
                "liveData": true
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 14,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "CMR Last Feature Count", "Count", "Features", { "label": "Total CMR Feature Count", "stat": "Maximum" } ],
                    [ ".", "Last New Granule Count", ".", "Granules", { "label": "Remaining Granules" } ],
                    [ ".", "Last Remaining Data Day Count", ".", "Days", { "label": "Remaining Data Days" } ]
                ],
                "sparkline": false,
                "view": "singleValue",
                "region": "${data.aws_region.current.name}",
                "stat": "Minimum",
                "period": 300,
                "singleValueFullPrecision": true,
                "title": "Input Data Counts",
                "start": "-PT24H",
                "end": "P0D",
                "liveData": true
            }
        },
        {
            "type": "metric",
            "x": 12,
            "y": 14,
            "width": 2,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "AWS/EFS", "StorageBytes", "StorageClass", "Total", "FileSystemId", "${local.fs_dep_id}", { "region": "${data.aws_region.current.name}" } ]
                ],
                "sparkline": false,
                "view": "singleValue",
                "region": "${data.aws_region.current.name}",
                "period": 300,
                "stat": "Maximum",
                "liveData": false,
                "singleValueFullPrecision": false,
                "title": "EFS Current Size"
            }
        }
    ]
}
EOF

  tfp_dashboard_definition = <<EOF
{
    "start": "-PT15M",
    "widgets": [
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Total Process Counts", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - Total",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO3_SIF", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO3 SIF",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 6,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO3", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO3",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Counts", "Dataset", "OCO2", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "stat": "Maximum",
                "period": 5,
                "title": "Days Processed - OCO2",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "height": 6,
            "width": 4,
            "y": 14,
            "x": 14,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "AWS/EFS", "BurstCreditBalance", "FileSystemId", "${local.fs_dep_id}" ]
                ],
                "region": "${data.aws_region.current.name}",
                "title": "EFS Filesystem Write Capacity",
                "period": 300,
                "start": "-PT24H",
                "end": "P0D"
            }
        },
        {
            "height": 2,
            "width": 2,
            "y": 18,
            "x": 12,
            "type": "alarm",
            "properties": {
                "title": "EFS Filesystem Capacity Alarm",
                "alarms": [
                    "${aws_cloudwatch_metric_alarm.efs_burst_alarm.arn}"
                ]
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 6,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "${local.metrics_namespace}", "Process Completion" ],
                    [ ".", "Write Completion" ]
                ],
                "region": "${data.aws_region.current.name}",
                "title": "Completion - %",
                "yAxis": {
                    "left": {
                        "min": 0,
                        "max": 100,
                        "label": ""
                    }
                },
                "period": 5,
                "stat": "Maximum",
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Process CPU Utilization", { "yAxis": "left", "label": "CPU Utilization" } ],
                    [ ".", "Process Memory Utilization", "Memory", "Physical Memory", { "label": "Physical Memory", "yAxis": "right" } ],
                    [ "...", "Virtual Memory", { "label": "Virtual Memory", "yAxis": "right" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "yAxis": {
                    "left": {
                        "label": "CPU Utilization",
                        "min": 0
                    },
                    "right": {
                        "label": "Memory Usage"
                    }
                },
                "stat": "Average",
                "period": 5,
                "title": "Process Task Performance",
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 0,
            "x": 18,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "${local.metrics_namespace}", "Execution time" ]
                ],
                "region": "${data.aws_region.current.name}",
                "period": 300,
                "start": "-PT24H",
                "end": "P0D",
                "liveData": true
            }
        },
        {
            "height": 7,
            "width": 6,
            "y": 7,
            "x": 18,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "Total Write Counts", "Count", "Completed", { "region": "${data.aws_region.current.name}" } ],
                    [ "...", "Total", { "region": "${data.aws_region.current.name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${data.aws_region.current.name}",
                "title": "Write Tasks",
                "period": 5,
                "stat": "Maximum",
                "yAxis": {
                    "left": {
                        "min": 0
                    }
                },
                "liveData": true
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 14,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "${local.metrics_namespace}", "CMR Last Feature Count", "Count", "Features", { "region": "${data.aws_region.current.name}", "yAxis": "left", "stat": "Maximum", "label": "Total CMR Feature Count" } ],
                    [ ".", "Last New Granule Count", ".", "Granules", { "region": "${data.aws_region.current.name}", "label": "Remaining Granules" } ],
                    [ ".", "Last Remaining Data Day Count", ".", "Days", { "region": "${data.aws_region.current.name}", "label": "Remaining Data Days" } ]
                ],
                "sparkline": false,
                "view": "singleValue",
                "region": "${data.aws_region.current.name}",
                "singleValueFullPrecision": true,
                "start": "-PT24H",
                "end": "P0D",
                "period": 300,
                "stat": "Minimum",
                "title": "Input Data Counts",
                "liveData": true
            }
        },
        {
            "type": "metric",
            "x": 12,
            "y": 14,
            "width": 2,
            "height": 4,
            "properties": {
                "metrics": [
                    [ "AWS/EFS", "StorageBytes", "StorageClass", "Total", "FileSystemId", "${local.fs_dep_id}", { "region": "${data.aws_region.current.name}" } ]
                ],
                "sparkline": false,
                "view": "singleValue",
                "region": "${data.aws_region.current.name}",
                "period": 300,
                "stat": "Maximum",
                "liveData": false,
                "singleValueFullPrecision": false,
                "title": "EFS Current Size"
            }
        }
    ]
}
EOF
}
