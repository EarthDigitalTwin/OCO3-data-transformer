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
          "Next": "check_efs_alarm",
          "Comment": "We're good. This is the first/only current execution."
        }
      ],
      "Default": "SKIP_CONCURRENT_EXECUTION",
      "Comment": "Check that this is the first running execution"
    },
    "check_efs_alarm": {
      "Type": "Task",
      "Parameters": {
        "AlarmNames": [
          "${aws_cloudwatch_metric_alarm.efs_burst_alarm.alarm_name}"
        ],
        "MaxRecords": 1
      },
      "Resource": "arn:aws:states:::aws-sdk:cloudwatch:describeAlarms",
      "Next": "evaluate_alarm_state",
      "ResultSelector": {
        "state.$": "$.MetricAlarms[0].StateValue"
      },
      "Comment": "Check the EFS alarm"
    },
    "evaluate_alarm_state": {
      "Type": "Choice",
      "Choices": [
        {
          "Not": {
            "Variable": "$.state",
            "StringEquals": "ALARM"
          },
          "Next": "init",
          "Comment": "If we're not in alarm state, continue with the execution"
        }
      ],
      "Default": "EFS_BACKOFF",
      "Comment": "Evaluate the state of the EFS alarm"
    },
    "EFS_BACKOFF": {
      "Type": "Succeed",
      "Comment": "EFS does not have enough burst credits to comfortable accommodate an execution, back off until it sufficiently recovers."
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
                      "Value": "C2912085112-GES_DISC"
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
              "Next": "err_check_0_oco2",
              "Retry": [
                {
                  "ErrorEquals": [
                    "Batch.ServerException"
                  ],
                  "BackoffRate": 2,
                  "IntervalSeconds": 5,
                  "MaxAttempts": 3,
                  "Comment": "Seeing an uptick in batch internal server failures (500) on submit. Retry these.",
                  "JitterStrategy": "FULL"
                }
              ]
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
              "Next": "err_check_0_oco3",
              "Retry": [
                {
                  "ErrorEquals": [
                    "Batch.ServerException"
                  ],
                  "BackoffRate": 2,
                  "IntervalSeconds": 5,
                  "MaxAttempts": 3,
                  "Comment": "Seeing an uptick in batch internal server failures (500) on submit. Retry these.",
                  "JitterStrategy": "FULL"
                }
              ]
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
        },
        {
          "StartAt": "cmr_search_oco3_sif",
          "States": {
            "cmr_search_oco3_sif": {
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
                      "Value": "/tmp/cmr-results-oco3_sif.json"
                    },
                    {
                      "Name": "COLLECTION_ID",
                      "Value": "C2910085832-GES_DISC"
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
                  "Next": "oco3_sif_search_fail",
                  "Comment": "General error catch"
                }
              ],
              "Next": "err_check_0_oco3_sif",
              "Retry": [
                {
                  "ErrorEquals": [
                    "Batch.ServerException"
                  ],
                  "BackoffRate": 2,
                  "IntervalSeconds": 5,
                  "MaxAttempts": 3,
                  "Comment": "Seeing an uptick in batch internal server failures (500) on submit. Retry these.",
                  "JitterStrategy": "FULL"
                }
              ]
            },
            "err_check_0_oco3_sif": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.Container.ExitCode",
                  "NumericEquals": 0,
                  "Next": "oco3_sif_search_success"
                }
              ],
              "Default": "oco3_sif_search_fail"
            },
            "oco3_sif_search_success": {
              "Type": "Succeed"
            },
            "oco3_sif_search_fail": {
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
        "Subject": "OCO-3 Processing failed${local.sns_subject_ws_id}"
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
      ],
      "Retry": [
        {
          "ErrorEquals": [
            "Batch.ServerException"
          ],
          "BackoffRate": 2,
          "IntervalSeconds": 5,
          "MaxAttempts": 3,
          "Comment": "Seeing an uptick in batch internal server failures (500) on submit. Retry these.",
          "JitterStrategy": "FULL"
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
        "Message": "Data staging failed or some other error occurred when preparing processing step run configuration YAML file"
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
      ],
      "Retry": [
        {
          "ErrorEquals": [
            "Batch.ServerException"
          ],
          "BackoffRate": 2,
          "IntervalSeconds": 5,
          "MaxAttempts": 3,
          "Comment": "Seeing an uptick in batch internal server failures (500) on submit. Retry these.",
          "JitterStrategy": "FULL"
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
        "Subject": "[CRITICAL] OCO-3 Processing failed during result write - Could not disable${local.sns_subject_ws_id}"
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
        "Subject": "[CRITICAL] OCO-3 Processing failed during result write${local.sns_subject_ws_id}"
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