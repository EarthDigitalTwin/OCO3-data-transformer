#!/bin/bash

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

if [ -f "run.env" ]; then
  echo Sourcing environment from run.env
  source run.env
else
  echo run.env not present, using current environment
fi

# Probe if Docker daemon is running

curl -s --unix-socket /var/run/docker.sock http/_ping 2>&1 >/dev/null
daemon_up=$?

if [ $daemon_up -ne 0 ]; then
  echo "Docker is not running! Will not proceed"
  exit 1
fi

SEARCH_YAML=${SEARCH_YAML:-dc-001-search-cmr-oco3.yaml}
DOWNLOAD_YAML=${DOWNLOAD_YAML:-dc-002-download.yaml}
GRANULE_LIMIT=${GRANULE_LIMIT:-50}

echo 'Running'

dt_start=$(TZ=UTC date -Iseconds | sed 's/://g;s/+.*$//')

python run.py \
  $([[ ! -z "$SEARCH_YAML" ]] && echo --stac-search-dc=$SEARCH_YAML) \
  $([[ ! -z "$DOWNLOAD_YAML" ]] && echo --stac-dl-dc=$DOWNLOAD_YAML) \
  $([[ ! -z "$PIPELINE_IMAGE" ]] && echo --pipeline-image=$PIPELINE_IMAGE) \
  $([[ ! -z "$RC_TEMPLATE" ]] && echo --rc-template=$RC_TEMPLATE) \
  $([[ ! -z "$STATE_FILE" ]] && echo --state=$STATE_FILE) \
  $([[ ! -z "$GRANULE_LIMIT" ]] && echo --limit=$GRANULE_LIMIT) \
  $([[ ! -z "$LOGGING_DIR" ]] && echo --logging=$LOGGING_DIR) \
  $([[ ! -z "$GAP_FILE" ]] && echo --gapfile=$GAP_FILE) \
  $([[ ! -z "$TARGET_FILE" ]] && echo --target-file-oco3=$TARGET_FILE) \
  $([[ ! -z "$TARGET_FILE_OCO2" ]] && echo --target-file-oco2=$TARGET_FILE_OCO2) \
  $([[ ! -z "$VERBOSE" ]] && echo -v) \
  $([[ ! -z "$SKIP" ]] && echo --skip=$SKIP) \

exit_code=$?

dt_end=$(TZ=UTC date -Iseconds | sed 's/+.*$//')

echo Script completed code $exit_code

if [ $exit_code -eq 255 ]; then
  echo 'Nothing noteworthy to log; dropping logs...'
  rm *.log
  exit 0
fi

echo Zipping up logs and cleaning up

S3_PATH=${S3_PATH:-s3://bucket/path}

zip -q9 ${dt_start}.logs.zip *.log

if [ -z ${KEEP+x} ]; then
  rm *.log
else
  echo 'Keeping logs; this should only be done for debugging purposes. unset KEEP to disable.'
  mkdir -p saved_logs/${dt_start}
  mv *.log saved_logs/${dt_start}/.
fi

echo Uploading logs to S3
aws --profile ${AWS_PROFILE:-default} s3 cp ${dt_start}.logs.zip ${S3_PATH}/${dt_start}.logs.zip --quiet
s3_exit_code=$?

if [ $exit_code -ne 0 ]; then
  echo 'Run failed!'

  msg="OCO3 Zarr generation pipeline failed (code ${exit_code}) around ${dt_end}."

  if [ $s3_exit_code -eq 0 ]; then
    msg="${msg} The log files have been uploaded to S3 at ${S3_PATH}/${dt_start}.logs.zip"
  else
    msg="${msg} The log files were unable to be uploaded to S3 and are still on the instance's FS for manual viewing."
  fi

  aws --profile ${AWS_PROFILE:-default} sns publish --topic-arn=${SNS_ARN} --message "${msg}" || echo 'Failed to publish failure notification'; true
else
  echo 'Run succeeded'

  if [ $s3_exit_code -ne 0 ]; then
    msg="The log files were unable to be uploaded to S3 and are still on the instance's FS for manual viewing."
    echo aws --profile ${AWS_PROFILE:-default} sns publish --topic-arn=${SNS_ARN} --message "${msg}"
  fi
fi

if [ $s3_exit_code -eq 0 ]; then
  echo Cleaning up
  rm ${dt_start}.logs.zip
fi

exit $exit_code

