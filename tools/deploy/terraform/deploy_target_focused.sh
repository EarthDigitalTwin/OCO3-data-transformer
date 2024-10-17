#!/bin/bash

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

current_ws=$(terraform workspace show)

python compare_tfvars.py global.tfvars target_focused.tfvars

valid_config=$?

if [ $valid_config -ne 0 ]; then
  echo "Not proceeding with deployment"
  exit 1
fi

terraform workspace select target_focused

TMP_PLAN=$(mktemp)
TMP_PLAN_STDOUT=$(mktemp)

echo "Checking Terraform plan"
terraform plan -var-file=common.tfvars -var-file=target_focused.tfvars -input=false -out=$TMP_PLAN -detailed-exitcode > $TMP_PLAN_STDOUT

plan_ec=$?

if [ $plan_ec -eq 0 ]; then
  echo "No changes for target-focused state"
  rm $TMP_PLAN_STDOUT $TMP_PLAN
  terraform workspace select $current_ws
  exit 0
elif [ $plan_ec -eq 1 ]; then
  echo "Terraform plan failed"
  rm $TMP_PLAN_STDOUT
  terraform workspace select $current_ws
  exit 1
elif [ $plan_ec -ne 2 ]; then
  echo "Terraform plan returned unexpected value: ${plan_ec}"
  rm $TMP_PLAN_STDOUT
  terraform workspace select $current_ws
  exit $plan_ec
fi

if grep 'Check block assertion failed' < $TMP_PLAN_STDOUT > /dev/null; then
  echo "Terraform checks failed"
  awk '/Check block assertion failed/,0' < $TMP_PLAN_STDOUT

  rm $TMP_PLAN_STDOUT $TMP_PLAN
  terraform workspace select $current_ws
  exit 1
fi

echo "All checks passed"

terraform apply -var-file=common.tfvars -var-file=target_focused.tfvars

apply_ec=$?

if [ $apply_ec -ne 0 ]; then
  echo "Deploy failed"
else
  echo "Deploy succeeded"
fi

rm $TMP_PLAN_STDOUT $TMP_PLAN
terraform workspace select $current_ws
exit $apply_ec

