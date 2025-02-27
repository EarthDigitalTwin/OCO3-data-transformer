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

current_ws=$(terraform workspace show)

if terraform workspace list | grep -q "global"; then
  echo "Global workspace already exists"
else
  echo "Creating global workspace"
  terraform workspace new global > /dev/null
fi

if terraform workspace list | grep -q "target_focused"; then
  echo "Target-focused workspace already exists"
else
  echo "Creating target-focused workspace"
  terraform workspace new target_focused > /dev/null
fi

echo "Restoring current workspace: ${current_ws}"
terraform workspace select $current_ws > /dev/null

# TODO: configurable behavior?
if [ -f "common.tfvars" ]; then
  echo "NOTE:: common.tfvars already exists. Moving it to common.tfvars.old and creating a new one. Replace the old one if it is still valid and correct"
  mv common.tfvars common.tfvars.old
else
  echo "Copying common terraform variables template"
fi

cp tfvar_templates/common.template.tfvars common.tfvars

if [ -f "global.tfvars" ]; then
  echo "NOTE:: global.tfvars already exists. Moving it to global.tfvars.old and creating a new one. Replace the old one if it is still valid and correct"
  mv global.tfvars global.tfvars.old
else
  echo "Copying global terraform variables template"
fi

cp tfvar_templates/global.template.tfvars global.tfvars

if [ -f "target_focused.tfvars" ]; then
  echo "NOTE:: target_focused.tfvars already exists. Moving it to target_focused.tfvars.old and creating a new one. Replace the old one if it is still valid and correct"
  mv target_focused.tfvars target_focused.tfvars.old
else
  echo "Copying target_focused terraform variables template"
fi

cp tfvar_templates/target_focused.template.tfvars target_focused.tfvars

echo "Ready"
