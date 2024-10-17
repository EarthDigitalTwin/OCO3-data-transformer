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
terraform workspace select global

terraform destroy

destroy_ec=$?

if [ $destroy_ec -ne 0 ]; then
  echo "Destroy failed"
else
  echo "Destroy succeeded"
fi

terraform workspace select $current_ws
exit $destroy_ec
