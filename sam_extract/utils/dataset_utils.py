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


from typing import Dict

from xarray import Dataset


def is_nan(dataset_group: Dict[str, Dataset]) -> bool:
    mapped = {k: dataset_group[k].isnull() for k in dataset_group}

    groups_nan = {k: all([all(mapped[k][v].data.flatten()) for v in mapped[k].data_vars]) for k in dataset_group}

    return all(groups_nan.values())
