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

import json
import logging

logger = logging.getLogger('progress')
json_logger = logging.getLogger('progress_json')


class Progress:
    def __init__(self, task_name, **subtasks):
        for i in subtasks:
            if not isinstance(i, str):
                raise TypeError(f'Invalid key type {i}: {type(i)}. Should be str.')

            if not isinstance(subtasks[i], int):
                raise TypeError(f'Invalid value type {i}: {type(i)}. Should be int.')

        self.__task_name = task_name
        self.__subtasks = subtasks
        self.__progress = {i: 0 for i in subtasks}

    def update_progress(self, subtask, increment=1):
        if subtask not in self.__progress:
            logger.warning(f'Progress update called for nonexistent subtask: {subtask}')
            return

        self.__progress[subtask] += increment

        if self.__progress[subtask] > self.__subtasks[subtask]:
            logger.warning(f'Subtask {subtask} incremented above total')
            self.__progress[subtask] = self.__subtasks[subtask]

    def to_string(self) -> str:
        subtask_strings = [f'{t}: {self.__progress[t]:,}/{self.__subtasks[t]:,}' for t in self.__progress]

        total_done = sum(self.__progress.values())
        total_subtasks = sum(self.__subtasks.values())

        percent_total_complete = total_done / total_subtasks if total_subtasks != 0 else float('nan')

        return (f'Progress for task {self.__task_name}: {", ".join(subtask_strings)}. '
                f'Total: {total_done:,}/{total_subtasks:,} [{percent_total_complete*100:7.3f}%]')

    def to_dict(self) -> dict:
        total_done = sum(self.__progress.values())
        total_subtasks = sum(self.__subtasks.values())

        percent_total_complete = total_done / total_subtasks if total_subtasks != 0 else float('nan')

        return {
            'task': self.__task_name,
            'subtask_progress': {t: dict(complete=self.__progress[t], total=self.__subtasks[t])
                                 for t in self.__progress},
            'total_complete': total_done,
            'total_subtasks': total_subtasks,
            'percent_total_complete': percent_total_complete
        }

    def to_json(self, **dumps_kwargs) -> str:
        return json.dumps(self.to_dict(), **dumps_kwargs)

    def log_progress(self):
        logger.info(self.to_string())
        json_logger.info(self.to_json(separators=(',', ':')))

    def __repr__(self):
        return self.to_string()
