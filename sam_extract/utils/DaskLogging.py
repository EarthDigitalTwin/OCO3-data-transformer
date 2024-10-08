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

import logging
import threading
from datetime import datetime
from threading import Lock
from time import sleep

from dask.callbacks import Callback

module_logger = logging.getLogger(__name__)


class ProgressLogging(Callback):
    def __init__(self, interval=5, logger=None, log_level=logging.DEBUG):
        super().__init__()
        self.interval = max(1, min(100, interval)) / 100
        self.logger = logger if logger is not None else module_logger
        self.log_level = log_level

        self.logger.setLevel(log_level)

        self.next_frac = self.interval

        self._complete = False  # In case of nesting
        self._lock = Lock()

    def _start(self, dsk):
        if self._complete:
            return

        self._state = None
        self._start_time = datetime.now()
        self._is_running = True
        self.__poll_t = threading.Thread(target=self.__poll, name='dask-logger')
        self.__poll_t.daemon = True
        self.__poll_t.start()

        if not self._complete:
            self.logger.log(self.log_level, 'Beginning dask task...')

    def _pretask(self, key, dsk, state):
        with self._lock:
            self._state = state

    def _finish(self, dsk, state, errored):
        if self._complete:
            return

        self._is_running = False
        self._complete = True
        self.__poll_t.join()
        run_time = datetime.now() - self._start_time
        self.last_duration = run_time.total_seconds()

        if not errored:
            self.logger.log(self.log_level, f'Dask task completed in {run_time}')
        else:
            self.logger.error(f'Dask task failed after {run_time}')

    def __poll(self):
        while self._is_running:
            s = self._state

            if s:
                with self._lock:
                    n_done = len(s["finished"])
                    n_tasks = sum(len(s[k]) for k in ["ready", "waiting", "running"]) + n_done

                if n_tasks != 0:
                    percent_done = n_done / n_tasks
                else:
                    percent_done = float('nan')

                if percent_done >= self.next_frac:
                    self.logger.log(self.log_level, f'Dask task is {percent_done * 100:.3f}% complete '
                                                    f'[{n_done:,} of {n_tasks:,} tasks completed]')
                    self.next_frac = min(100, self.next_frac + self.interval)

        sleep(0.1)

