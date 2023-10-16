import logging
import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Tuple, List

logger = logging.getLogger(__name__)


class Metrics(ABC):
    @abstractmethod
    def start_time(self, metric_name: str):
        pass

    @abstractmethod
    def end_time(self, metric_name: str):
        pass

    @abstractmethod
    def done(self):
        pass


INSTANCE: Metrics | None = None


def get_metrics() -> Metrics:
    global INSTANCE

    if INSTANCE is not None:
        return INSTANCE

    if os.getenv('NO_METRICS') is None:
        INSTANCE = BaseMetrics()
    else:
        INSTANCE = NullMetrics()

    return INSTANCE


class NullMetrics(Metrics):
    def __init__(self):
        pass

    def start_time(self, metric_name: str):
        pass

    def end_time(self, metric_name: str):
        pass

    def done(self):
        pass


class BaseMetrics(Metrics):
    def __init__(self):
        self.__lock = threading.Lock()
        self.__m: Dict[str, List[timedelta]] = {}
        self.__active: Dict[Tuple[str, threading.Thread], datetime] = {}

    def start_time(self, metric_name: str):
        with self.__lock:
            if metric_name not in self.__m:
                self.__m[metric_name] = [] # timedelta()

            key = (metric_name, threading.current_thread())

            if key in self.__active:
                logger.warning(f'Metric start time for metric/thread pair {key} already exists; replacing it')

            self.__active[key] = datetime.now()

    def end_time(self, metric_name: str):
        with self.__lock:
            if metric_name not in self.__m:
                self.__m[metric_name] = []

            key = (metric_name, threading.current_thread())

            if key not in self.__active:
                logger.warning(f'Metric start time for metric/thread pair {key} doesn\'t exist; ignoring it')
                return

            self.__m[metric_name].append((datetime.now() - self.__active[key]))
            del self.__active[key]

    def done(self):
        with self.__lock:
            metrics = []
            times = []
            averages = []

            if len(self.__active) > 0:
                logger.warning(f'There are still {len(self.__active)} active time measurements. Clearing them')
                self.__active.clear()

            if len(self.__m) == 0:
                return

            for m, t in self.__m.items():
                metrics.append(m)

                sum_time = sum(t, timedelta())

                times.append(str(sum_time))
                averages.append(str(sum_time / len(t)))

            logger.info('Dumping metrics:')
            logger.info('')

            def c_width(l, h):
                return max(
                    [len(str(o)) for o in l + [h]]
                )

            col_widths = dict(
                metrics=c_width(metrics, 'Metrics'),
                sum_time=c_width(times, 'Total time'),
                avg_time=c_width(averages, 'Average time')
            )

            row_width = sum(col_widths.values()) + (len(col_widths) * 3) - 1

            header_format_s = (f' {{: ^{col_widths["metrics"]}}} | {{: ^{col_widths["sum_time"]}}} | '
                               f'{{: ^{col_widths["avg_time"]}}}')

            row_format_s = (f' {{: <{col_widths["metrics"]}}} | {{: ^{col_widths["sum_time"]}}} | '
                            f'{{: ^{col_widths["avg_time"]}}}')

            logger.info(header_format_s.format('Metrics', 'Total time', 'Average time'))

            logger.info('-' * row_width)

            for m, t, a in zip(metrics, times, averages):
                logger.info(row_format_s.format(m, t, a))

            self.__m.clear()
