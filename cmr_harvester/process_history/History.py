import json
from abc import ABC, abstractmethod
from os.path import exists
from typing import List, Tuple


class History(ABC):
    def __init__(self):
        self._history = self._read_history()

    @abstractmethod
    def _read_history(self):
        pass

    @abstractmethod
    def _write_history(self):
        pass

    def publish(self, to_publish: List[Tuple[str, str]]):
        for path, timestamp in to_publish:
            self._history[path] = timestamp
        self._write_history()

    def check(self, to_check: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        ret = []

        for t in to_check:
            if t[0] not in self._history or self._history[t[0]] < t[1]:
                ret.append(t)

        return ret


class LocalHistory(History):
    def __init__(self, file):
        self.__history_file = file
        History.__init__(self)

    def _read_history(self):
        if exists(self.__history_file):
            with open(self.__history_file) as f:
                history = json.load(f)
        else:
            history = {}

        return history

    def _write_history(self):
        with open(self.__history_file, 'w') as f:
            json.dump(self._history, f, indent=4)
