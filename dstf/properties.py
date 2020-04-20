from math import inf
from typing import Optional, Any, Dict

from dstf import Property, Schedule, Task


class ProcessedTimesProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[Dict[Any, float]]:
        if self.task not in schedule:
            return None

        prcs_times = {}

        for chk in schedule[self.task]:
            for node, ptime in chk.proc_times.items():
                if node in prcs_times:
                    prcs_times[node] += ptime
                else:
                    prcs_times[node] = ptime

        return prcs_times


class StartedProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> bool:
        prcs_times = schedule.get(ProcessedTimesProperty(self.task))

        if prcs_times is None:
            return False

        for ptime in prcs_times.values():
            if ptime > 0:
                return True

        return False


class StartTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> float:
        if not schedule.get(StartedProperty(self.task)):
            return inf

        return min(chk.start_time for chk in schedule[self.task])
