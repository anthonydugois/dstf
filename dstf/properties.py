from math import inf
from typing import Optional, Any, Dict

from dstf.core import Property, Schedule, Task


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


class CompletedProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> bool:
        prcs_times = schedule.get(ProcessedTimesProperty(self.task))

        if prcs_times is None:
            return False

        for node, ptime in self.task.processing_times.items():
            if node in prcs_times:
                rmn_ptime = ptime - prcs_times[node]
            else:
                rmn_ptime = ptime

            if rmn_ptime > 0:
                return False

        return True


class StartTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> float:
        if not schedule.get(StartedProperty(self.task)):
            return inf

        return min(chk.start_time for chk in schedule[self.task])


class CompletionTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> float:
        if not schedule.get(CompletedProperty(self.task)):
            return inf

        return max(chk.start_time + max(ptime for ptime in chk.proc_times.values()) for chk in schedule[self.task])


class MaxCompletionTimeProperty(Property):
    def get(self, schedule: "Schedule") -> float:
        ctimes = []

        for task in schedule:
            ctime = schedule.get(CompletionTimeProperty(task))

            if ctime != inf:
                ctimes.append(ctime)

        if len(ctimes) <= 0:
            return inf

        return max(ctimes)


class SumCompletionTimeProperty(Property):
    def get(self, schedule: "Schedule") -> float:
        ctimes = []

        for task in schedule:
            ctime = schedule.get(CompletionTimeProperty(task))

            if ctime != inf:
                ctimes.append(ctime)

        if len(ctimes) <= 0:
            return inf

        return sum(ctimes)
