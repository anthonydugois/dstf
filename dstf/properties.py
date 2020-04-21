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


class StartTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[float]:
        if self.task not in schedule:
            return None

        return min(chk.start_time for chk in schedule[self.task])


class CompletionTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[float]:
        if self.task not in schedule:
            return None

        return max(chk.completion_time for chk in schedule[self.task])
