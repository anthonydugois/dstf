from math import inf
from typing import Optional, Dict

from dstf.core import Schedule, Task, Node


def processed_times(schedule: "Schedule", task: "Task") -> Optional[Dict["Node", float]]:
    if task not in schedule:
        return None

    prcs_times = {}

    for chk in schedule[task]:
        for node, ptime in chk.proc_times.items():
            if node in prcs_times:
                prcs_times[node] += ptime
            else:
                prcs_times[node] = ptime

    return prcs_times


def is_started(schedule: "Schedule", task: "Task") -> bool:
    prcs_times = processed_times(schedule, task)

    if prcs_times is None:
        return False

    for ptime in prcs_times.values():
        if ptime > 0:
            return True

    return False


def is_completed(schedule: "Schedule", task: "Task") -> bool:
    prcs_times = processed_times(schedule, task)

    if prcs_times is None:
        return False

    for node, ptime in task.processing_times.items():
        if node in prcs_times:
            rmn_ptime = ptime - prcs_times[node]
        else:
            rmn_ptime = ptime

        if rmn_ptime > 0:
            return False

    return True


def start_time(schedule: "Schedule", task: "Task") -> float:
    if not is_started(schedule, task):
        return inf

    return min(chk.start_time for chk in schedule[task])


def completion_time(schedule: "Schedule", task: "Task") -> float:
    if not is_completed(schedule, task):
        return inf

    return max(chk.start_time + max(ptime for ptime in chk.proc_times.values()) for chk in schedule[task])
