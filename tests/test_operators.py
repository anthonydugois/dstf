from dstf import *


def test_apply__append():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 0, {node: 10}))

    assert task in sched
    assert len(sched[task]) == 1
    assert sched[task][0].start_time == 0
    assert sched[task][0].proc_times == {node: 10}


def test_apply__preempt():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 0, {node: 10})).apply(PreemptOperator(task, 5, {node: 10}))

    assert task in sched
    assert len(sched[task]) == 2
    assert sched[task][0].start_time == 0
    assert sched[task][0].proc_times == {node: 5}
    assert sched[task][1].start_time == 5
    assert sched[task][1].proc_times == {node: 10}
