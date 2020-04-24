from dstf import *


def test_apply__append():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 0, {node: 10}))

    assert sched.hastask(task)
    assert sched.task(task)[0].start_time == 0
    assert sched.task(task)[0].proctimes == {node: 10}


def test_apply__preempt():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 0, {node: 10})).apply(PreemptOperator(task, 5, {node: 10}))

    assert sched.hastask(task)
    assert sched.task(task)[0].start_time == 0
    assert sched.task(task)[0].proctimes == {node: 5}
    assert sched.task(task)[1].start_time == 5
    assert sched.task(task)[1].proctimes == {node: 10}
