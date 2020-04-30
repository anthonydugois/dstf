from dstf import *


def test_apply__append():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 0, {node: 10})))

    assert sched.hastask(task)
    assert sched.task(task)[0].start_time == 0
    assert sched.task(task)[0].proctimes == {node: 10}


def test_apply__remove():
    task = Task("t0")
    node = "n0"
    sched = Schedule()
    chunk = Chunk(task, 0, {node: 10})

    sched.apply(AppendOperator(chunk))

    assert len(sched.task(task)) > 0

    sched.apply(RemoveOperator(chunk))

    assert len(sched.task(task)) == 0


def test_apply__preempt():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 0, {node: 10})))
    sched.apply(PreemptOperator(Chunk(task, 5, {node: 10})))

    assert sched.hastask(task)
    assert sched.task(task)[0].start_time == 0
    assert sched.task(task)[0].proctimes == {node: 5}
    assert sched.task(task)[1].start_time == 5
    assert sched.task(task)[1].proctimes == {node: 10}
