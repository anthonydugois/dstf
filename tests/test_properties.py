from dstf import *


def test_get__processed_times():
    node = "n0"
    task = Task("t0")
    sched = Schedule().apply(AppendOperator(task, 0, {node: 10})).apply(AppendOperator(task, 10, {node: 5}))

    assert sched.get(ProcessedTimesProperty(task)) == {node: 15}


def test_get__started():
    node = "n0"
    tasks = [Task("t{}".format(i)) for i in range(3)]
    sched = Schedule().apply(AppendOperator(tasks[0], 0, {node: 1})).apply(AppendOperator(tasks[1], 1, {node: 0}))

    assert sched.get(StartedProperty(tasks[0]))
    assert not sched.get(StartedProperty(tasks[1]))
    assert not sched.get(StartedProperty(tasks[2]))


def test_get__start_time():
    node = "n0"
    tasks = [Task("t{}".format(i)) for i in range(3)]
    sched = Schedule().apply(AppendOperator(tasks[0], 0, {node: 1})).apply(AppendOperator(tasks[1], 1, {node: 0}))

    assert sched.get(StartTimeProperty(tasks[0])) == 0
    assert sched.get(StartTimeProperty(tasks[1])) == inf
    assert sched.get(StartTimeProperty(tasks[2])) == inf
