from dstf import *


def test_get__processed_times():
    node = "n0"
    task = Task("t0")
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 0, {node: 10})))
    sched.apply(AppendOperator(Chunk(task, 10, {node: 5})))

    assert sched.get(ProcessedTimesProperty(task)) == {node: 15}


def test_get__start_time():
    node = "n0"
    tasks = [Task("t{}".format(i)) for i in range(3)]
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(tasks[0], 0, {node: 1})))
    sched.apply(AppendOperator(Chunk(tasks[1], 1, {node: 0})))

    assert sched.get(StartTimeProperty(tasks[0])) == 0
    assert sched.get(StartTimeProperty(tasks[1])) == 1
    assert sched.get(StartTimeProperty(tasks[2])) is None


def test_get__completion_time():
    node = "n0"
    tasks = [Task("t{}".format(i)) for i in range(3)]
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(tasks[0], 0, {node: 1})))
    sched.apply(AppendOperator(Chunk(tasks[1], 1, {node: 0})))

    assert sched.get(CompletionTimeProperty(tasks[0])) == 1
    assert sched.get(CompletionTimeProperty(tasks[1])) == 1
    assert sched.get(CompletionTimeProperty(tasks[2])) is None
