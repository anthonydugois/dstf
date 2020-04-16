from dstf import *


def test_is_valid__idle():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 10, {node: 10}))

    ctr = IdleConstraint()

    assert ctr.is_valid(sched, Chunk(task, 20, {node: 10}))
    assert ctr.is_valid(sched, Chunk(task, 0, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 15, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 5, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 10, {node: 10}))


def test_is_valid__processing_times():
    task = Task("t0")
    node = "n0"
    sched = Schedule().apply(AppendOperator(task, 0, {node: 5}))

    ctr = ProcessingTimesConstraint({node: 10})

    assert ctr.is_valid(sched, Chunk(task, 5, {node: 5}))
    assert ctr.is_valid(sched, Chunk(task, 5, {node: 2}))
    assert not ctr.is_valid(sched, Chunk(task, 5, {node: 6}))


def test_is_valid__release_time():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    ctr = ReleaseTimeConstraint(10)

    assert ctr.is_valid(sched, Chunk(task, 10, {node: 10}))
    assert ctr.is_valid(sched, Chunk(task, 11, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 9, {node: 10}))


def test_is_valid__deadline():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    ctr = DeadlineConstraint(10)

    assert ctr.is_valid(sched, Chunk(task, 0, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 1, {node: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {node: 11}))


def test_is_valid__multipurpose_machines():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = MultipurposeMachinesConstraint(nodes[:3])

    assert ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10}))
    assert ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[3]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[3]: 10}))


def test_is_valid__execution_size():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = ExecutionSizeConstraint(2)

    assert ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10}))
    assert ctr.is_valid(sched, Chunk(task, 0, {nodes[3]: 10, nodes[4]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))


def test_is_valid__execution_nodes():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = ExecutionNodesConstraint(nodes[:3])

    assert ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10, nodes[3]: 10}))
    assert not ctr.is_valid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[3]: 10}))
