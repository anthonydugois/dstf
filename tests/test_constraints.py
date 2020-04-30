from dstf import *


def test_isvalid__no_simultaneous_execution():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 10, {node: 10})))

    ctr = NoSimultaneousExecutionConstraint()

    assert ctr.isvalid(sched, Chunk(task, 20, {node: 10}))
    assert ctr.isvalid(sched, Chunk(task, 0, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 15, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 5, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 10, {node: 10}))


def test_isvalid__no_migration():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(3)]
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 0, {nodes[0]: 10})))

    ctr = NoMigrationConstraint()

    assert ctr.isvalid(sched, Chunk(task, 10, {nodes[0]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 10, {nodes[1]: 10}))


def test_isvalid__processing_times():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    sched.apply(AppendOperator(Chunk(task, 0, {node: 5})))

    ctr = ProcessingTimesConstraint({node: 10})

    assert ctr.isvalid(sched, Chunk(task, 5, {node: 5}))
    assert ctr.isvalid(sched, Chunk(task, 5, {node: 2}))
    assert not ctr.isvalid(sched, Chunk(task, 5, {node: 6}))


def test_isvalid__release_time():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    ctr = ReleaseTimeConstraint(10)

    assert ctr.isvalid(sched, Chunk(task, 10, {node: 10}))
    assert ctr.isvalid(sched, Chunk(task, 11, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 9, {node: 10}))


def test_isvalid__deadline():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    ctr = DeadlineConstraint(10)

    assert ctr.isvalid(sched, Chunk(task, 0, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 1, {node: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {node: 11}))


def test_isvalid__multipurpose_machines():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = MultipurposeMachinesConstraint(nodes[:3])

    assert ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10}))
    assert ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[3]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[3]: 10}))


def test_isvalid__execution_size():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = ExecutionSizeConstraint(2)

    assert ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10}))
    assert ctr.isvalid(sched, Chunk(task, 0, {nodes[3]: 10, nodes[4]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))


def test_isvalid__execution_nodes():
    task = Task("t0")
    nodes = ["n{}".format(i) for i in range(10)]
    sched = Schedule()

    ctr = ExecutionNodesConstraint(nodes[:3])

    assert ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[2]: 10, nodes[3]: 10}))
    assert not ctr.isvalid(sched, Chunk(task, 0, {nodes[0]: 10, nodes[1]: 10, nodes[3]: 10}))
