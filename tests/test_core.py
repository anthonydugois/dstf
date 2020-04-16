import pytest

from dstf import *


def test_set__task():
    task = Task("t0")
    ctr = IdleConstraint()

    task.set(ctr)

    assert IdleConstraint in task
    assert task[IdleConstraint] == ctr


def test_getattr__task():
    task = Task("t0")

    task.set(ReleaseTimeConstraint(10))

    assert task.release_time == 10

    with pytest.raises(AttributeError):
        attr = task.not_found_attribute


def test_is_valid__chunk():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    task.set(ReleaseTimeConstraint(10))

    assert Chunk(task, 10, {node: 10}).is_valid(sched)
    assert Chunk(task, 11, {node: 10}).is_valid(sched)
    assert not Chunk(task, 9, {node: 10}).is_valid(sched)


def test_append_to__chunk():
    task = Task("t0")
    node = "n0"
    sched = Schedule()

    task.set(ReleaseTimeConstraint(10))

    chks = [Chunk(task, 10, {node: 10}), Chunk(task, 11, {node: 10}), Chunk(task, 9, {node: 10})]

    chks[0].append_to(sched)
    chks[1].append_to(sched)

    assert task in sched
    assert chks[0] in sched[task]
    assert chks[1] in sched[task]

    with pytest.raises(ConstraintError):
        chks[2].append_to(sched)
