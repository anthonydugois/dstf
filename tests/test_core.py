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
    node = Node("n0")
    sched = Schedule()

    task.set(ReleaseTimeConstraint(10))

    assert Chunk(task, 10, {node: 10}).is_valid(sched)
    assert Chunk(task, 11, {node: 10}).is_valid(sched)
    assert not Chunk(task, 9, {node: 10}).is_valid(sched)
