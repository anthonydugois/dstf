.. figure:: _static/dstf-large-margins.svg
   :alt: DSTF
   :width: 450px
   :align: center

   `PyPI <https://pypi.org/project/dstf/>`__
   | `GitHub <https://github.com/anthonydugois/dstf>`__
   | `Issues <https://github.com/anthonydugois/dstf/issues>`__

Overview
========

DSTF is a `scheduling theory <https://en.wikipedia.org/wiki/Notation_for_theoretic_scheduling_problems>`__ framework
based on Python.
It may be used to implement algorithms solving scheduling problems.
It is based on the classic concepts used in scheduling theory: tasks, also called jobs, have to be scheduled on one or
multiple processors, in a way that optimizes some criteria, while respecting a set of constraints.
For example, the problem :math:`1||C_{\max}` is to schedule a set of tasks on one processor, in a way that
minimizes the makespan.
This problem is trivially solvable by any list scheduling strategy.
DSTF provides various utilities to facilitate the evaluation of scheduling solutions:

.. code-block:: python

   from dstf import *
   from math import randint

   node = Node("node")
   tasks = [Task("task" + str(i)) for i in range(10)]

   for task in tasks:
       task.set(IdleConstraint())
       task.set(ProcessingTimesConstraint({node: randint(1, 10)}))

   schedule = Schedule()

   start_time = 0

   for task in tasks:
       processing_time = task.processing_times[node]
       schedule = schedule.apply(AppendOperator(task, start_time, processing_time))
       start_time += processing_time

.. warning::
   This library is not intended to be used in real systems: its scope is strictly theoretical and its use should remain
   at an abstract level.
