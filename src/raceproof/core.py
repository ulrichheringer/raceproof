from __future__ import annotations

import asyncio
import contextvars
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

Operation = Callable[[], Awaitable[Any]]
Invariant = Callable[["Result"], bool | None | Awaitable[bool | None]]


@dataclass(frozen=True)
class Event:
    sequence: int
    elapsed: float
    worker: int | None
    kind: str
    detail: str = ""


@dataclass(frozen=True)
class Outcome:
    worker: int
    value: Any = None
    error: Exception | None = None

    @property
    def successful(self) -> bool:
        if self.error is not None:
            return False
        status = getattr(self.value, "status_code", None)
        return status is None or 200 <= status < 300


class RaceFailure(AssertionError):
    """An invariant violation, including its execution report."""

    def __init__(self, message: str, result: Result) -> None:
        self.result = result
        super().__init__(f"{message}\n{result.format()}")


@dataclass
class Result:
    seed: int
    outcomes: tuple[Outcome, ...]
    timeline: tuple[Event, ...]
    violations: tuple[str, ...] = ()
    reduced: Result | None = None
    shrink_attempts: int = 0
    reduction_note: str = ""

    @property
    def successes(self) -> tuple[Outcome, ...]:
        return tuple(item for item in self.outcomes if item.successful)

    @property
    def passed(self) -> bool:
        return not self.violations and all(item.error is None for item in self.outcomes)

    def assert_successes(self, *, min: int = 0, max: int | None = None) -> None:
        count = len(self.successes)
        if count < min or (max is not None and count > max):
            raise RaceFailure(f"Expected successes in [{min}, {max}], got {count}", self)

    def assert_valid(self) -> None:
        if not self.passed:
            raise RaceFailure("Scenario failed: " + "; ".join(self.violations), self)

    def format(self) -> str:
        lines = [f"raceproof seed={self.seed} workers={len(self.outcomes)}"]
        for event in self.timeline:
            worker = "runner" if event.worker is None else f"worker-{event.worker}"
            lines.append(
                f"{event.sequence:03d} +{event.elapsed:.6f}s {worker} "
                f"{event.kind} {event.detail}".rstrip()
            )
        conflicts = [item.worker for item in self.successes]
        if self.violations:
            lines.append("Violations: " + "; ".join(self.violations))
        lines.append(f"Successful workers (candidate conflicts): {conflicts}")
        if self.reduced is not None:
            lines.append(f"Smallest reproduced worker count: {len(self.reduced.outcomes)}")
            lines.append(self.reduced.format())
        if self.reduction_note:
            lines.append(self.reduction_note)
        return "\n".join(lines)


@dataclass
class _Execution:
    count: int
    timeout: float
    started: float = field(default_factory=time.monotonic)
    events: list[Event] = field(default_factory=list)
    barriers: dict[str, asyncio.Barrier] = field(default_factory=dict)

    def record(self, worker: int | None, kind: str, detail: str = "") -> None:
        self.events.append(
            Event(len(self.events), time.monotonic() - self.started, worker, kind, detail)
        )


_context: contextvars.ContextVar[tuple[_Execution, int] | None] = contextvars.ContextVar(
    "raceproof_worker", default=None
)


async def checkpoint(name: str, *, parties: int | None = None) -> None:
    """Wait for all workers (or explicit parties) at a named reusable barrier."""
    context = _context.get()
    if context is None:
        raise RuntimeError("checkpoint() must run inside a raceproof operation")
    execution, worker = context
    count = execution.count if parties is None else parties
    if count < 1 or count > execution.count:
        raise ValueError("parties must be between 1 and concurrency")
    barrier = execution.barriers.setdefault(name, asyncio.Barrier(count))
    if barrier.parties != count:
        raise ValueError(f"Checkpoint {name!r} has inconsistent parties")
    execution.record(worker, "checkpoint-arrive", name)
    await asyncio.wait_for(barrier.wait(), execution.timeout)
    execution.record(worker, "checkpoint-release", name)


async def _execute(
    operations: Sequence[Operation],
    seed: int,
    timeout: float,
    invariants: Sequence[Invariant],
) -> Result:
    execution = _Execution(len(operations), timeout)
    order = list(range(len(operations)))
    random.Random(seed).shuffle(order)
    gate = asyncio.Event()

    async def worker(index: int) -> Outcome:
        token = _context.set((execution, index))
        try:
            await gate.wait()
            execution.record(index, "start")
            try:
                value = await asyncio.wait_for(operations[index](), timeout)
                execution.record(index, "finish", f"status={getattr(value, 'status_code', 'ok')}")
                return Outcome(index, value=value)
            except Exception as error:  # noqa: BLE001 -- operation failures are result data
                execution.record(index, "error", f"{type(error).__name__}: {error}")
                return Outcome(index, error=error)
        finally:
            _context.reset(token)

    tasks = [asyncio.create_task(worker(index), name=f"raceproof-{index}") for index in order]
    execution.record(None, "schedule", str(order))
    gate.set()
    try:
        outcomes = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    result = Result(
        seed, tuple(sorted(outcomes, key=lambda item: item.worker)), tuple(execution.events)
    )
    violations = []
    for invariant in invariants:
        name = getattr(invariant, "__name__", type(invariant).__name__)
        try:
            async with asyncio.timeout(timeout):
                check = invariant(result)
                if isinstance(check, Awaitable):
                    check = await check
                if check is False:
                    violations.append(f"{name}: returned False")
        except AssertionError as error:
            violations.append(f"{name}: {error.args[0] if error.args else 'assertion failed'}")
    result.violations = tuple(violations)
    return result


async def run(
    operation: Operation | Sequence[Operation],
    *,
    concurrency: int = 2,
    seed: int = 0,
    timeout: float = 10.0,
    invariants: Sequence[Invariant] = (),
    shrink: bool = False,
    reset: Callable[[], Awaitable[None]] | None = None,
) -> Result:
    """Run operations concurrently; optionally replay smaller failing worker counts.

    A sequence runs once per entry; concurrency applies only to a single callable.
    Reset is required for shrinking and runs before every execution, including the first.
    """
    if concurrency < 1 or timeout <= 0:
        raise ValueError("concurrency and timeout must be positive")
    operations = [operation] * concurrency if callable(operation) else list(operation)
    if not operations:
        raise ValueError("At least one operation is required")
    if shrink and (reset is None or not invariants):
        raise ValueError("Shrinking requires reset and at least one invariant")
    if reset is not None:
        await reset()
    result = await _execute(operations, seed, timeout, invariants)
    if shrink and result.violations:
        signature = tuple(item.split(":", 1)[0] for item in result.violations)
        for count in range(1, len(operations)):
            assert reset is not None
            await reset()
            candidate = await _execute(operations[:count], seed, timeout, invariants)
            result.shrink_attempts += 1
            candidate_signature = tuple(item.split(":", 1)[0] for item in candidate.violations)
            if candidate_signature == signature and all(
                item.error is None for item in candidate.outcomes
            ):
                result.reduced = candidate
                break
        result.reduction_note = (
            "Reduction checks prefixes in ascending size; minimum is for this replay, "
            "not a proof of causality or all possible schedules."
        )
        if result.reduced is None:
            result.reduction_note += (
                f" No smaller replay reproduced the violation; original count: {len(operations)}."
            )
    return result
