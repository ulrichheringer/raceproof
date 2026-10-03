from __future__ import annotations

from typing import Any

import pytest

from .core import run


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("raceproof")
    group.addoption("--raceproof-seed", type=int, default=0, help="Race scenario seed")
    group.addoption(
        "--raceproof-timeout", type=float, default=10.0, help="Operation timeout in seconds"
    )


@pytest.fixture
def raceproof_seed(request: Any) -> int:
    return int(request.config.getoption("--raceproof-seed"))


@pytest.fixture
def raceproof(request: Any) -> Any:
    """Configured async runner; accepts the same arguments as raceproof.run."""

    async def configured(operation: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("seed", request.config.getoption("--raceproof-seed"))
        kwargs.setdefault("timeout", request.config.getoption("--raceproof-timeout"))
        return await run(operation, **kwargs)

    return configured
