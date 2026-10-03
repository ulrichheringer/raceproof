import asyncio
from types import SimpleNamespace

import pytest

import raceproof as rp


async def test_seed_and_concurrency():
    active = 0
    peak = 0

    async def operation():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await rp.checkpoint("all-started")
        active -= 1

    first = await rp.run(operation, concurrency=8, seed=123)
    second = await rp.run(operation, concurrency=8, seed=123)
    assert peak == 8
    assert first.timeline[0].detail == second.timeline[0].detail
    first.assert_valid()


async def test_detect_and_reduce_coupon_race():
    redeemed = False

    async def reset():
        nonlocal redeemed
        redeemed = False

    async def redeem():
        nonlocal redeemed
        available = not redeemed
        await rp.checkpoint("after-read")
        if available:
            redeemed = True
            return SimpleNamespace(status_code=200)
        return SimpleNamespace(status_code=409)

    def one_redemption(result):
        result.assert_successes(max=1)

    result = await rp.run(
        redeem, concurrency=20, invariants=[one_redemption], shrink=True, reset=reset, seed=42
    )
    assert not result.passed
    assert result.reduced is not None
    assert len(result.reduced.outcomes) == 2
    assert result.shrink_attempts == 2
    with pytest.raises(rp.RaceFailure, match="Smallest reproduced worker count: 2"):
        result.assert_valid()


async def test_timeout_and_errors():
    async def stuck():
        await asyncio.Event().wait()

    result = await rp.run(stuck, timeout=0.01)
    assert all(isinstance(outcome.error, TimeoutError) for outcome in result.outcomes)
    assert not result.passed


async def test_mixed_operations_and_async_invariant():
    async def success():
        return SimpleNamespace(status_code=201)

    async def conflict():
        return SimpleNamespace(status_code=409)

    async def invariant(result):
        return len(result.successes) == 1

    result = await rp.run([success, conflict], invariants=[invariant])
    result.assert_valid()


async def test_shrink_requires_isolation():
    with pytest.raises(ValueError, match="reset"):
        await rp.run(lambda: asyncio.sleep(0), shrink=True)


async def test_cancellation_cleans_workers():
    exited = []

    async def operation():
        try:
            await asyncio.Event().wait()
        finally:
            exited.append(True)

    task = asyncio.create_task(rp.run(operation, concurrency=3))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(exited) == 3


async def test_plugin_fixture(raceproof, raceproof_seed):
    result = await raceproof(lambda: asyncio.sleep(0))
    assert result.seed == raceproof_seed


async def test_asgi():
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    async with rp.asgi_client(app) as client:
        result = await rp.run(lambda: client.post("/redeem"), concurrency=5)
    result.assert_successes(min=5, max=5)


async def test_checkpoint_mismatch_times_out():
    async def waiting():
        await rp.checkpoint("only-one")

    result = await rp.run([waiting, lambda: asyncio.sleep(0)], timeout=0.02)
    assert isinstance(result.outcomes[0].error, TimeoutError)


async def test_invariant_programming_error_propagates():
    def broken(result):
        raise ValueError("bad invariant")

    with pytest.raises(ValueError, match="bad invariant"):
        await rp.run(lambda: asyncio.sleep(0), invariants=[broken])
