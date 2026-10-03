import asyncio
from types import SimpleNamespace

import raceproof


async def test_atomic_redemption_passes():
    lock = asyncio.Lock()
    redeemed = False

    async def redeem():
        nonlocal redeemed
        await raceproof.checkpoint("before-transaction")
        async with lock:
            if redeemed:
                return SimpleNamespace(status_code=409)
            await asyncio.sleep(0)
            redeemed = True
            return SimpleNamespace(status_code=200)

    result = await raceproof.run(redeem, concurrency=20)
    result.assert_valid()
    result.assert_successes(min=1, max=1)


async def test_reusable_barrier():
    async def operation():
        for _ in range(3):
            await raceproof.checkpoint("phase")

    result = await raceproof.run(operation, concurrency=4)
    result.assert_valid()
    assert sum(event.kind == "checkpoint-release" for event in result.timeline) == 12
