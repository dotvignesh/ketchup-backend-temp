"""Background task to expire stale group invites."""

import asyncio
import logging

from database import db

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 5 * 60
INVITE_TTL_HOURS = 24


async def expire_stale_invites_loop() -> None:
    """Run invite expiry on a fixed interval until cancelled."""
    logger.info(
        "Invite expiry task started (interval=%ds, ttl=%dh)",
        CHECK_INTERVAL_SECONDS,
        INVITE_TTL_HOURS,
    )
    while True:
        try:
            await _expire_batch()
        except asyncio.CancelledError:
            logger.info("Invite expiry task cancelled")
            raise
        except Exception:
            logger.exception("Error in invite expiry task")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


async def _expire_batch() -> None:
    """Expire pending invites older than `INVITE_TTL_HOURS`."""
    result = await db.execute(
        """
        UPDATE group_invites
        SET status = 'expired'
        WHERE status = 'pending'
          AND created_at < NOW() - make_interval(hours => $1)
        """,
        INVITE_TTL_HOURS,
    )

    if result and result != "UPDATE 0":
        count = result.split()[-1]
        logger.info("Expired %s stale invite(s)", count)
