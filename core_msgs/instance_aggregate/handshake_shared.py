from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger(__name__)


class HandshakeStatus(str, Enum):
    REQUEST = "request"
    ACK = "ack"
    NACK = "nack"
    CANCEL = "cancel"
    CANCEL_ACK = "cancel_ack"
    BID = "bid"
    COMPLETE = "complete"
    COMPLETE_ACK = "complete_ack"


class EpochGuard:
    """Mixin for initiators. The initiator owns the counter; responders only echo."""

    def _new_epoch(self) -> int:
        self.epoch = getattr(self, "epoch", 0) + 1
        return self.epoch

    def _is_current(self, env) -> bool:
        if getattr(env, "epoch", 0) == getattr(self, "epoch", 0):
            return True
        logger.debug("dropping stale reply: env.epoch=%s, current=%s",
                     getattr(env, "epoch", 0), getattr(self, "epoch", 0))
        return False

