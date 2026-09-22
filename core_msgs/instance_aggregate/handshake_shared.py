"""
handshake_shared.py

Vocabulary shared by both ends of every handshake (instantiate and mission).
Merge this with your existing file, only add what you are missing.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class HandshakeStatus(str, Enum):
    REQUEST = "request"            # aggregate -> instance : "can you take <subject>?"
    BID = "bid"                    # instance  -> aggregate: "yes, and this is my offer"
    ACK = "ack"                    # instance  -> aggregate: "yes" (no bidding) / "confirmed"

    NACK = "nack"                  # "no": busy, infeasible, or a claim we do not recognise
    CANCEL = "cancel"              # aggregate -> instance : drop it (reservation or active)
    CANCEL_ACK = "cancel_ack"

    # Mission extension
    COMPLETE = "complete"          # instance  -> aggregate: subject finished (missions)
    COMPLETE_ACK = "complete_ack"


@dataclass
class HandshakeEnvelope:
    """One envelope for every subject.

    id       subject id: agent name (instantiate) or mission id
    sender   who wrote it (aggregate name or instance name)
    target   who it is for. None = everybody on the topic (pooled REQUEST)
    epoch    attempt counter for this subject, replies to older attempts are dropped
    payload  what is being assigned: DiscoveryMessage / Mission
    hint     what the aggregate planned for this instance: MissionPlanHint.
             The presence of a hint is what turns a request into a bidding request.
    bid      what the instance offers back: MissionBidding
    """
    id: str
    handshake_status: HandshakeStatus
    sender: str
    target: str | None = None
    epoch: int = 0
    payload: Any = None
    hint: Any = None
    bid: Any = None
    timestamp: float = field(default_factory=time.time)


class InitiatorState(str, Enum):
    IDLE = "idle"
    REQUESTED = "requested"
    BID = "bid"                    # instance offered, we have not decided yet
    AWARDED = "awarded"            # we said ACK, waiting for the instance's confirm
    CONFIRMED = "confirmed"        # the instance holds the subject
    REJECTED = "rejected"          # the instance said no, or gave it back
    CANCELLED = "cancelled"        # we told it to let go, waiting for CANCEL_ACK
    RELEASED = "released"
    DONE = "done"
    TIMEOUT = "timeout"


#: A conversation in one of these needs nothing further from its instance.
TERMINAL_STATES = frozenset({
    InitiatorState.REJECTED, InitiatorState.RELEASED,
    InitiatorState.DONE, InitiatorState.TIMEOUT,
})

#: States where we are waiting on a reply and a timeout is meaningful.
WAITING_STATES = frozenset({
    InitiatorState.REQUESTED, InitiatorState.AWARDED, InitiatorState.CANCELLED,
})


class EpochGuard:
    """Epoch bookkeeping for anything that can re-run an attempt on a subject.

    The epoch is bumped on every new request, so replies to the attempt before it
    are recognised as stale and dropped instead of confusing the state machine.
    """

    epoch: int = 0

    def _new_epoch(self) -> int:
        self.epoch += 1
        return self.epoch

    def is_stale(self, env: HandshakeEnvelope) -> bool:
        return env.epoch != self.epoch