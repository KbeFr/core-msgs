"""
mission_handshake.py — centralized mission handshake + bidding protocol.

REQUEST → BID → ACK (award) → ACK        happy path
REQUEST → NACK                               agent busy / infeasible
BID     → CANCEL → CANCEL_ACK                losing bidder released
ACTIVE  → CANCEL → CANCEL_ACK                mission aborted
ACTIVE  → COMPLETE → COMPLETE_ACK            mission finished
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from core_msgs.instance_aggregate.handshake import (
    HandshakeAction, HandshakeInitiator, HandshakeResponder, HandshakeResult, ResponderResult,
)
from core_msgs.instance_aggregate.handshake_shared import (
    HandshakeEnvelope, HandshakeStatus, InitiatorState,
)

logger = logging.getLogger(__name__)

DEFAULT_BID_TIMEOUT = 2.0      # seconds to wait for bids

@dataclass
class MissionBidding:
    """Bid returned from the instance twin. Lower is better on both axes."""
    time_bidding: float | None = None      # [s] estimated time to finish
    battery_bidding: float | None = None   # [%] estimated battery spend
    battery_margin: float | None = None    # [%] SoC left afterward

@dataclass
class MissionPlanHint:
    """What the aggregate planned for this specific agent."""
    distance: float | None = None    # [m] path length
    plan_cost: float | None = None   # A* cost (posture-weighted)
    path: list | None = None          # list [(x,y),(x,y)] with path, no numpy cause of jsonpickle


# ------------ Instance -------------

class MissionResponder(HandshakeResponder):
    """Instance side Mission handshake state, expands on default handshakeResponder"""

    def expand_states(self, env: HandshakeEnvelope) -> ResponderResult:
        status, env_id = env.handshake_status , env.id

        # COMPLETE -> COMPLETE_ACK, the aggregate heard us, stop retransmitting
        if status == HandshakeStatus.COMPLETE_ACK:
            if env_id == self.active and env.epoch == self.active_epoch:
                self._clear()
                return ResponderResult(action=HandshakeAction.RELEASE_SUBJECT, subject=env_id)
        return ResponderResult(reply=None)

    def get_completed(self, subject: str) -> HandshakeEnvelope | None:
        """Called by the instance when the mission is finished. Retransmit until
        COMPLETE_ACK."""
        if subject != self.active:
            return None
        return HandshakeEnvelope(id=subject,
                                 handshake_status=HandshakeStatus.COMPLETE,
                                 sender=self.instance,
                                 epoch=self.active_epoch)


# ------------ Aggregate -------------

#: A mission can still be finished after we stopped tracking it, so a retransmitted
#: COMPLETE has to be answered or the instance never lets go.
MISSION_ORPHAN_REPLIES = {HandshakeStatus.COMPLETE: HandshakeStatus.COMPLETE_ACK}


class MissionInitiator(HandshakeInitiator):
    """Aggregate side of one mission. The base flow ends at the link; a mission can
    also finish, so this adds the completion leg and nothing else."""

    def expand_states(self, env: HandshakeEnvelope, res: HandshakeResult) -> HandshakeResult:
        # ACTIVE -> COMPLETE -> COMPLETE_ACK
        if env.handshake_status == HandshakeStatus.COMPLETE:
            if self.state not in (InitiatorState.CONFIRMED, InitiatorState.DONE):
                self._illegal(env.handshake_status)
                return res
            self.state = InitiatorState.DONE        # idempotent: retransmits re-ACK
            self.sent_at = None
            res.out.append(self._env(HandshakeStatus.COMPLETE_ACK))
            res.state = self.state
            res.action = HandshakeAction.COMPLETE_SUBJECT
            return res

        return super().expand_states(env, res)