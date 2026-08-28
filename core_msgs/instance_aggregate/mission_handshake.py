"""
mission_handshake.py — centralised mission handshake + bidding protocol.

REQUEST → BID → ACK (award) → ACK        happy path
REQUEST → NACK                               agent busy / infeasible
BID     → CANCEL → CANCEL_ACK                losing bidder released
ACTIVE  → CANCEL → CANCEL_ACK                mission aborted
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable

from core_msgs.instance_aggregate.handshake_shared import HandshakeStatus
from core_msgs.instance_aggregate.mission import Mission

logger = logging.getLogger(__name__)

DEFAULT_BID_TIMEOUT = 2.0      # seconds to wait for bids

@dataclass
class MissionBidding:
    """Bid returned from the instance twin. Lower is better on both axes."""
    time_bidding: float | None = None      # [s] estimated time to finish
    battery_bidding: float | None = None   # [%] estimated battery spend
    battery_margin: float | None = None    # [%] SoC left afterwards



@dataclass
class MissionPlanHint:
    """What the aggregate planned for this specific agent."""
    distance: float | None = None    # [m] path length
    plan_cost: float | None = None   # A* cost (posture-weighted)
    path: Any | None = None          # (2, N) ndarray, optional



@dataclass
class MissionEnvelope:
    mission_id: str
    handshake_status: HandshakeStatus
    sender : str


    bidding: MissionBidding | None = None
    payload: Mission | None = None
    hint: MissionPlanHint | None = None
    timestamp: float = field(default_factory=time.time)


class MissionAction(Enum):
    DO_NOTHING = auto()
    START_MISSION = auto()
    CLEAR_MISSION = auto()
    BIDDING = auto()
    DROP_BID = auto()

@dataclass
class ResponderResult:
    reply: MissionEnvelope | None
    action: MissionAction = MissionAction.DO_NOTHING
    mission: Mission | None = None


# ------------ Instance -------------

# Becomes stateful cause instance needs to remember the bid

# Actually we could split up the instance handling into bidding and ack, this way it doesn't need to remember the
# Mission or bid i guess, but here we just do request -> bid and then activate -> ack or something
# But the stateful way may be better for closed loop handling

class MissionResponder:
    """Instance side Mission handshake state"""

    def __init__(
        self,
        agent_name: str,
        instance_name : str,
        get_bidding_fn : Callable[[MissionPlanHint], MissionBidding]

    ) -> None:
        self.agent_name = agent_name
        self.instance_name = instance_name
        self.get_bidding_fn = get_bidding_fn
        self.reserved: dict[str, MissionBidding] = {}
        self.reserved_payload: dict[str, Mission] = {}

    def handle(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        status = env.handshake_status

        # request -> get bidding and send it back
        if status == HandshakeStatus.REQUEST:
            return self._on_request(env, active_id)
        # ack -> got awarded the mission
        if status == HandshakeStatus.ACK:
            return self._on_award(env, active_id)
        # cancel -> either drop bid, or cancel acive mission
        if status == HandshakeStatus.CANCEL:
            return self._on_cancel(env, active_id)
        return ResponderResult(reply=None)


    def _on_request(self, env: MissionEnvelope, active_id: str | None):
        # First check for active mission already (should be handled by aggregate)
        if active_id is not None:
            return ResponderResult(reply=self.get_nack(env.mission_id, self.agent_name))

        bid = self.get_bidding_fn(env.hint)


        if bid is None or bid.time_bidding is None:
            return ResponderResult(reply=self.get_nack(env.mission_id, self.agent_name))

        # Store bid and mission for further handling
        self.reserved[env.mission_id] = bid
        if env.payload is not None:
            self.reserved_payload[env.mission_id] = env.payload

        return ResponderResult(
            reply=self._env(env.mission_id, HandshakeStatus.BID, bidding=bid),
            action=MissionAction.BIDDING,
        )

    def _on_award(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        if active_id is not None and active_id != env.mission_id:
            return ResponderResult(reply=self.get_nack(env.mission_id, self.agent_name))

        # Get missions
        payload = env.payload or self.reserved_payload.get(env.mission_id)

        if payload is None and env.mission_id not in self.reserved:
            # Award for something we never bid on.
            return ResponderResult(reply=self.get_nack(env.mission_id, self.agent_name))

        # Winning one mission drops every other outstanding reservation.
        self.reserved.clear()
        self.reserved_payload.clear()

        return ResponderResult(
            reply=self._env(env.mission_id, HandshakeStatus.ACK),
            action=MissionAction.START_MISSION,
            mission=payload,
        )

    def _on_cancel(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        mission_id = env.mission_id
        if mission_id == active_id:
            self.reserved.pop(mission_id, None)
            return ResponderResult(
                reply=self._env(mission_id, HandshakeStatus.CANCEL_ACK),
                action=MissionAction.CLEAR_MISSION,
            )
        if mission_id in self.reserved:
            self.reserved.pop(mission_id, None)
            self.reserved_payload.pop(mission_id, None)
            return ResponderResult(
                reply=self._env(mission_id, HandshakeStatus.CANCEL_ACK),
                action=MissionAction.DROP_BID,
            )
        return ResponderResult(reply=self.get_nack(mission_id, self.agent_name))

    # -- helpers ----------------------------------------------------------
    def _env(self, mission_id: str, status: HandshakeStatus, **kw) -> MissionEnvelope:
        return MissionEnvelope(
            mission_id=mission_id,
            handshake_status=status,
            sender=self.agent_name,
            **kw,
        )

    @staticmethod
    def get_nack(mission_id: str, sender: str | None = None) -> MissionEnvelope:
        return MissionEnvelope(
            mission_id=mission_id,
            handshake_status=HandshakeStatus.NACK,
            sender=sender,
        )


# ----- Aggregate Part -----

class InitiatorState(Enum):
    IDLE = auto()
    REQUESTED = auto()
    BID = auto()
    REJECTED = auto()
    AWARDED = auto()
    CONFIRMED = auto()
    CANCELLED = auto()
    TIMEOUT = auto()


class MissionInitiator:
    """Protocol state for ONE (mission, agent) pair."""

    def __init__(
        self,
        mission_id: str,
        agent_name: str,
        aggregate_name : str,
        timeout: float = DEFAULT_BID_TIMEOUT,

    ) -> None:
        self.mission_id = mission_id
        self.agent_name = agent_name
        self.aggregate_name = aggregate_name

        self.timeout = timeout
        self.state = InitiatorState.IDLE
        self.bid: MissionBidding | None = None
        self.confirmed = False
        self.sent_at: float | None = None

        self._clock = time.time



    @property
    def expired(self) -> bool:
        if self.sent_at is None:
            return False
        if self.state not in (InitiatorState.REQUESTED, InitiatorState.AWARDED):
            return False
        return (self._clock() - self.sent_at) > self.timeout

    def request(
        self,
        payload: Mission | None = None,
        hint: MissionPlanHint | None = None,
    ) -> MissionEnvelope:
        self.state = InitiatorState.REQUESTED
        self.sent_at = self._clock()
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.REQUEST,
            payload=payload,
            hint=hint,
        )

    def award(self, payload: Mission | None = None) -> MissionEnvelope:
        self.state = InitiatorState.AWARDED
        self.sent_at = self._clock()
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.ACK,
            payload=payload,
        )

    def cancel(self) -> MissionEnvelope:
        self.state = InitiatorState.CANCELLED
        self.sent_at = self._clock()
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.CANCEL,
        )

    def handle(self, env: MissionEnvelope) -> InitiatorState | None:
        if env.mission_id != self.mission_id:
            return None

        if env.sender != self.agent_name:
            return None  # a losing bidder's CANCEL_ACK (auction state not kept)


        s = env.handshake_status

        if s == HandshakeStatus.BID:
            self.bid = env.bidding
            self.state = InitiatorState.BID
            return self.state

        if s == HandshakeStatus.ACK:
            if self.state is InitiatorState.AWARDED :
                self.confirmed = True
                self.state = InitiatorState.CONFIRMED
                return self.state
            return None                          # duplicate

        if s == HandshakeStatus.NACK:
            self.bid = env.bidding
            self.state = InitiatorState.REJECTED
            return self.state

        if s == HandshakeStatus.CANCEL_ACK:
            self.confirmed = False
            self.state = InitiatorState.IDLE
            return self.state

        return None



# ══════════════════════════════════════════════════════════════════════════
# Aggregate side — auction over N candidates
# ══════════════════════════════════════════════════════════════════════════


class MissionAuction:
    """Fan out REQUESTs, collect one reply per candidate, report, done."""

    def __init__(
        self,
        aggregate_name: str,
        mission: Mission,
        hints: dict[str, MissionPlanHint],
        on_complete: Callable[[Mission, dict[str, MissionBidding | None],
                               dict[str, MissionPlanHint]], None],
        timeout: float = DEFAULT_BID_TIMEOUT,
    ) -> None:
        self.aggregate_name = aggregate_name
        self.mission = mission
        self.hints = dict(hints)
        self.on_complete = on_complete
        self.timeout = timeout
        self.replies: dict[str, MissionBidding | None] = {}
        self.closed = False
        self.opened_at: float | None = None

    def open(self) -> dict[str, MissionEnvelope]:
        """{agent_name: envelope} for the twin to send."""
        self.opened_at = time.time()
        return {
            name: MissionEnvelope(
                mission_id=self.mission.mission_id,
                handshake_status=HandshakeStatus.REQUEST,
                sender=self.aggregate_name,
                payload=self.mission,
                hint=hint,
            )
            for name, hint in self.hints.items()
        }

    def handle(self, env: MissionEnvelope, agent_name: str) -> None:
        if self.closed or agent_name not in self.hints:
            return
        if env.mission_id != self.mission.mission_id:
            return
        if env.handshake_status is HandshakeStatus.BID:
            self.replies[agent_name] = env.bidding
        elif env.handshake_status is HandshakeStatus.NACK:
            self.replies[agent_name] = None
        else:
            return                      # ACK/CANCEL_ACK belong to the initiator
        self._maybe_close()

    def tick(self) -> None:
        """Call once per loop. Non-repliers count as a NACK."""
        if self.closed or self.opened_at is None:
            return
        if (time.time() - self.opened_at) > self.timeout:
            for name in self.hints:
                self.replies.setdefault(name, None)
            self._maybe_close()

    def _maybe_close(self) -> None:
        if len(self.replies) < len(self.hints):
            return
        self.closed = True
        self.on_complete(self.mission, dict(self.replies), dict(self.hints))