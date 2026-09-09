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
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from core_msgs.instance_aggregate.handshake_shared import HandshakeStatus
from core_msgs.instance_aggregate.mission import Mission, MissionStatus

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



@dataclass
class MissionEnvelope:
    mission_id: str
    handshake_status: HandshakeStatus
    sender : str


    bidding: MissionBidding | None = None
    payload: Mission | None = None
    hint: MissionPlanHint | None = None
    epoch: int = 0 # for reauction when failed
    timestamp: float = field(default_factory=time.time)


class MissionAction(str, Enum):
    DO_NOTHING = "do_nothing"
    START_MISSION = "start_mission"
    CLEAR_MISSION = "clear_mission"
    BIDDING = "bidding"
    DROP_BID = "drop_bid"


@dataclass
class ResponderResult:
    reply: MissionEnvelope | None
    action: MissionAction = MissionAction.DO_NOTHING
    mission: Mission | None = None


# ------------ Instance -------------

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
        self.active_epoch: int = 0

        logger.debug("MissionResponder created for agent : %s and  instance : %s" ,agent_name, instance_name)

    # ---- Handle responses ----

    def handle(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        status = env.handshake_status

        logger.debug("Handle called, mission_id: %s , status : %s ", active_id , status.value)

        # request -> get bidding and send it back
        if status == HandshakeStatus.REQUEST:
            return self._on_request(env, active_id)
        # ack -> got awarded the mission
        if status == HandshakeStatus.ACK:
            return self._on_award(env, active_id)
        # cancel -> either drop bid, or cancel active mission
        if status == HandshakeStatus.CANCEL:
            return self._on_cancel(env, active_id)
        # complete_ack -> aggregate acknowledged our COMPLETE, stop retransmitting
        if status == HandshakeStatus.COMPLETE_ACK:
            return ResponderResult(reply=None, action=MissionAction.CLEAR_MISSION)
        return ResponderResult(reply=None)


    def _on_request(self, env: MissionEnvelope, active_id: str | None):
        # First check for active mission already (should be handled by aggregate)
        if active_id is not None:
            return ResponderResult(reply=self.get_nack(env.mission_id, env.epoch))

        bid = self.get_bidding_fn(env.hint)

        if bid is None or bid.time_bidding is None:
            return ResponderResult(reply=self.get_nack(env.mission_id, env.epoch))

        # Store bid and mission for further handling
        self.reserved[env.mission_id] = bid
        self.active_epoch = env.epoch
        if env.payload is not None:
            self.reserved_payload[env.mission_id] = env.payload

        logger.debug("Bid calculated: duration: %s , drain: %s for mission : %s ",
                     bid.time_bidding , bid.battery_bidding , env.mission_id )

        return ResponderResult(
            reply=self._env(env.mission_id, HandshakeStatus.BID, env.epoch, bidding=bid),
            action=MissionAction.BIDDING,
        )

    def _on_award(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        if active_id == env.mission_id:
            return ResponderResult(reply=None)      # already ours ignore duplicates
        if active_id is not None:
            return ResponderResult(reply=self.get_nack(env.mission_id, env.epoch))

        # Get missions
        payload = env.payload or self.reserved_payload.get(env.mission_id)

        if payload is None and env.mission_id not in self.reserved:
            # Award for something we never bid on.
            return ResponderResult(reply=self.get_nack(env.mission_id, env.epoch))

        # Winning one mission drops every other outstanding reservation.
        self.reserved.clear()
        self.reserved_payload.clear()
        self.active_epoch = env.epoch

        logger.debug("Got awarded the mission %s", env.mission_id)

        return ResponderResult(
            reply=self._env(env.mission_id, HandshakeStatus.ACK, env.epoch),
            action=MissionAction.START_MISSION,
            mission=payload,
        )

    def _on_cancel(self, env: MissionEnvelope, active_id: str | None) -> ResponderResult:
        mission_id = env.mission_id

        logger.debug("Cancel called for mission %s", mission_id)

        if mission_id == active_id:
            self.reserved.pop(mission_id, None)
            return ResponderResult(
                reply=self._env(mission_id, HandshakeStatus.CANCEL_ACK, env.epoch),
                action=MissionAction.CLEAR_MISSION,
            )
        if mission_id in self.reserved:
            self.reserved.pop(mission_id, None)
            self.reserved_payload.pop(mission_id, None)
            return ResponderResult(
                reply=self._env(mission_id, HandshakeStatus.CANCEL_ACK, env.epoch),
                action=MissionAction.DROP_BID,
            )
        return ResponderResult(reply=self._env(mission_id, HandshakeStatus.CANCEL_ACK, env.epoch))

    # -- helpers ----------------------------------------------------------
    def _env(self, mission_id: str, status: HandshakeStatus, epoch: int = 0, **kw) -> MissionEnvelope:
        return MissionEnvelope(
            mission_id=mission_id,
            handshake_status=status,
            sender=self.instance_name,
            epoch=epoch,
            **kw,
        )

    def get_nack(self, mission_id: str, epoch: int = 0) -> MissionEnvelope:
        return self._env(mission_id, HandshakeStatus.NACK, epoch)

    def get_completed(self, mission_id: str) -> MissionEnvelope:
        """Called by the instance when the mission is finished. Retransmit until complete_ack."""
        return self._env(mission_id, HandshakeStatus.COMPLETE, self.active_epoch)




# ----- Aggregate Part -----

class InitiatorState(str, Enum):
    IDLE = "idle"
    REQUESTED = "requested"
    BID = "bid"
    REJECTED = "rejected"
    AWARDED = "awarded"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    RELEASED = "released"
    DONE = "done"
    TIMEOUT = "timeout"


#: A conversation in one of these needs nothing further from its agent.
TERMINAL_STATES = frozenset({
    InitiatorState.REJECTED, InitiatorState.RELEASED,
    InitiatorState.DONE, InitiatorState.TIMEOUT,
})

#: States where we are waiting on a reply and a timeout is meaningful.
WAITING_STATES = frozenset({
    InitiatorState.REQUESTED, InitiatorState.AWARDED, InitiatorState.CANCELLED,
})


class MissionInitiator:
    """Protocol state for ONE (mission, agent) pair."""

    def __init__(
        self,
        mission_id: str,
        agent_name: str,
        aggregate_name : str,
        clock: Callable[[], float] = time.time,
        timeout: float = DEFAULT_BID_TIMEOUT,
        epoch: int = 0,

    ) -> None:
        self.mission_id = mission_id
        self.agent_name = agent_name
        self.aggregate_name = aggregate_name

        self.timeout = timeout
        self.epoch = epoch
        self.state = InitiatorState.IDLE
        self.bid: MissionBidding | None = None
        self.sent_at: float | None = None
        self.illegal = 0

        self._clock = clock

        self.hint : MissionPlanHint | None = None # to store hint (full path for visualization)
        # could be that instance sends back own path with kine, but for later

        logger.debug("Initiator created for mission: %s , agent: %s , aggregate: %s",
                     mission_id, agent_name, aggregate_name)

    @property
    def confirmed(self) -> bool:
        return self.state is InitiatorState.CONFIRMED

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def expired(self) -> bool:
        if self.sent_at is None:
            return False
        if self.state not in WAITING_STATES:
            return False
        return (self._clock() - self.sent_at) > self.timeout

    def time_out(self) -> InitiatorState:
        """Called by the session when `expired` is True."""
        self.state = InitiatorState.TIMEOUT
        self.sent_at = None
        return self.state

    def request(self, mission: Mission, hint: MissionPlanHint | None = None,
    ) -> MissionEnvelope:
        """Get request envelope, mission needs to be attached here """

        self.state = InitiatorState.REQUESTED
        self.sent_at = self._clock()

        self.hint = hint # save hint
        logger.debug("request called for mission %s", self.mission_id)

        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.REQUEST,
            payload=mission,
            hint=hint,
            epoch=self.epoch,
        )

    def award(self) -> MissionEnvelope:
        self.state = InitiatorState.AWARDED
        self.sent_at = self._clock()
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.ACK,
            epoch=self.epoch,
        )

    def cancel(self) -> MissionEnvelope:
        self.state = InitiatorState.CANCELLED
        self.sent_at = self._clock()
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.CANCEL,
            epoch=self.epoch,
        )

    def complete_ack(self) -> MissionEnvelope:
        return MissionEnvelope(
            sender=self.aggregate_name,
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.COMPLETE_ACK,
            epoch=self.epoch,
        )

    def _illegal(self, status: HandshakeStatus) -> None:
        self.illegal += 1
        logger.warning("illegal transition: mission=%s agent=%s state=%s got=%s",
                       self.mission_id, self.agent_name, self.state.value, status.value)

    def handle(self, env: MissionEnvelope) -> InitiatorState | None:
        """Returns the NEW state on a legal transition, else None.

        The bid itself is read from `self.bid`, not returned -- one return type
        keeps the caller from having to type-check what came back.
        """
        if env.mission_id != self.mission_id:
            logger.warning("mission %s, received not handled by this initiator (agent %s)",
                           self.mission_id, self.agent_name)
            return None

        if env.epoch != self.epoch:
            logger.warning("stale epoch %s (current %s) mission=%s agent=%s",
                           env.epoch, self.epoch, self.mission_id, self.agent_name)
            return None

        status = env.handshake_status

        logger.debug("handle called for mission %s, status : %s", self.mission_id, status.value)

        # request -> bid
        if status == HandshakeStatus.BID:
            if self.state is not InitiatorState.REQUESTED:
                self._illegal(status)
                return None
            self.bid = env.bidding
            self.state = InitiatorState.BID
            self.sent_at = None
            return self.state

        # request -> bid -> ack (award) -> ack (confirm)
        if status == HandshakeStatus.ACK:
            if self.state is not InitiatorState.AWARDED:
                self._illegal(status)
                return None
            self.state = InitiatorState.CONFIRMED
            self.sent_at = None
            return self.state

        # refusal, either to the request or to the award
        if status == HandshakeStatus.NACK:
            if self.state not in (InitiatorState.REQUESTED, InitiatorState.AWARDED):
                self._illegal(status)
                return None
            self.state = InitiatorState.REJECTED
            self.sent_at = None
            return self.state

        # cancel -> cancel_ack
        if status == HandshakeStatus.CANCEL_ACK:
            if self.state is not InitiatorState.CANCELLED:
                self._illegal(status)
                return None
            self.state = InitiatorState.RELEASED
            self.sent_at = None
            return self.state

        # request -> bid -> ack -> ack -> complete
        if status == HandshakeStatus.COMPLETE:
            if self.state not in (InitiatorState.CONFIRMED, InitiatorState.DONE):
                self._illegal(status)
                return None
            self.state = InitiatorState.DONE      # idempotent: retransmits re-ACK
            self.sent_at = None
            return self.state

        self._illegal(status)
        return None



# ══════════════════════════════════════════════════════════════════════════
# Aggregate side - Session for multiple MissionInitiators
# ══════════════════════════════════════════════════════════════════════════


class SessionState(str, Enum):
    SOLICITING = "soliciting"   # requests out, collecting bids
    AWARDING = "awarding"       # winner picked, waiting for its confirm
    ACTIVE = "active"           # winner confirmed, mission running
    DONE = "done"               # winner reported COMPLETE
    FAILED = "failed"           # no winner, or winner refused / timed out
    CANCELED = "canceled"       # canceled by aggregate or instance

class MissionSession:
    """
    Owns the full lifecycle of one mission: solicitation, evaluation, award,
    execution and closure, across N per-agent MissionInitiators.

    Sans-I/O: every entry point RETURNS {agent_name: envelope} for the twin to
    send. The session never touches the transport.

    Args:
        :param aggregate_name: the name of aggregate that owns the session
        :param mission: mission of the session
        :param hints: per agent hints for path and cost
        :param get_winner_fn: function to evaluate winner
        :param timeout: timeout for bidding response
    """

    def __init__(
        self,
        aggregate_name: str,
        mission: Mission,
        hints: dict[str, MissionPlanHint],
        get_winner_fn : Callable[[Mission, dict[str, MissionBidding | None],
                               dict[str, MissionPlanHint]] , str | None ],

        timeout: float = DEFAULT_BID_TIMEOUT,
        clock: Callable[[], float] = time.time,

    ) -> None:
        self.aggregate_name = aggregate_name
        self.mission = mission
        self.hints = dict(hints)
        self.get_winner_fn = get_winner_fn

        self.initiators: dict[str, MissionInitiator] = {}

        self.bid_replies: dict[str, MissionBidding | None] = {}

        self.timeout = timeout
        self.state = SessionState.SOLICITING
        self.winner: str | None = None
        self.epoch: int = 0
        self.attempt: int = 0
        self.opened_at: float | None = None

        self.clock = clock

        logger.debug("Session created for mission: %s , aggregate: %s, agents: %d ",
                     mission.mission_id , aggregate_name , len(hints.keys()))


    # -- lifecycle --------------------------------------------------------

    def open(self, hints: dict[str, MissionPlanHint] | None = None) -> dict[str, MissionEnvelope]:
        """{agent_name: envelope} for the twin to send. Re-openable: calling it
        again bumps the epoch, so replies to the previous attempt are dropped."""
        if hints is not None:
            self.hints = dict(hints)

        self.epoch += 1
        self.attempt += 1
        self.state = SessionState.SOLICITING
        self.winner = None
        self.bid_replies = {}
        self.initiators = {}
        self.opened_at = self.clock()

        envs = {}
        for name, hint in self.hints.items():
            self.initiators[name] = MissionInitiator(
                self.mission.mission_id, name, self.aggregate_name,
                clock=self.clock, timeout=self.timeout, epoch=self.epoch,
            )
            envs[name] = self.initiators[name].request(mission=self.mission, hint=hint)

        logger.debug("Session opened for mission=%s attempt=%d epoch=%d",
                     self.mission.mission_id, self.attempt, self.epoch)
        return envs


    def handle(self, env: MissionEnvelope, agent_name: str) -> dict[str, MissionEnvelope]:
        """Route one reply to its initiator. Returns anything to send."""
        logger.debug("handle called, mission : %s , agent: %s", self.mission.mission_id, agent_name)

        initiator = self.initiators.get(agent_name)
        if initiator is None:
            logger.warning("No initiator for agent %s in current session", agent_name)
            return {}

        new_state = initiator.handle(env)
        if new_state is None:
            return {}                                   # illegal or stale, already logged

        # --- bidding phase ------------------------------------------------
        if new_state is InitiatorState.BID and not self.closed:
            self.bid_replies[agent_name] = initiator.bid
            return self._maybe_close()

        if new_state is InitiatorState.REJECTED and not self.closed:
            self.bid_replies[agent_name] = None          # a refusal is still a reply
            return self._maybe_close()

        # --- award phase --------------------------------------------------
        if new_state is InitiatorState.CONFIRMED and agent_name == self.winner:
            self.state = SessionState.ACTIVE
            self.mission.assigned_ugv = agent_name
            self.mission.mission_status = MissionStatus.ACTIVE
            self.mission.path = initiator.hint.path if initiator.hint else None
            logger.debug("mission=%s ACTIVE on %s", self.mission.mission_id, agent_name)
            return {}

        if new_state is InitiatorState.REJECTED and agent_name == self.winner:
            logger.warning("winner %s refused mission=%s", agent_name, self.mission.mission_id)
            return self._stop("winner refused")

        # --- execution ----------------------------------------------------
        if new_state is InitiatorState.DONE and agent_name == self.winner:
            self.state = SessionState.DONE
            self.mission.mission_status = MissionStatus.COMPLETE
            logger.debug("mission=%s COMPLETE on %s", self.mission.mission_id, agent_name)
            return {agent_name: initiator.complete_ack()}   # idempotent, re-ACKs retransmits

        # --- losers closing out -------------------------------------------
        if new_state is InitiatorState.RELEASED:
            logger.debug("agent %s released mission=%s", agent_name, self.mission.mission_id)
            return {}

        return {}


    def tick(self) -> dict[str, MissionEnvelope]:
        """Call once per loop. Non-repliers count as a NACK; a winner that never
        confirms fails the session."""
        out: dict[str, MissionEnvelope] = {}

        for name, ini in self.initiators.items():
            if not ini.expired:
                continue
            prev = ini.state
            ini.time_out()
            logger.warning("timeout mission=%s agent=%s in state=%s",
                           self.mission.mission_id, name, prev.value)
            if prev is InitiatorState.REQUESTED and not self.closed:
                self.bid_replies.setdefault(name, None)
            elif prev is InitiatorState.AWARDED and name == self.winner:
                return self._stop("winner never confirmed")

        if not self.closed:
            out.update(self._maybe_close())
        return out


    def _maybe_close(self) -> dict[str, MissionEnvelope]:
        if len(self.bid_replies) < len(self.hints):
            return {}
        return self._award()


    def _award(self) -> dict[str, MissionEnvelope]:
        """All bids in: pick a winner, award it, cancel the rest."""
        winner = self.get_winner_fn(self.mission, self.bid_replies, self.hints)
        if winner is None or winner not in self.initiators:
            return self._stop("no winner")

        self.winner = winner
        self.state = SessionState.AWARDING

        out = {winner: self.initiators[winner].award()}
        for name, ini in self.initiators.items():
            # Only cancel agents that actually hold a reservation.
            if name != winner and ini.state is InitiatorState.BID:
                out[name] = ini.cancel()

        logger.debug("mission=%s awarded to %s (%d cancels)",
                     self.mission.mission_id, winner, len(out) - 1)
        return out


    def _stop(self, reason: str, state: SessionState ) -> dict[str, MissionEnvelope]:
        """Release everyone still holding a reservation and mark the session
        failed. The twin decides whether to re-open."""
        logger.info("mission=%s session failed: %s (attempt %d)",
                    self.mission.mission_id, reason, self.attempt)
        self.state = state
        self.winner = None

        if state == SessionState.FAILED:
            self.mission.assigned_ugv = None
            self.mission.mission_status = MissionStatus.PENDING # Can retry maybe wait but yea
        elif state == SessionState.CANCELED:
            self.mission.mission_status = MissionStatus.CANCELLED # stop retry

        out = {}
        for name, ini in self.initiators.items():
            if ini.state in (InitiatorState.BID, InitiatorState.CONFIRMED):
                out[name] = ini.cancel()
        return out


    def cancel(self, reason: str = "cancelled by aggregate") -> dict[str, MissionEnvelope]:
        """Abort at any point in the lifecycle. use private _fail."""
        if self.state in (SessionState.DONE, SessionState.FAILED):
            return {}

        return self._stop(reason, SessionState.CANCELED)

    # -- views ------------------------------------------------------------

    @property
    def closed(self) -> bool:
        """Bidding is over."""
        return self.state is not SessionState.SOLICITING

    @property
    def outstanding(self) -> list[str]:
        """Agents whose conversation has not reached a terminal state."""
        return [a for a, i in self.initiators.items() if not i.terminal]

    @property
    def retirable(self) -> bool:
        """Safe for the twin to drop this session."""
        return (self.state in (SessionState.DONE, SessionState.FAILED , SessionState.CANCELED)
                and not self.outstanding)

    @property
    def committed_agent(self) -> str | None:
        """Replaces the twin's _committed dict: derived, not stored."""
        if self.state in (SessionState.AWARDING, SessionState.ACTIVE):
            return self.winner
        return None
