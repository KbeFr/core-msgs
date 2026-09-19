"""
handshake.py

One handshake for every "aggregate assigns <subject> to an instance" conversation:
agent <-> instance pairing (instantiate) and mission assignment.

The subject is always the thing being assigned: an agent name or a mission id.
An instance is never a subject, it is a receiver. A discovered instance does not
start a handshake, it only joins the pool of candidates.

Three flows. A hint in the REQUEST is what asks for a bid, nothing else changes.

  POOLED - global topic, first reply wins. One initiator, many instances.
      REQUEST (no target, no hint)  -->
                                    <--  ACK          first ACK wins
      ACK   (to the winner)         -->               link
      CANCEL (to every later ACK)   -->  <-- CANCEL_ACK

  DIRECTED - the aggregate already knows who. It wins automatically.
      REQUEST (target=instance)     -->  <-- ACK
      ACK                           -->               link

  AUCTION - hint per instance, a HandshakeAuction fans out one initiator each.
      REQUEST (target, hint)        -->  <-- BID
      ACK    (to the winner)        -->  <-- ACK      link
      CANCEL (to the losers)        -->  <-- CANCEL_ACK

  After linking all three are identical:
      CANCEL   -->  <-- CANCEL_ACK          release
               <--  COMPLETE  -->  COMPLETE_ACK       mission finished

Aggregate side there is ONE object per subject, and HandshakeInitiator and
HandshakeAuction expose the same four things, so they live in the same dict and
the aggregate never asks which kind it is:

    subject             what is being assigned
    handle(env)         -> HandshakeResult
    tick()              -> HandshakeResult
    cancel(reason)      -> HandshakeResult
    done                safe to drop from the dict
    receiver            the instance holding it, or None

Sans-I/O: everything here RETURNS envelopes, nothing touches a transport.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable

from core_msgs.instance_aggregate.handshake_shared import (
    EpochGuard, HandshakeEnvelope, HandshakeStatus, InitiatorState,
    TERMINAL_STATES, WAITING_STATES,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5.0          # aggregate side: waiting for a reply
DEFAULT_RESERVE_TTL = 10.0     # instance side: how long an unanswered reservation lives
MAX_CANCEL_ATTEMPTS = 3        # give up releasing an instance that never answers

#: We are no longer interested in this conversation. An offer that lands here is not
#: illegal, it is just late, and the instance has to be released rather than ignored.
MOVED_ON = frozenset({InitiatorState.TIMEOUT, InitiatorState.REJECTED,
                      InitiatorState.RELEASED, InitiatorState.DONE})


class HandshakeAction(str, Enum):
    DO_NOTHING = "do_nothing"
    LINK_SUBJECT = "link_subject"          # agent: bind it     | mission: start it
    RELEASE_SUBJECT = "release_subject"    # agent: unbind it   | mission: back to pending
    COMPLETE_SUBJECT = "complete_subject"  # mission finished (aggregate side only)


# ══════════════════════════════════════════════════════════════════════════
# Instance side
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class ResponderResult:
    reply: HandshakeEnvelope | None = None
    action: HandshakeAction = HandshakeAction.DO_NOTHING
    subject: str | None = None      # set together with LINK_/RELEASE_SUBJECT
    payload: Any = None             # set with LINK_SUBJECT: the Mission / AgentDiscoveryMessage
    hint: Any = None                # set with LINK_SUBJECT: the winning path


@dataclass
class Reservation:
    """A promise we made and have not been answered on yet."""
    env: HandshakeEnvelope          # the REQUEST that created it: payload, hint, epoch
    bid: Any = None
    at: float = 0.0


class HandshakeResponder:
    """Instance side of one slot. Holds at most one active subject.

    Two of these per twin:
        agent slot   -- HandshakeResponder(self.name, max_reserved=1)
                        no bid_fn, and one open offer at a time so an unbound
                        instance cannot promise itself to two agents at once.
        mission slot -- HandshakeResponder(self.name, bid_fn=self._estimate_bid)
                        created in _bind, dropped in _unbind, bids on anything.

    bid_fn      hint -> bid, or None to refuse. Only called when the REQUEST
                carries a hint. Without a hint there is nothing to rank, so the
                answer is a plain ACK.
    timeout     reservation TTL. Must outlast the aggregate's bid window.
    """

    def __init__(self,
                 instance: str,
                 bid_fn: Callable[[Any], Any] | None = None,
                 max_reserved: int | None = None,
                 timeout: float = DEFAULT_RESERVE_TTL,
                 clock: Callable[[], float] = time.time,
                 ) -> None:

        self.instance = instance
        self.bid_fn = bid_fn
        self.max_reserved = max_reserved
        self.timeout = timeout
        self.clock = clock

        # Awaiting ACK confirmation
        self.reserved: dict[str, Reservation] = {}

        self.active: str | None = None
        self.active_env: HandshakeEnvelope | None = None
        self.active_epoch: int = 0

    def handle(self, env: HandshakeEnvelope) -> ResponderResult:
        if env.sender == self.instance:                     # our own echo on a shared topic
            return ResponderResult(reply=None)

        if env.target is not None and env.target != self.instance:
            return ResponderResult(reply=None)              # addressed to another instance

        self.purge() # check timeout

        status, env_id = env.handshake_status, env.id

        if status == HandshakeStatus.REQUEST:
            return self._on_request(env, env_id)

        # REQUEST -> BID/ACK -> ACK
        if status == HandshakeStatus.ACK:
            return self._on_award(env, env_id)

        # REQUEST -> BID/ACK -> NACK
        if status == HandshakeStatus.NACK:
            self._drop(env, env_id)
            return ResponderResult()

        # CANCEL -> CANCEL_ACK, for a reservation as well as for an active subject
        if status == HandshakeStatus.CANCEL:
            return self._on_cancel(env, env_id)

        return self.expand_states(env)

    def expand_states(self, env: HandshakeEnvelope) -> ResponderResult:
        """Hook for statuses this class does not know. Override in a subclass."""
        return ResponderResult()


    # ---- inbound ----

    def _on_request(self, env: HandshakeEnvelope, env_id: str) -> ResponderResult:

        # already active
        if self.active is not None:
            if self.active == env_id:               # re-request of holding -> ACK
                self.active_epoch = env.epoch
                logger.debug("[%s] re-request for the subject we already hold: %s",
                             self.instance, env_id)
                return ResponderResult(reply=self._env(env, HandshakeStatus.ACK))
            return ResponderResult(reply=self._env(env, HandshakeStatus.NACK))

        held = self.reserved.get(env_id)
        if held is not None:
            if env.epoch < held.env.epoch:
                return ResponderResult(reply=None)                    # stale attempt, ignore
            if env.epoch == held.env.epoch:                 # retransmit, same answer
                return self._offer(env, held.bid)
        elif self.max_reserved is not None and len(self.reserved) >= self.max_reserved:
            logger.debug("[%s] NACK %s, %d reservation(s) open",
                         self.instance, env_id, len(self.reserved))
            return ResponderResult(reply=self._env(env, HandshakeStatus.NACK))

        # Hint -> bidding. No bid means we cannot do it.
        bid = None
        if env.hint is not None:
            bid = self.bid_fn(env.hint) if self.bid_fn else None
            if bid is None:
                return ResponderResult(reply=self._env(env, HandshakeStatus.NACK))

        # No hint -> REQUEST -> ACK -> ACK. We accept, the aggregate decides who won.
        self.reserved[env_id] = Reservation(env=env, bid=bid, at=self.clock())
        return self._offer(env, bid)

    def _on_award(self, env: HandshakeEnvelope, env_id: str) -> ResponderResult:
        if self.active == env_id:                           # duplicate award, re-confirm
            if env.epoch == self.active_epoch:
                return ResponderResult(reply=self._env(env, HandshakeStatus.ACK))
            return ResponderResult()                        # stale award, ignore
        if self.active is not None:
            return ResponderResult(reply=self._env(env, HandshakeStatus.NACK))

        # there should be a reserved for this
        held = self.reserved.get(env_id)
        if held is None or held.env.epoch != env.epoch:
            logger.debug("[%s] ACK received for non reserved item %s, NACK back",
                         self.instance, env_id)
            return ResponderResult(reply=self._env(env, HandshakeStatus.NACK))

        return self._claim(env, held)

    def _on_cancel(self, env: HandshakeEnvelope, env_id: str) -> ResponderResult:
        ack = self._env(env, HandshakeStatus.CANCEL_ACK)

        if env_id == self.active:
            if env.epoch < self.active_epoch:               # cancel of an older attempt
                return ResponderResult(reply=ack)
            self._clear()
            return ResponderResult(reply=ack, action=HandshakeAction.RELEASE_SUBJECT,
                                   subject=env_id)

        self._drop(env, env_id)
        return ResponderResult(reply=ack)                   # idempotent


    # ---- outbound ----

    def _claim(self, env: HandshakeEnvelope, held: Reservation) -> ResponderResult:
        """We won -> take the subject. Winning one drops every other open bid."""
        self.active = held.env.id
        self.active_env = held.env
        self.active_epoch = held.env.epoch
        self.reserved.clear()

        logger.debug("[%s] claimed %s", self.instance, self.active)

        return ResponderResult(
            reply=self._env(env, HandshakeStatus.ACK),
            action=HandshakeAction.LINK_SUBJECT,
            subject=self.active,
        )

    def get_revoked(self, subject: str) -> HandshakeEnvelope | None:
        """Binding of subject failed"""
        if subject != self.active:
            return None
        epoch = self.active_epoch
        self._clear()
        return HandshakeEnvelope(id=subject, handshake_status=HandshakeStatus.NACK,
                                 sender=self.instance, epoch=epoch)

    # ---- helpers ----

    def purge(self) -> None:
        now = self.clock()
        for env_id in [k for k, r in self.reserved.items() if now - r.at > self.timeout]:
            logger.debug("[%s] reservation for %s expired", self.instance, env_id)
            self.reserved.pop(env_id)

    def _offer(self, env: HandshakeEnvelope, bid: Any) -> ResponderResult:
        """Bidding request -> BID, plain request -> ACK"""
        if bid is None:
            return ResponderResult(reply=self._env(env, HandshakeStatus.ACK))
        return ResponderResult(reply=self._env(env, HandshakeStatus.BID, bid=bid))

    def _drop(self, env: HandshakeEnvelope, env_id: str) -> None:
        held = self.reserved.get(env_id)
        if held is not None and env.epoch >= held.env.epoch:
            self.reserved.pop(env_id)

    def _clear(self) -> None:
        self.active = None
        self.active_env = None

    def _env(self, env: HandshakeEnvelope, status: HandshakeStatus, **kw) -> HandshakeEnvelope:
        return HandshakeEnvelope(id=env.id, handshake_status=status, sender=self.instance,
                                 target=env.sender, epoch=env.epoch, **kw)


# ══════════════════════════════════════════════════════════════════════════
# Aggregate side
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class HandshakeResult:
    """What the aggregate has to do about one inbound message or one tick.
    Returned by both HandshakeInitiator and HandshakeAuction."""
    out: list[HandshakeEnvelope] = field(default_factory=list)
    action: HandshakeAction = HandshakeAction.DO_NOTHING
    subject: str | None = None
    receiver: str | None = None
    state: InitiatorState | None = None

    def merge(self, other: HandshakeResult) -> HandshakeResult:
        self.out.extend(other.out)
        if other.action is not HandshakeAction.DO_NOTHING:
            self.action, self.subject = other.action, other.subject
            self.receiver = other.receiver
        return self


def subject_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not args:
            raise TypeError("subject_required decorator must be applied to an instance method.")

        _self = args[0]

        if getattr(_self, "subject", None) is None:
            logger.warning("Subject required for initiator")
            return None
        return func(*args, **kwargs)

    return wrapper


def orphan_reply(env: HandshakeEnvelope, aggregate: str) -> HandshakeEnvelope | None:
    """A message about a subject the aggregate holds nothing for. Answering anyway
    keeps an instance from sitting on something we already forgot."""
    status = {HandshakeStatus.BID: HandshakeStatus.CANCEL,      # drop the reservation
              HandshakeStatus.ACK: HandshakeStatus.CANCEL,      # let go of the subject
              HandshakeStatus.COMPLETE: HandshakeStatus.COMPLETE_ACK,
              }.get(env.handshake_status)
    if status is None:
        return None
    logger.debug("orphan %s for %s from %s -> %s",
                 env.handshake_status.value, env.id, env.sender, status.value)
    return HandshakeEnvelope(id=env.id, handshake_status=status, sender=aggregate,
                             target=env.sender, epoch=env.epoch)


class HandshakeInitiator(EpochGuard):
    """Aggregate side of ONE subject, talking to one instance or to the pool.

    Pooled:   request()                      -> broadcast, first ACK becomes the receiver
    Directed: request(target=instance)       -> it wins automatically
    Bidding:  request(target=..., hint=...)  -> answered with a BID, award() decides.
              Only HandshakeAuction does that; on its own an initiator has nothing
              to compare a bid against.
    """

    def __init__(self,
                 subject: str | None,
                 aggregate: str,
                 receiver: str | None = None,
                 timeout: float = DEFAULT_TIMEOUT,
                 clock: Callable[[], float] = time.time,
                 epoch: int = 0,
                 payload: Any = None,
                 ) -> None:

        self.subject = subject      # agent / mission
        self.receiver = receiver    # instance
        self.aggregate = aggregate

        self.clock = clock
        self.timeout = timeout
        self.epoch = epoch

        self.payload = payload      # AgentDiscoveryMessage / Mission
        self.hint = None            # kept for the award: the winner's own path
        self.bid = None
        self.bidding = False        # do we expect a BID back from the request

        self.state = InitiatorState.IDLE
        self.sent_at: float | None = None
        self.claimed = False        # we handed the subject over at least once
        self.cancel_attempts = 0
        self.illegal = 0

    # ---- views ----

    @property
    def confirmed(self) -> bool:
        return self.state is InitiatorState.CONFIRMED

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def done(self) -> bool:
        """Safe for the aggregate to drop. CONFIRMED is not done: the instance is
        holding the subject and we still need this object to release it."""
        if self.state is InitiatorState.TIMEOUT:
            return not self.claimed or self.cancel_attempts >= MAX_CANCEL_ATTEMPTS
        return self.state in (InitiatorState.REJECTED, InitiatorState.RELEASED,
                              InitiatorState.DONE)

    @property
    def holds_subject(self) -> bool:
        """The instance may be holding the subject and has not been told to let go.
        TIMEOUT counts: a winner whose confirm was lost is still holding it."""
        return self.claimed and self.state in (InitiatorState.AWARDED,
                                               InitiatorState.CONFIRMED,
                                               InitiatorState.TIMEOUT)

    @property
    def expired(self) -> bool:
        if self.sent_at is None or self.state not in WAITING_STATES:
            return False
        return self.clock() - self.sent_at > self.timeout

    def time_out(self) -> InitiatorState:
        self.state = InitiatorState.TIMEOUT
        self.sent_at = None
        return self.state

    # ---- outbound ----

    @subject_required
    def request(self, payload: Any = None, hint: Any = None,
                target: str | None = None) -> HandshakeEnvelope | None:
        """
        :param payload: what makes the binding possible (Mission, AgentDiscoveryMessage)
        :param hint: what the instance needs to bid. None -> it accepts with a plain ACK
        :param target: the instance to ask. None -> pooled, whoever is on the topic
        :return: the REQUEST envelope
        """
        self.payload = payload if payload is not None else self.payload
        self.hint = hint
        self.bidding = hint is not None
        if target is not None:
            self.receiver = target

        self._new_epoch()
        self.state = InitiatorState.REQUESTED
        self.sent_at = self.clock()
        self.cancel_attempts = 0

        return HandshakeEnvelope(id=self.subject,
                                 handshake_status=HandshakeStatus.REQUEST,
                                 sender=self.aggregate,
                                 target=self.receiver,
                                 payload=self.payload,
                                 hint=hint,
                                 epoch=self.epoch)

    @subject_required
    def award(self) -> HandshakeEnvelope | None:
        """Called by the auction when this one won."""
        if self.state is not InitiatorState.BID:
            self._illegal(HandshakeStatus.ACK)
            return None
        self.state = InitiatorState.AWARDED
        self.claimed = True
        self.sent_at = self.clock()
        return self._env(HandshakeStatus.ACK, hint=self.hint)   # the winner needs its path

    @subject_required
    def cancel(self, reason: str = "cancelled by aggregate") -> HandshakeResult:
        """Release whatever the instance holds: a reservation (losing bidder) or the
        subject itself (abort)."""
        if self.state in (InitiatorState.IDLE, InitiatorState.REJECTED,
                          InitiatorState.RELEASED, InitiatorState.DONE):
            return HandshakeResult()
        logger.debug("cancelling subject=%s on %s: %s", self.subject, self.receiver, reason)
        self.state = InitiatorState.CANCELLED
        self.sent_at = self.clock()
        self.cancel_attempts += 1
        return HandshakeResult(out=[self._env(HandshakeStatus.CANCEL)], state=self.state)

    @subject_required
    def nack(self, receiver: str | None = None) -> HandshakeEnvelope:
        return self._env(HandshakeStatus.NACK, target=receiver)

    def reset(self) -> None:
        """Back to IDLE for a retry. Subject and receiver are kept, pass a new target
        to request() to move the conversation to another instance."""
        self.state = InitiatorState.IDLE
        self.payload = None
        self.bid = None
        self.hint = None
        self.sent_at = None
        self.bidding = False
        self.claimed = False
        self.cancel_attempts = 0

    def bind_subject(self, subject: str) -> None:
        self.subject = subject

    def bind_receiver(self, receiver: str) -> None:
        self.receiver = receiver

    # ---- inbound ----

    def handle(self, env: HandshakeEnvelope) -> HandshakeResult:
        res = HandshakeResult(subject=self.subject, receiver=self.receiver)

        if env.sender == self.aggregate:                    # our own echo
            return res
        if env.id != self.subject:
            logger.warning("Envelope for %s received not handled by this initiator (subject %s)",
                           env.id, self.subject)
            return res
        if self.is_stale(env):
            logger.warning("stale epoch %s (current %s) subject=%s, instance=%s",
                           env.epoch, self.epoch, self.subject, self.receiver)
            return res

        status, sender = env.handshake_status, env.sender

        # Pooled: the first instance to answer becomes the receiver. Everyone after it
        # is holding a reservation we do not want, so cancel it.
        if self.receiver is None and status in (HandshakeStatus.BID, HandshakeStatus.ACK):
            self.receiver = sender
            res.receiver = sender
            logger.debug("subject=%s adopted instance %s (first reply)", self.subject, sender)
        elif sender != self.receiver:
            if status in (HandshakeStatus.BID, HandshakeStatus.ACK):
                logger.debug("subject=%s: %s answered too late, cancelling its reservation",
                             self.subject, sender)
                res.out.append(self._env(HandshakeStatus.CANCEL, target=sender))
            return res                                      # CANCEL_ACK / NACK from a loser

        # A late offer: we already timed out, refused or finished. Cancel it or the
        # instance sits on a reservation until its TTL runs out.
        if status in (HandshakeStatus.BID, HandshakeStatus.ACK) and self.state in MOVED_ON:
            logger.debug("subject=%s: late %s in %s, releasing %s",
                         self.subject, status.value, self.state.value, sender)
            res.out.append(self._env(HandshakeStatus.CANCEL))
            return res

        # REQUEST -> BID
        if status == HandshakeStatus.BID:
            if self.state is not InitiatorState.REQUESTED:
                self._illegal(status)
                return res
            self.bid = env.bid
            self.state = InitiatorState.BID
            self.sent_at = None
            res.state = self.state
            return res

        if status == HandshakeStatus.ACK:
            return self._on_ack(res)

        # REQUEST -> NACK, or a revoke after we linked
        if status == HandshakeStatus.NACK:
            if self.state in (InitiatorState.REQUESTED, InitiatorState.BID,
                              InitiatorState.AWARDED):
                self.state = InitiatorState.REJECTED
                self.sent_at = None
                res.state = self.state
                if self.claimed:                            # refused the award it was given
                    res.action = HandshakeAction.RELEASE_SUBJECT
                return res
            if self.state in (InitiatorState.CONFIRMED, InitiatorState.DONE):
                self.state = InitiatorState.REJECTED        # it gave the subject back
                self.sent_at = None
                logger.warning("instance %s revoked subject %s", self.receiver, self.subject)
                res.state = self.state
                res.action = HandshakeAction.RELEASE_SUBJECT
                return res
            self._illegal(status)
            return res

        # CANCEL -> CANCEL_ACK
        if status == HandshakeStatus.CANCEL_ACK:
            if self.state is not InitiatorState.CANCELLED:
                logger.debug("subject=%s: late CANCEL_ACK in %s", self.subject, self.state.value)
                return res
            released = self.claimed
            self.state = InitiatorState.RELEASED
            self.sent_at = None
            res.state = self.state
            if released:                                    # it really was holding it
                res.action = HandshakeAction.RELEASE_SUBJECT
            return res

        return self.expand_states(env, res)

    def _on_ack(self, res: HandshakeResult) -> HandshakeResult:
        # No bidding: the instance's ACK is its acceptance, so this is where we pick
        # it. We send the 3rd ACK and link, exactly the old global flow.
        if self.state is InitiatorState.REQUESTED and not self.bidding:
            self.state = InitiatorState.CONFIRMED
            self.claimed = True
            self.sent_at = None
            res.out.append(self._env(HandshakeStatus.ACK))
            res.state = self.state
            res.action = HandshakeAction.LINK_SUBJECT
            return res

        # Bidding: REQUEST -> BID -> ACK (award) -> ACK (confirm)
        if self.state is InitiatorState.AWARDED:
            self.state = InitiatorState.CONFIRMED
            self.sent_at = None
            res.state = self.state
            res.action = HandshakeAction.LINK_SUBJECT
            return res

        # The instance's ack-back on the 3-way flow. It is proof the bind landed,
        # which is what makes the 3rd ACK safe, so it is expected, not an error.
        if self.state is InitiatorState.CONFIRMED:
            logger.debug("subject=%s: bind confirmed by %s", self.subject, self.receiver)
            res.state = self.state
            return res

        self._illegal(HandshakeStatus.ACK)
        return res

    # For mission expansion
    def expand_states(self, env: HandshakeEnvelope, res: HandshakeResult) -> HandshakeResult:
        status = env.handshake_status

        # ACTIVE -> COMPLETE -> COMPLETE_ACK
        if status == HandshakeStatus.COMPLETE:
            if self.state not in (InitiatorState.CONFIRMED, InitiatorState.DONE):
                self._illegal(status)
                return res
            self.state = InitiatorState.DONE        # idempotent: retransmits re-ACK
            self.sent_at = None
            res.out.append(self._env(HandshakeStatus.COMPLETE_ACK))
            res.state = self.state
            res.action = HandshakeAction.COMPLETE_SUBJECT
            return res

        self._illegal(status)
        return res

    # ---- tick ----

    def tick(self) -> HandshakeResult:
        """Once per loop. Nobody answered -> give up. An instance that may be holding
        the subject is cancelled rather than walked away from."""
        if not self.expired:
            return HandshakeResult()

        previous = self.state
        self.time_out()
        logger.warning("timeout subject=%s instance=%s in state=%s",
                       self.subject, self.receiver, previous.value)

        if self.holds_subject and self.cancel_attempts < MAX_CANCEL_ATTEMPTS:
            res = self.cancel("release not acknowledged")
            res.subject, res.receiver = self.subject, self.receiver
            return res

        res = HandshakeResult(subject=self.subject, receiver=self.receiver,
                              state=InitiatorState.TIMEOUT)
        if previous is InitiatorState.CANCELLED and self.claimed:
            # We gave up on the release. Treat it as gone on our side.
            res.action = HandshakeAction.RELEASE_SUBJECT
        return res

    # ---- helpers ----

    def _env(self, status: HandshakeStatus, target: str | None = None,
             **kw) -> HandshakeEnvelope:
        return HandshakeEnvelope(id=self.subject, handshake_status=status,
                                 sender=self.aggregate, target=target or self.receiver,
                                 epoch=self.epoch, **kw)

    def _illegal(self, status: HandshakeStatus) -> None:
        self.illegal += 1
        logger.warning("illegal %s at %s (subject=%s inst=%s)",
                       status.value, self.state.value, self.subject, self.receiver)


# ══════════════════════════════════════════════════════════════════════════
# Aggregate side, auction over several instances
# ══════════════════════════════════════════════════════════════════════════

class SessionState(str, Enum):
    SOLICITING = "soliciting"   # requests out, collecting bids
    AWARDING = "awarding"       # winner picked, waiting for its confirm
    ACTIVE = "active"           # winner confirmed, it holds the subject
    DONE = "done"               # winner reported COMPLETE
    FAILED = "failed"           # no winner, or the winner refused / timed out
    CANCELED = "canceled"       # canceled by the aggregate


class HandshakeAuction:
    """Runs the auction for ONE subject and then stays as its owner, so the aggregate
    keeps one object per subject from request to release.

    Only needed when there is something to rank. A pooled or directed assignment is a
    bare HandshakeInitiator, and both answer the same four calls.

    :param hints: {instance: hint}. The keys are who gets a request.
    :param get_winner_fn: (payload, {instance: bid|None}, {instance: hint}) -> instance
    """

    def __init__(self,
                 subject: str,
                 aggregate: str,
                 hints: dict[str, Any],
                 get_winner_fn: Callable[[Any, dict[str, Any], dict[str, Any]], str | None],
                 payload: Any = None,
                 timeout: float = DEFAULT_TIMEOUT,
                 clock: Callable[[], float] = time.time,
                 epoch: int = 0,
                 ) -> None:

        self.subject = subject
        self.aggregate = aggregate
        self.hints = dict(hints)
        self.get_winner_fn = get_winner_fn
        self.payload = payload

        self.timeout = timeout
        self.clock = clock
        self.epoch = epoch

        self.initiators: dict[str, HandshakeInitiator] = {}
        self.bid_replies: dict[str, Any] = {}       # instance -> bid, None = refused

        self.state = SessionState.SOLICITING
        self.winner: str | None = None
        self.attempt: int = 0
        self.opened_at: float | None = None

        logger.debug("Auction created for subject=%s, instances=%d", subject, len(self.hints))

    # ---- lifecycle ----

    def open(self, hints: dict[str, Any] | None = None,
             payload: Any = None) -> HandshakeResult:
        """Re-openable: calling it again bumps the epoch, so replies to the previous
        attempt are dropped."""
        if hints is not None:
            self.hints = dict(hints)
        if payload is not None:
            self.payload = payload

        self.epoch += 1
        self.attempt += 1
        self.state = SessionState.SOLICITING
        self.winner = None
        self.bid_replies = {}
        self.initiators = {}
        self.opened_at = self.clock()

        res = HandshakeResult(subject=self.subject)
        for instance, hint in self.hints.items():
            initiator = HandshakeInitiator(self.subject, self.aggregate, receiver=instance,
                                           timeout=self.timeout, clock=self.clock,
                                           epoch=self.epoch - 1)   # request() bumps it
            self.initiators[instance] = initiator
            res.out.append(initiator.request(payload=self.payload, hint=hint, target=instance))

        logger.debug("Auction opened for subject=%s attempt=%d epoch=%d",
                     self.subject, self.attempt, self.epoch)
        return res

    def handle(self, env: HandshakeEnvelope) -> HandshakeResult:
        res = HandshakeResult(subject=self.subject, receiver=self.winner)
        if env.id != self.subject or env.sender == self.aggregate:
            return res

        initiator = self.initiators.get(env.sender)
        if initiator is None:
            logger.warning("No initiator for instance %s in auction %s", env.sender, self.subject)
            reply = orphan_reply(env, self.aggregate)       # release whatever it holds
            if reply is not None:
                res.out.append(reply)
            return res

        return res.merge(self._advance(env.sender, initiator, initiator.handle(env)))

    def tick(self) -> HandshakeResult:
        """A silent instance counts as a refusal, a winner that never confirms fails
        the auction."""
        res = HandshakeResult(subject=self.subject, receiver=self.winner)

        for instance, initiator in list(self.initiators.items()):
            if not initiator.expired:
                continue
            previous = initiator.state
            inner = initiator.tick()
            res.out.extend(inner.out)

            if previous is InitiatorState.REQUESTED and not self.closed:
                self.bid_replies.setdefault(instance, None)
            elif previous is InitiatorState.AWARDED and instance == self.winner:
                # It may be holding the subject with only its confirm lost, so _stop
                # cancels it instead of walking away.
                return res.merge(self._stop(SessionState.FAILED, "winner never confirmed"))
            elif inner.action is not HandshakeAction.DO_NOTHING and instance == self.winner:
                res.merge(inner)

        if not self.closed:
            res.merge(self._maybe_close())
        return res

    def cancel(self, reason: str = "cancelled by aggregate") -> HandshakeResult:
        """Abort at any point in the lifecycle."""
        if self.state in (SessionState.FAILED, SessionState.CANCELED):
            return HandshakeResult()
        return self._stop(SessionState.CANCELED, reason)

    # ---- internals ----

    def _advance(self, instance: str, initiator: HandshakeInitiator,
                 inner: HandshakeResult) -> HandshakeResult:
        state = inner.state
        is_winner = instance == self.winner
        res = HandshakeResult(out=list(inner.out), subject=self.subject, receiver=self.winner)

        # --- bidding phase ---
        if state is InitiatorState.BID and not self.closed:
            self.bid_replies[instance] = initiator.bid
            return res.merge(self._maybe_close())

        if state is InitiatorState.REJECTED and not self.closed:
            self.bid_replies[instance] = None           # a refusal is still a reply
            return res.merge(self._maybe_close())

        # A bid that lands after we closed: release it, it holds a reservation.
        if state is InitiatorState.BID and self.closed and not is_winner:
            return res.merge(initiator.cancel("auction already closed"))

        if not is_winner:
            return res

        # --- the winner ---
        if state is InitiatorState.CONFIRMED:
            self.state = SessionState.ACTIVE
            logger.debug("subject=%s ACTIVE on %s", self.subject, instance)
            res.action, res.receiver = HandshakeAction.LINK_SUBJECT, instance
            return res

        if state is InitiatorState.DONE:
            self.state = SessionState.DONE
            res.action, res.receiver = HandshakeAction.COMPLETE_SUBJECT, instance
            return res

        if state is InitiatorState.REJECTED:
            logger.warning("winner %s refused subject=%s", instance, self.subject)
            was_active = self.state is SessionState.ACTIVE
            res.merge(self._stop(SessionState.FAILED, "winner refused"))
            if was_active:
                res.action, res.receiver = HandshakeAction.RELEASE_SUBJECT, instance
            return res

        if state is InitiatorState.RELEASED:
            logger.debug("winner %s released subject=%s", instance, self.subject)
            if inner.action is HandshakeAction.RELEASE_SUBJECT:
                res.action, res.receiver = HandshakeAction.RELEASE_SUBJECT, instance
            return res

        return res

    def _maybe_close(self) -> HandshakeResult:
        if len(self.bid_replies) < len(self.hints):
            return HandshakeResult()
        return self._award()

    def _award(self) -> HandshakeResult:
        """All bids in: pick a winner, award it, cancel the rest."""
        try:
            winner = self.get_winner_fn(self.payload, dict(self.bid_replies), dict(self.hints))
        except Exception:
            logger.exception("subject=%s: get_winner_fn raised", self.subject)
            winner = None

        initiator = self.initiators.get(winner) if winner else None
        if initiator is None or initiator.state is not InitiatorState.BID:
            return self._stop(SessionState.FAILED, "no winner")

        self.winner = winner
        self.state = SessionState.AWARDING

        res = HandshakeResult(subject=self.subject, receiver=winner)
        res.out.append(initiator.award())
        for instance, other in self.initiators.items():
            # Only cancel instances that actually hold a reservation.
            if instance != winner and other.state is InitiatorState.BID:
                res.out.extend(other.cancel("lost the auction").out)

        logger.debug("subject=%s awarded to %s", self.subject, winner)
        return res

    def _stop(self, state: SessionState, reason: str) -> HandshakeResult:
        """Release everyone still holding something and close the auction. The
        aggregate decides whether to re-open."""
        logger.info("subject=%s auction %s: %s (attempt %d)",
                    self.subject, state.value, reason, self.attempt)
        self.state = state
        # self.winner is kept on purpose: its CANCEL_ACK still has to route to it.
        # The `receiver` view already reports None once the auction is not live.

        res = HandshakeResult(subject=self.subject)
        for initiator in self.initiators.values():
            if initiator.state is InitiatorState.BID or initiator.holds_subject:
                res.out.extend(initiator.cancel(reason).out)
        return res

    # ---- views ----

    @property
    def closed(self) -> bool:
        """Bidding is over."""
        return self.state is not SessionState.SOLICITING

    @property
    def receiver(self) -> str | None:
        """The instance holding the subject, or about to. Derived, not stored."""
        if self.state in (SessionState.AWARDING, SessionState.ACTIVE):
            return self.winner
        return None

    @property
    def outstanding(self) -> list[str]:
        """Instances whose conversation has not reached a terminal state."""
        return [i for i, ini in self.initiators.items() if not ini.done]

    @property
    def done(self) -> bool:
        """Safe for the aggregate to drop."""
        return (self.state in (SessionState.DONE, SessionState.FAILED, SessionState.CANCELED)
                and not self.outstanding)