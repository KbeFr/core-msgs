"""
instantiate_handshake.py

Two-way handshake to spin up an instance twin for this newly-discovered
agent
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable

from core_msgs.global_msgs.global_payloads import DiscoveryMessage
from core_msgs.instance_aggregate.handshake_shared import HandshakeStatus


logger = logging.getLogger(__name__)

@dataclass
class InstantiateEnvelope:
    agent_name: str
    handshake_status: HandshakeStatus
    sender : str
    instance : Optional[str] = None
    agent_id: Optional[int] = None
    payload: DiscoveryMessage | None = None  # discovered agent's info lives here for Requests


class InstantiateAction(str, Enum):
    DO_NOTHING = "do_nothing"
    LINK_AGENT = "link_agent"
    RELEASE_AGENT = "release_agent"


@dataclass
class InstantiateResponderResult:
    reply: InstantiateEnvelope | None
    action: InstantiateAction = InstantiateAction.DO_NOTHING


DEFAULT_BID_TIMEOUT = 5

# MessageTypes that are addressed to specific instance (trough instance field in global or specific link)
_ADDRESSED = {HandshakeStatus.ACK, HandshakeStatus.NACK, HandshakeStatus.CANCEL}


class InstantiateResponder:
    """Instance-side. """
    def __init__(self,
                 instance_name: str,
                 timeout: float = DEFAULT_BID_TIMEOUT,
                 clock: Callable[[], float] = time.time,
    ) -> None:

        self.instance_name = instance_name
        self.clock = clock
        self.timeout = timeout

        self.confirmed : bool = False
        self.confirmed_time : float = 0.0

        self.discovery : DiscoveryMessage | None = None


    def handle(self, envelope: InstantiateEnvelope ,active_agent_name: str | None ) -> InstantiateResponderResult:

        logger.debug(f"[InstantiateResponder] handle called, instance : {self.instance_name} ")


        if envelope.handshake_status in _ADDRESSED and envelope.instance != self.instance_name:
            return InstantiateResponderResult(reply=None)


        if envelope.handshake_status == HandshakeStatus.REQUEST:
            if self.confirmed and self.clock() - self.confirmed_time > self.timeout:
                self._clear()

            if active_agent_name is None and not self.confirmed:
                self.confirmed = True
                self.confirmed_time = self.clock()
                self.discovery = envelope.payload
                reply = InstantiateEnvelope(agent_name=envelope.agent_name,
                                            sender=self.instance_name,
                                            handshake_status=HandshakeStatus.ACK)
                return InstantiateResponderResult(reply=reply)

            # busy or already reserved -> NACK
            reply = InstantiateEnvelope(
                sender=self.instance_name,
                agent_name=envelope.agent_name,
                handshake_status=HandshakeStatus.NACK,
            )
            logger.debug("[InstantiateResponder] NACK agent=%s (already active)", envelope.agent_name)
            return InstantiateResponderResult(reply=reply, action=InstantiateAction.DO_NOTHING)

        # request -> ack -> ack
        if envelope.handshake_status == HandshakeStatus.ACK:
            if self.confirmed:
                self.confirmed = False
                return InstantiateResponderResult(reply=None, action=InstantiateAction.LINK_AGENT)
            return InstantiateResponderResult(reply=None)

        # we lost
        if envelope.handshake_status == HandshakeStatus.NACK:
            if self.confirmed:
                self._clear()
            return InstantiateResponderResult(reply=None)

        if envelope.handshake_status == HandshakeStatus.CANCEL:
            if envelope.agent_name == active_agent_name:
                self._clear()
                reply = InstantiateEnvelope(
                    sender=self.instance_name,
                    agent_name=envelope.agent_name,
                    handshake_status=HandshakeStatus.CANCEL_ACK,
                )
                logger.debug("[InstantiateResponder] CANCEL_ACK release agent=%s", envelope.agent_name)
                return InstantiateResponderResult(reply=reply, action=InstantiateAction.RELEASE_AGENT)

            reply = InstantiateEnvelope(
                sender=self.instance_name,
                agent_name=envelope.agent_name,
                handshake_status=HandshakeStatus.NACK,
            )
            logger.debug("[InstantiateResponder] NACK cancel agent=%s (not active)", envelope.agent_name)
            return InstantiateResponderResult(reply=reply, action=InstantiateAction.DO_NOTHING)

        return InstantiateResponderResult(reply=None)


    def _clear(self) -> None:
        self.confirmed = False
        self.confirmed_time = 0.0
        self.discovery = None

# ----- Aggregate part -----

class InstantiateInitiator:
    """Tracks protocol state for linking/releasing an Agent to an Instance."""

    def __init__(self, agent_name: str, aggregate_name : str) -> None:
        self.agent_name = agent_name
        self.aggregate_name = aggregate_name
        self.confirmed = False
        self.instance: str | None = None     # winning instance

    def request(self, payload: DiscoveryMessage | None = None) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            sender=self.aggregate_name,
            agent_name=self.agent_name,
            handshake_status=HandshakeStatus.REQUEST,
            payload= payload
        )

    def cancel(self, instance: str | None = None) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            sender=self.aggregate_name,
            instance=instance or self.instance,
            agent_name=self.agent_name,
            handshake_status=HandshakeStatus.CANCEL,
        )

    def ack(self, instance: str) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            sender=self.aggregate_name,
            agent_name=self.agent_name,
            instance=instance,
            handshake_status=HandshakeStatus.ACK
        )

    def nack(self, instance: str) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            sender=self.aggregate_name,
            agent_name=self.agent_name,
            instance=instance,
            handshake_status=HandshakeStatus.NACK,
        )

    def handle(self, env: InstantiateEnvelope) -> InstantiateResponderResult:
        """Processes the inbound envelope and returns a domain action string."""

        # should not be possible -> aggregate routing
        if env.agent_name != self.agent_name:
            return InstantiateResponderResult(reply=None)

        instance_responder = env.sender

        # request -> ack
        if env.handshake_status == HandshakeStatus.ACK:
            if self.confirmed:
                if instance_responder == self.instance:
                    return InstantiateResponderResult(reply=None)   # winner retransmit
                return InstantiateResponderResult(reply=self.nack(instance_responder))

            self.confirmed = True
            self.instance = instance_responder
            return InstantiateResponderResult(reply=self.ack(instance_responder),
                                              action=InstantiateAction.LINK_AGENT)


        # Just if instance is busy or faulty agent -> log only
        if env.handshake_status == HandshakeStatus.NACK:
            logger.warning("Got NACK from instance %s for agent %s", instance_responder, self.agent_name )
            return InstantiateResponderResult(reply=None)

        # cancel -> cancel_ack (not global)
        if env.handshake_status == HandshakeStatus.CANCEL_ACK:
            self.confirmed = False
            self.instance = None
            return InstantiateResponderResult(reply=None, action=InstantiateAction.RELEASE_AGENT)

        return InstantiateResponderResult(reply=None)