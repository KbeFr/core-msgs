"""
instantiate_handshake.py

Two-way handshake to spin up an instance twin for this newly-discovered
agent
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from core_msgs.global_msgs.global_payloads import DiscoveryMessage
from core_msgs.instance_aggregate.handshake_shared import HandshakeStatus


logger = logging.getLogger(__name__)

@dataclass
class InstantiateEnvelope:
    agent_name: str
    handshake_status: HandshakeStatus
    aggregate_name: Optional[str] = None
    instance_name: Optional[str] = None
    agent_id: Optional[int] = None
    payload: DiscoveryMessage | None = None  # discovered agent's info lives here for Requests


class InstantiateAction(Enum):
    DO_NOTHING = auto()
    LINK_AGENT = auto()
    RELEASE_AGENT = auto()


@dataclass
class InstantiateResponderResult:
    reply: InstantiateEnvelope | None
    action: InstantiateAction = InstantiateAction.DO_NOTHING


class InstantiateResponder:
    """Instance-side. Static, same shape as MissionResponder."""

    @staticmethod
    def handle(envelope: InstantiateEnvelope,instance_name: str ,active_agent_name: str | None ) -> InstantiateResponderResult:

        logger.debug(f"[InstantiateResponder] handle called, instance : {instance_name} ")

        if envelope.handshake_status == HandshakeStatus.REQUEST:
            if active_agent_name is None:
                # 2-way protocol: ACK, and tell the instance to link up.
                reply = InstantiateEnvelope(
                    aggregate_name=envelope.aggregate_name,
                    instance_name=instance_name,
                    agent_name=envelope.agent_name,
                    handshake_status=HandshakeStatus.ACK,
                )
                logger.debug("[InstantiateResponder] ACK link agent=%s", envelope.agent_name)
                return InstantiateResponderResult(reply=reply, action=InstantiateAction.LINK_AGENT)

            reply = InstantiateEnvelope(
                aggregate_name=envelope.aggregate_name,
                instance_name=instance_name,
                agent_name=envelope.agent_name,
                handshake_status=HandshakeStatus.NACK,
            )
            logger.debug("[InstantiateResponder] NACK agent=%s (already active)", envelope.agent_name)
            return InstantiateResponderResult(reply=reply, action=InstantiateAction.DO_NOTHING)

        if envelope.handshake_status == HandshakeStatus.CANCEL:
            if envelope.agent_name == active_agent_name:
                reply = InstantiateEnvelope(
                    aggregate_name=envelope.aggregate_name,
                    instance_name=instance_name,
                    agent_name=envelope.agent_name,
                    handshake_status=HandshakeStatus.CANCEL_ACK,
                )
                logger.debug("[InstantiateResponder] CANCEL_ACK release agent=%s", envelope.agent_name)
                return InstantiateResponderResult(reply=reply, action=InstantiateAction.RELEASE_AGENT)

            reply = InstantiateEnvelope(
                aggregate_name=envelope.aggregate_name,
                instance_name=instance_name,
                agent_name=envelope.agent_name,
                handshake_status=HandshakeStatus.NACK,
            )
            logger.debug("[InstantiateResponder] NACK cancel agent=%s (not active)", envelope.agent_name)
            return InstantiateResponderResult(reply=reply, action=InstantiateAction.DO_NOTHING)

        return InstantiateResponderResult(reply=None)


# ----- Aggregate part -----

class InstantiateInitiator:
    """Tracks protocol state for linking/releasing an Agent to an Instance."""

    def __init__(self, agent_name: str, aggregate_name : str) -> None:
        self.agent_name = agent_name
        self.aggregate_name = aggregate_name
        self.confirmed = False

    def request(self, payload: DiscoveryMessage | None = None) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            aggregate_name=self.aggregate_name,
            agent_name=self.agent_name,
            handshake_status=HandshakeStatus.REQUEST,
            payload= payload
        )

    def cancel(self) -> InstantiateEnvelope:
        return InstantiateEnvelope(
            aggregate_name=self.aggregate_name,
            agent_name=self.agent_name,
            handshake_status=HandshakeStatus.CANCEL
        )

    def handle(self, env: InstantiateEnvelope) -> str | None:
        """Processes the inbound envelope and returns a domain action string."""
        if env.agent_name != self.agent_name:
            return None

        if env.handshake_status == HandshakeStatus.ACK:
            if self.confirmed:
                return None  # Ignore duplicate ACKs
            self.confirmed = True
            return "confirmed"

        if env.handshake_status == HandshakeStatus.NACK:
            return "release_rejected" if self.confirmed else "rejected"

        if env.handshake_status == HandshakeStatus.CANCEL_ACK:
            self.confirmed = False
            return "released"

        return None