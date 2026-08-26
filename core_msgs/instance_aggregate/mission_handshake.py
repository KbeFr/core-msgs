"""
mission_handshake.py
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from core_msgs.instance_aggregate.handshake_shared import HandshakeStatus
from core_msgs.instance_aggregate.mission import Mission


@dataclass
class MissionEnvelope:
    mission_id: str
    handshake_status: HandshakeStatus
    payload: Mission | None = None  # Full mission parameters live here for Requests (mission.tojson())


class MissionAction(Enum):
    DO_NOTHING = auto()
    START_MISSION = auto()
    CLEAR_MISSION = auto()

@dataclass
class ResponderResult:
    reply: MissionEnvelope | None
    action: MissionAction = MissionAction.DO_NOTHING

class MissionResponder:
    @staticmethod
    def handle(envelope: MissionEnvelope, active_id: str | None) -> ResponderResult:

        if envelope.handshake_status == HandshakeStatus.REQUEST:
            if active_id is None:
                # 2-Way Protocol: Send ACK, and immediately tell the Instance to start.
                reply = MissionEnvelope(mission_id=envelope.mission_id, handshake_status=HandshakeStatus.ACK)
                return ResponderResult(reply=reply, action=MissionAction.START_MISSION)

            reply = MissionEnvelope(mission_id=envelope.mission_id, handshake_status=HandshakeStatus.NACK)
            return ResponderResult(reply=reply, action=MissionAction.DO_NOTHING)

        if envelope.handshake_status == HandshakeStatus.CANCEL:
            if envelope.mission_id == active_id:
                reply = MissionEnvelope(mission_id=envelope.mission_id, handshake_status=HandshakeStatus.CANCEL_ACK)
                return ResponderResult(reply=reply, action=MissionAction.CLEAR_MISSION)

            reply = MissionEnvelope(mission_id=envelope.mission_id, handshake_status=HandshakeStatus.NACK)
            return ResponderResult(reply=reply, action=MissionAction.DO_NOTHING)

        return ResponderResult(reply=None)



# ----- Aggregate Part -----

class MissionInitiator:
    """Tracks protocol state for dispatching or clearing a Mission on an Agent."""

    def __init__(self, mission_id: str, agent_name: str) -> None:
        self.mission_id = mission_id
        self.agent_name = agent_name
        self.confirmed = False

    def request(self, payload: Mission | None = None) -> MissionEnvelope:
        return MissionEnvelope(
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.REQUEST,
            payload=payload
        )

    def cancel(self) -> MissionEnvelope:
        return MissionEnvelope(
            mission_id=self.mission_id,
            handshake_status=HandshakeStatus.CANCEL
        )

    def handle(self, env: MissionEnvelope) -> str | None:
        """Processes the inbound envelope and returns a domain action string."""
        if env.mission_id != self.mission_id:
            return None

        if env.handshake_status == HandshakeStatus.ACK:
            if self.confirmed:
                return None  # Ignore duplicate ACKs
            self.confirmed = True
            return "confirmed"

        if env.handshake_status == HandshakeStatus.NACK:
            return "rejected"

        if env.handshake_status == HandshakeStatus.CANCEL_ACK:
            self.confirmed = False
            return "released"

        return None