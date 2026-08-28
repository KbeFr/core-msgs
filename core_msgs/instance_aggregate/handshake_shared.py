from __future__ import annotations

from enum import Enum

class HandshakeStatus(str, Enum):
    REQUEST = "request"
    ACK = "ack"
    NACK = "nack"
    CANCEL = "cancel"
    CANCEL_ACK = "cancel_ack"
    BID = "bid"

