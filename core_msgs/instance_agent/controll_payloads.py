from dataclasses import dataclass, field
from typing import Optional
import time

@dataclass
class VelocityCommandMessage:
    kinematics: str
    cmd: list[float]
    name: str = "VelocityCommand"
    timestamp: Optional[float] = field(default_factory=time.time)

@dataclass
class InitialCommandMessage:
    """Message to let agent know that an instance has been linked to it"""
    instance_name : str
    agent_name: str
    name: str = "InitialCommand"

