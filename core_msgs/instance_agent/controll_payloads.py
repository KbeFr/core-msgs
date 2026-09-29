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

@dataclass
class MotionCommand:
    """The twin's action in every form an agent may need; interfaces pick theirs, it never goes on the wire."""
    kinematics: str
    action: list[float]                                                     # raw kinematics action, e.g. diff [v, w]
    linear: list[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])    # body frame [vx, vy, vz] m/s
    angular: list[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])   # body frame [wx, wy, wz] rad/s
    timestamp: Optional[float] = field(default_factory=time.time)

