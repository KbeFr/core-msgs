import time
from dataclasses import dataclass, field
from typing import Optional

from core_msgs.agents_contract import AgentKind


@dataclass
class DiscoveryMessage:

    namespace: Optional[str] = None

    robot_id: Optional[int] = None
    robot_name: Optional[str] = None

    kind : Optional[AgentKind] = None
    kinematics: Optional[str] = None
    shape: Optional[str] = None

    mass : Optional[float] = None       #kg
    friction : Optional[float] = None
    avg_speed : Optional[float] = None  #m/s
    max_speed : Optional[float] = None  #m/s

    topics: Optional[dict] = None

    timestamp: Optional[float] = field(default_factory=time.time)


