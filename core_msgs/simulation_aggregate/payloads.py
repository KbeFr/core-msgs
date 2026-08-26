from dataclasses import dataclass, field
from enum import Enum

from typing import Optional
import time

from core_msgs.instance_aggregate.payloads import TwinStatePayload

@dataclass
class RobotSpawnMessage:
    name: str = "RobotSpawn"
    robot_name: Optional[str] = None
    kinematics: str = "diff"                # 'diff', 'omni', or 'acker'
    shape: Optional[dict] = None            # e.g. {"name": "circle", "radius": 0.2}
    state: Optional[list[float]] = None     # initial [x, y, theta, ...]
    goal: Optional[list] = None             # single goal or waypoint list
    behavior: Optional[dict] = None         # e.g. {"name": "dash"}, BEHAVIOR mode only
    color: str = "g"
    mode: str = "teleop"                    # 'behavior', 'teleop', or 'mirror'
    external_id: Optional[str] = None       # real-world agent id this robot mirrors
    wheel_base: float = 0.2

@dataclass
class SimStartupMessage:
    name: str = "SimStartup"
    namespace: Optional[str] = None
    sim_id: Optional[str] = None
    world_file: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None
    step_time: Optional[float] = None
    sample_time: Optional[float] = None
    timestamp: Optional[float] = field(default_factory=time.time)


class GuiCommandKind(str, Enum):
    ADD_MISSION = "add_mission"
    CANCEL_MISSION = "cancel_mission"
    SET_POSTURE = "set_posture"

@dataclass
class GuiCommand:
    """GUI -> AggregateDTTwin. `args` shape depends on `kind`:
    ADD_MISSION: kwargs for Mission(...); CANCEL_MISSION: {"mission_id": ...};
    SET_POSTURE: {"posture": "<MissionPosture name>"}."""
    kind: GuiCommandKind
    args: dict = field(default_factory=dict)
    request_id: str | None = None
    timestamp: float = field(default_factory=time.time)


class SimControlAction(str, Enum):
    START = "start"
    PAUSE = "pause"
    RESUME = "resume"
    RESET = "reset"
    SPAWN_ROBOT = "spawn_robot"

@dataclass
class SimControlCommand:
    """GUI -> AggregateSimTwin. `args` shape depends on `action`:
    SPAWN_ROBOT: {"name", "kinematics", "shape", "state", "goal"?, ...}
    (mirrors irsim-twin's RobotSpawnMessage); others take no args today."""
    action: SimControlAction
    args: dict = field(default_factory=dict)
    request_id: str | None = None
    timestamp: float = field(default_factory=time.time)

@dataclass
class SimStatus:
    """AggregateSimTwin -> GUI, published on every tick it hears from the
    irsim-twin (or on state change - see sim_twin.py)."""
    sim_id: str | None = None
    running: bool = False
    connected: bool = False        # heard from the irsim-twin at all yet?
    robot_count: int = 0
    world_width: float | None = None
    world_height: float | None = None
    timestamp: float = field(default_factory=time.time)

@dataclass
class MissionSummary:
    """Trimmed-down Mission for dashboard display - avoids shipping the
    full Mission model (with its planner-internal fields) to the GUI."""
    mission_id: str
    mission_type: str
    mission_status: str
    assigned_ugv: str | None = None
    goal_xy: tuple[float, float] | None = None

@dataclass
class FleetSnapshotPayload:
    """AggregateDTTwin -> GUI, published periodically (see
    aggregate_twin.py:_publish_fleet_snapshot) for dashboard display."""
    agents: list[TwinStatePayload] = field(default_factory=list)
    missions: list[MissionSummary] = field(default_factory=list)
    sim_step: int = 0
    timestamp: float = field(default_factory=time.time)
