"""
MissionType - enum of supported mission kinds.

Mission - dataclass describing one task (goal, constraints, status).

"""
import math
from dataclasses import dataclass, field
from enum import Enum, auto


# ══════════════════════════════════════════════════════════════════════════════
# Posture weight presets
# ══════════════════════════════════════════════════════════════════════════════

class MissionPosture(Enum):
    COVERAGE = 1
    CONSERVE = 2
    URGENT = 3
    SAFE = 4
    ASTAR = 5

    @classmethod
    def get_names(cls):
        return [member.name for member in cls]


POSTURE_WEIGHTS: dict[MissionPosture, tuple] = {
    #            Wd    We    Wt    Wu    Wr
    MissionPosture.COVERAGE: (1.0, 1.0, 0.5, 8.0, 2.0),
    MissionPosture.CONSERVE: (1.0, 5.0, 0.5, 2.0, 1),
    MissionPosture.URGENT:   (1.0, 0.5, 5.0, 0.2, 1.0),
    MissionPosture.SAFE: (1.0, 1.0, 1.0, 1.0, 8.0),
    MissionPosture.ASTAR: (0, 0, 0, 0, 0),  # Custom identification in mission planner to use defaut a_star plnanner
}

DEFAULT_POSTURE = MissionPosture.COVERAGE

#: distance [m] within which a point on `Mission.path` counts as reached and
PATH_WAYPOINT_TOLERANCE = 1.5


def _as_point_list(path) -> list | None:
    """Normalize a path into a plain list of (x, y) tuples.

    Accepts what's documented (a list of (x, y) pairs) as well as a (2, N)
    array-like -- the shape `PlanResult.path` actually comes back in from
    the A* planner -- so a caller that hands over the raw planner output
    still ends up with something jsonpickle-safe and truthiness-safe,
    instead of a numpy array masquerading as `Mission.path`.
    """
    if path is None:
        return None
    shape = getattr(path, "shape", None)
    if shape is not None and len(shape) == 2 and shape[0] == 2:
        return [(float(x), float(y)) for x, y in zip(path[0], path[1])]
    return [tuple(p) for p in path]


# ══════════════════════════════════════════════════════════════════════════════
# Mission types
# ══════════════════════════════════════════════════════════════════════════════

class MissionType(Enum):
    GOTO_WAYPOINT = auto()  # drive to a fixed (x, y)
    TRACK_TARGET = auto()  # intercept / follow a moving object by ir-sim id
    COVERAGE_PATROL = auto()  # visit an ordered list of (x, y) waypoints
    TIME_GATED_GOTO = auto()  # waypoint becomes available at sim time T

    @classmethod
    def get_names(cls):
        return [member.name for member in cls]


# ══════════════════════════════════════════════════════════════════════════════
# Mission status
# ══════════════════════════════════════════════════════════════════════════════

class MissionStatus(Enum):
    PENDING = auto()
    ACTIVE = auto()
    COMPLETE = auto()
    FAILED = auto()
    CANCELLED = auto()
    BIDDING = auto()
    @classmethod
    def get_names(cls):
        return [member.name for member in cls]



# ══════════════════════════════════════════════════════════════════════════════
# Mission dataclass
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Mission:
    """
    One mission task.

    Fields
    ------
    mission_id      : unique string name (e.g. "patrol_A", "intercept_1").
    mission_type    : MissionType enum value.
    goal_xy         : (x, y) world-metre goal for GOTO / TIME_GATED missions.
    waypoints       : ordered list of (x, y) goals for COVERAGE_PATROL.
    target_id       : ir-sim object id for TRACK_TARGET missions.
    unlock_time     : simulation time [s] at which TIME_GATED becomes active.
    battery_budget  : maximum battery percentage the UGV may spend on this task.
                      None = no budget constraint.
    assigned_ugv    : ugv_id string once assigned, else None.
    status          : "pending" → "active" → "complete" | "failed" // "canceled".
    last_cost       : cost returned by the planner for the current assignment.
    """
    mission_id: str
    mission_type: MissionType

    mission_posture: MissionPosture

    # GOTO / TIME_GATED
    goal_xy: tuple[float, float] | None = None
    unlock_time: float = 0.0

    # TRACK_TARGET
    target_id: int | None = None

    # COVERAGE_PATROL
    waypoints: list[tuple[float, float]] = field(default_factory=list)

    # Constraints
    battery_budget: float | None = None  # % of battery allowed for this task
    battery_threshold: float | None = None

    distance : float | None = None

    path : list | None = None

    # Runtime state
    assigned_ugv: str | None = None
    mission_status : MissionStatus = MissionStatus.PENDING
    last_cost: float | None = None

    _wp_index: int = field(default=0, repr=False, compare=False)      # into `waypoints`
    _path_index: int = field(default=0, repr=False, compare=False)    # into `path`

    def set_path(self, path) -> None:
        """Attach a freshly (re)planned route and reset progress through it."""
        self.path = _as_point_list(path)
        self._path_index = 0

    def next_goal(self, ugv_pos: tuple[float, float] | None = None) -> tuple | None:
        """
        Return the current (x, y) goal for the mission, or None if not yet
        available (TIME_GATED not unlocked, TRACK_TARGET out of UAV coverage).

        ugv_pos drives two different kinds of progression: walking `path`
        (the planner's fine-grained route) for GOTO/TIME_GATED missions, and
        finding the current unvisited entry of `waypoints` for
        COVERAGE_PATROL.
        """
        if self.mission_type in (MissionType.GOTO_WAYPOINT, MissionType.TIME_GATED_GOTO):
            # Caller checks unlock_time before calling for TIME_GATED_GOTO.
            return self._next_path_point(ugv_pos) or self.goal_xy

        if self.mission_type == MissionType.COVERAGE_PATROL:
            if self._wp_index < len(self.waypoints):
                return self.waypoints[self._wp_index]
            return None  # all waypoints visited

        if self.mission_type == MissionType.TRACK_TARGET:
            # Goal resolved dynamically from the UAV world map by the caller.
            return None

        return None

    def _next_path_point(self, ugv_pos: tuple[float, float] | None) -> tuple | None:
        """Walk `path` one point at a time instead of handing back `goal_xy`
        straight away -- that's what let the agent ignore the planned route
        entirely and drive straight through whatever it was routed around.

        Returns None (letting the caller fall back to `goal_xy`) when there
        is no path at all -- e.g. a mission that was never planned through
        the grid, or a path that's already been fully consumed.
        """
        if not self.path:
            return None
        if ugv_pos is not None:
            px, py = ugv_pos
            while self._path_index < len(self.path) - 1:
                wx, wy = self.path[self._path_index]
                if math.hypot(wx - px, wy - py) > PATH_WAYPOINT_TOLERANCE:
                    break
                self._path_index += 1
        return tuple(self.path[self._path_index])

    def advance_patrol(self) -> bool:
        """
        Mark current patrol waypoint as visited and advance to the next.
        Returns True if more waypoints remain, False if patrol is complete.
        """
        self._wp_index += 1
        if self._wp_index >= len(self.waypoints):
            self.mission_status = MissionStatus.COMPLETE
            return False
        return True