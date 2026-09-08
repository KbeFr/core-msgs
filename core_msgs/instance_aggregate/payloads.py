from __future__ import annotations

import time

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TwinStatePayload:
    name: str
    state: tuple[float, float, float]  # (x, y, theta)
    velocity:  tuple[float, float]
    battery_pct: float
    arrive_flag: bool
    sim_time: float
    active_mission_id : Optional[str] = None
    mission_time: Optional[float] = None
    timestamp: float = field(default_factory=time.time)



@dataclass
class ObstacleObservation:
    """One obstacle/agent snapshot, independent of where it came from."""

    id: str
    x: float
    y: float
    theta: float = 0.0
    radius: float | None = None                       # circular footprint
    polygon: list[tuple[float, float]] | None = None  # polygon footprint (world frame)
    is_dynamic: bool = False
    vx: float = 0.0
    vy: float = 0.0
    confidence: float = 1.0            # 1.0 for ground truth (sim), <1.0 for vision fixes
    timestamp: float = field(default_factory=time.time)


    # properties below needed for irsim controllers to interface with obstacles (instance twin)

    @property
    def position(self):
        import numpy as np
        return np.array([[self.x], [self.y]], dtype=float)

    @property
    def velocity(self):
        import numpy as np
        return np.array([[self.vx], [self.vy]], dtype=float)


    @property
    def velocity_xy(self):
        return self.velocity

    @property
    def shape(self) -> str:
        return "polygon" if self.polygon else "circle"

    @property
    def geometry(self):
        """Only cbf_qp.py's diff-mode nearest-point lookup for non-circle
        obstacles actually needs this. imported lazily so nothing else
        that touches ObstacleObservation needs shapely installed."""
        from shapely.geometry import Point, Polygon
        return Polygon(self.polygon) if self.polygon else Point(self.x, self.y).buffer(self.radius or 0.0)

    @property
    def unobstructed(self) -> bool:
        return False
