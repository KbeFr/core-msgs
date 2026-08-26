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
