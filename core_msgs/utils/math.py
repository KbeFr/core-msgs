from __future__ import annotations

import math
from dataclasses import dataclass


def wrap_angle(a: float) -> float:
    """Wrap to (-pi, pi]."""
    return (float(a) + math.pi) % (2.0 * math.pi) - math.pi


def yaw_to_quaternion(yaw: float) -> tuple[float, float, float, float]:
    """(x, y, z, w), the ROS/OpenCV ordering."""
    half = float(yaw) / 2.0
    return 0.0, 0.0, math.sin(half), math.cos(half)


def quaternion_to_yaw(x: float, y: float, z: float, w: float) -> float:
    """Rotation about z. Exact for yaw-only quaternions, and the right
    ground-plane projection for a genuinely tilted one."""
    return math.atan2(2.0 * (w * z + x * y), 1.0 - 2.0 * (y * y + z * z))


@dataclass
class Vector3:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0


@dataclass
class Quaternion:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    w: float = 1.0
