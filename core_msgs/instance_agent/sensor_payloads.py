from dataclasses import dataclass, field
from typing import Optional
import time

@dataclass
class PoseMessage:
    name: str = "Pose"
    x: Optional[float] = None
    y: Optional[float] = None
    theta: Optional[float] = None
    linear_velocity: Optional[float] = None
    angular_velocity: Optional[float] = None
    frame_id: Optional[str] = None
    timestamp: Optional[float] = field(default_factory=time.time)

@dataclass
class ImuMessage:
    name: str = "Imu"
    linear_acceleration: Optional[list[float]] = None    # [ax, ay, az]
    angular_velocity: Optional[list[float]] = None        # [wx, wy, wz]
    orientation_yaw: Optional[float] = None                # theta, radians (2D shortcut)
    orientation_quaternion: Optional[list[float]] = None   # [x, y, z, w]
    timestamp: Optional[float] = field(default_factory=time.time)

@dataclass
class WheelSpeedMessage:
    name: str = "WheelSpeed"
    left: Optional[float] = None
    right: Optional[float] = None
    wheel_speeds: Optional[list[float]] = None   # generic fallback, any kinematics
    timestamp: Optional[float] = field(default_factory=time.time)