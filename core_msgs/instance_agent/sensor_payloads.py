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



## --- Detections ---


@dataclass
class DetectedObjectSim2D:
    """One object seen by a sensor, relative to the agent's own body frame"""
    id: Optional[int] = None                 # sim object id today; a real ArUco tag id tomorrow
    distance: Optional[float] = None         # meters, from the agent's own center
    bearing: Optional[float] = None          # radians, relative to the agent's own heading (0 = straight ahead)
    confidence: Optional[float] = 1.0        # 0..1; sensors that can't judge quality just report 1.0


# --- ArUco-shaped marker detections -----------------------------------
# Mirrors the real geometry_msgs/Point, geometry_msgs/Quaternion

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

@dataclass
class Pose:
    position: Vector3 = field(default_factory=Vector3)
    orientation: Quaternion = field(default_factory=Quaternion)

@dataclass
class ArucoDetection:
    """marker_ids[i] corresponds to poses[i] ."""
    marker_ids: Optional[list[int]] = None
    poses: Optional[list[Pose]] = None


@dataclass
class DetectionMessage:
    """wrapper for all object detections"""
    sensor_type: str
    payload : list[DetectedObjectSim2D] | ArucoDetection
    timestamp: Optional[float] = field(default_factory=time.time)


