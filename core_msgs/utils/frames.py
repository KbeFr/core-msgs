"""
base/sensors/frames.py

"""

from __future__ import annotations

from dataclasses import dataclass

from scipy.spatial.transform import Rotation

from core_msgs.utils.math import quaternion_to_yaw, yaw_to_quaternion, Vector3, Quaternion

@dataclass
class Frame:
    """A pose, or equivalently a transform from the frame it describes into
    its parent frame."""

    pose: Vector3
    quaternion: Quaternion

    def __init__(self, pose: Vector3, quaternion: Quaternion) -> None:
        self.pose = pose
        self.quaternion = quaternion

    @classmethod
    def from_2d(cls, x: float, y: float, yaw: float = 0.0, z: float = 0.0) -> "Frame":
        return cls(
            Vector3(float(x), float(y), float(z)),
            Quaternion(*yaw_to_quaternion(float(yaw)))
        )

    def update_2d(self,  x: float, y: float, yaw: float = 0.0, z: float = 0.0) -> "Frame":
        self.pose = Vector3(float(x), float(y), float(z))
        self.quaternion = Quaternion(*yaw_to_quaternion(float(yaw)))

    @classmethod
    def from_euler(cls,x: float,y: float,z: float = 0.0,
            roll: float = 0.0, pitch: float = 0.0, yaw: float = 0.0,
            degrees: bool = True,
            seq: str = "xyz",
    ) -> "Frame":
        """Construct a Frame from position and roll-pitch-yaw Euler angles."""
        rot = Rotation.from_euler(seq, [roll, pitch, yaw], degrees=degrees)
        qx, qy, qz, qw = rot.as_quat()
        return cls(
            Vector3(float(x), float(y), float(z)),
            Quaternion(qx, qy, qz, qw),
        )


    # --- transform ops ----

    def apply(self, x: float, y: float, z: float = 0.0) -> tuple[float, float, float]:
        """ add point to own pose """
        wx, wy, wz = self._rotation().apply([x, y, z])
        p = self.pose
        return float(wx + p.x), float(wy + p.y), float(wz + p.z)

    def compose(self, other: "Frame") -> "Frame":
        """ frame of other relative to own frame """
        wx, wy, wz = self.apply(other.pose.x, other.pose.y, other.pose.z)
        qx, qy, qz, qw = (self._rotation() * other._rotation()).as_quat()
        return Frame(Vector3(wx, wy, wz), Quaternion(qx, qy, qz, qw))

    def inverse(self) -> "Frame":
        """ reverse transformation of frame (make self origin) """
        inv_rot = self._rotation().inv()
        p = self.pose
        ix, iy, iz = inv_rot.apply([-p.x, -p.y, -p.z])
        qx, qy, qz, qw = inv_rot.as_quat()
        return Frame(Vector3(float(ix), float(iy), float(iz)), Quaternion(qx, qy, qz, qw))


    # --- convenience scalar accessors ------

    @property
    def x(self) -> float:
        return self.pose.x

    @property
    def y(self) -> float:
        return self.pose.y

    @property
    def z(self) -> float:
        return self.pose.z

    @property
    def yaw(self) -> float:
        """Ground-plane heading: exact if this frame is yaw-only, otherwise
        the ground-plane projection of the full 3D orientation."""
        q = self.quaternion
        return quaternion_to_yaw(q.x, q.y, q.z, q.w)

    def _rotation(self) -> Rotation:
        q = self.quaternion
        return Rotation.from_quat([q.x, q.y, q.z, q.w])
