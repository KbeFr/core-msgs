# core_msgs/topic_contract.py
import logging

from enum import Enum
import yaml

from core_msgs.global_msgs.global_payloads import AgentDiscoveryMessage
from core_msgs.instance_agent.controll_payloads import VelocityCommandMessage
from core_msgs.instance_agent.sensor_payloads import (
    BatteryMessage, ImuMessage, WheelSpeedMessage, PoseMessage, DetectionMessage, DetectedObjectSim2D, ArucoDetection,
)
from core_msgs.global_msgs.global_payloads import HeartBeatMessage, RegisteredMessage
from core_msgs.instance_aggregate.handshake_shared import HandshakeEnvelope
from core_msgs.instance_aggregate.payloads import ObstacleObservation, TwinStatePayload

from core_msgs.simulation_aggregate.payloads import (
    RobotSpawnMessage,
    SimStartupMessage,
)
from core_msgs.utils.utils import format_nested_strings

logger = logging.getLogger(__name__)

GLOBAL_SCOPE = "global"


# Cloned from flexComm PropertyBinding
class Direction(str, Enum):
    """Direction of data flow for a property."""
    IN = "in"
    OUT = "out"
    INOUT = "inout"


class MessageType(str, Enum):
    """Universal message categories used by the system."""

    # (instance <-> aggregate)
    MISSION = "mission"
    OBSTACLE = "obstacle"
    TWIN_STATE = "twin_state"
    ACTIVATE = "activate"     # instance_discovery = true
    HEARTBEAT = "heartbeat"


    POSE = "pose" #either aggregate->instance or simulated agent-> instance

    # ---------- (instance <-> agent) ----------------
    ACTION = "action"

    # Perception sensors
    ARUCO_DETECTIONS = "aruco_detections"
    SIM2D_DETECTIONS = "sim2d_detections"
    LIDAR = "lidar"

    # State sensors
    IMU = "imu"
    WHEEL_ODOM = "wheel_odom"
    BATTERY = "battery"
    ODOM = "odom"             # self integrated tb4 odom
    GPS = "gps"

    # simulation <-> aggregate twin
    SPAWN = "spawn"
    STARTUP = "startup"

    # global channel (GLOBAL_SCOPE)
    DISCOVERY = "discovery"  # (agent -> system/aggregate)
    INSTANTIATE = "instantiate"  # (aggregate -> instance) (instance_discovery = false)

    # GUI <-> aggregate // Not implemented (idea)
    GUI_COMMAND = "gui_command"  # (GUI -> AggregateDTTwin)
    SIM_CONTROL = "sim_control"  # (GUI -> AggregateSimTwin)
    SIM_STATUS = "sim_status"  # (AggregateSimTwin -> GUI)
    FLEET_SNAPSHOT = "fleet_snapshot"  # (AggregateDTTwin -> GUI)


# Pure data definitions so core_msgs doesn't depend on flexNode
TOPIC_SPECS = {

    # --- instance <-> aggregate

    MessageType.MISSION: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": HandshakeEnvelope.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 2},
    },
    MessageType.OBSTACLE: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": ObstacleObservation.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 2},
    },
    MessageType.TWIN_STATE: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": TwinStatePayload.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },
    # The per-instance instantiate channel. INSTANTIATE is globally scoped, so
    # directed and auctioned assignment travel here instead. Also carries the
    # registration ack that answers an InstanceDiscoveryMessage.
    MessageType.ACTIVATE: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": HandshakeEnvelope.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    # -------------------------- agent <-> instance ---------------------------------

    # Commands
    MessageType.ACTION: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": VelocityCommandMessage.__name__,       # internal msg_type
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    # Perception sensors

    MessageType.ARUCO_DETECTIONS: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": ArucoDetection.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    MessageType.SIM2D_DETECTIONS: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": DetectedObjectSim2D.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    MessageType.LIDAR: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": DetectedObjectSim2D.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },




    # State sensors


    MessageType.IMU: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": ImuMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },
    MessageType.WHEEL_ODOM: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": WheelSpeedMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    MessageType.ODOM: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": WheelSpeedMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    MessageType.GPS: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": WheelSpeedMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    MessageType.POSE: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": PoseMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },
    MessageType.BATTERY: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": BatteryMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    # --- sim (not used) ----

    MessageType.SPAWN: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": RobotSpawnMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },
    MessageType.STARTUP: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": SimStartupMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    # Used system-wide

    MessageType.HEARTBEAT: {
        "name": "{node_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": HeartBeatMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{node_id}/{message_type}", "QoS": 1},
    },

    # --- global messages ----
    MessageType.DISCOVERY: {
        "name": "{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": AgentDiscoveryMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{message_type}", "QoS": 1},
    },
    MessageType.INSTANTIATE: {
        "name": "{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": HandshakeEnvelope.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{message_type}", "QoS": 1},
    },


}


def get_data_name(node_id: str, msg_type: MessageType):
    """ Get data name from ´´agent_id´´ and ´´msg_type´´. wrapper of ´´get_comm_matrix_property´´ """

    rendered_spec = get_comm_matrix_property(msg_type=msg_type, node_id=node_id, namespace="")

    return rendered_spec.get("name")


def get_comm_matrix_property(msg_type: MessageType, namespace: str, node_id: str) -> dict:
    """Retrieves and dynamically renders a property spec for any protocol."""

    spec = TOPIC_SPECS.get(msg_type)
    if not spec:
        raise ValueError(f"No topic spec defined for {msg_type}")

    rendered_spec = format_nested_strings(
        spec,
        message_type=msg_type.value,
        namespace=namespace,
        node_id=node_id,
    )
    return rendered_spec


def load_topic_config(path: str) -> dict:
    """Load a topic_config.yaml (list of ``{name, dir}`` entries) to a ``{name: dir}`` dict"""

    with open(path, "r") as f:
        entries = yaml.safe_load(f) or []

    return {entry["name"]: entry["dir"] for entry in entries}


def register_node_topics(node, topic_dict: dict, namespace: str, node_id: str,
                         in_callbacks: dict | None = None) -> dict:
    """
    Register every topic in ´´topic_dict´´ against ``node.property_registry``.
    """
    in_callbacks = in_callbacks or {}
    published: dict = {}

    logger.debug(
        "[register_node_topics] node=%s namespace=%s agent_id=%s topics=%s",
        getattr(node, "name", node), namespace, node_id, topic_dict,
    )

    for msg_name, direction in topic_dict.items():
        try:
            msg_type = MessageType(msg_name)
            dir_ = Direction(direction)
        except ValueError:
            logger.warning(
                "skipping unrecognized topic entry: name=%s dir=%s", msg_name, direction
            )
            continue

        rendered = get_comm_matrix_property(msg_type=msg_type, namespace=namespace, node_id=node_id)
        node.property_registry.add_property(**rendered)
        logger.debug("[register_node_topics] registered %s dir=%s topic_name=%s",
                     msg_type.value, dir_.value, rendered.get("name"))

        if dir_ in (Direction.OUT, Direction.INOUT):
            published[msg_type] = rendered["class"]

        if dir_ in (Direction.IN, Direction.INOUT):
            callback = in_callbacks.get(msg_type)
            if callback is not None:
                node.register_data_callback(data_name=rendered["name"], callback=callback)
                logger.debug(
                    "[register_node_topics] wired inbound callback for %s -> %s",
                    rendered["name"], getattr(callback, "__name__", callback),
                )
            elif dir_ == Direction.IN:
                logger.warning(" %s registered inbound with no callback wired up", msg_type.value)
            elif dir_ == Direction.INOUT:
                # Silently having no inbound callback on an INOUT topic is how the
                # activate channel ended up dead: the callback dict was keyed by
                # INSTANTIATE while ACTIVATE was the topic being registered.
                logger.warning(" %s registered INOUT with no callback wired up (keys: %s)",
                               msg_type.value, [k.value for k in in_callbacks])

    return published