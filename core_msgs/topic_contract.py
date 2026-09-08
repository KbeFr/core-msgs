# core_msgs/topic_contract.py
import logging

from enum import Enum
import yaml

from core_msgs.global_msgs.global_payloads import DiscoveryMessage
from core_msgs.instance_agent.controll_payloads import VelocityCommandMessage
from core_msgs.instance_agent.sensor_payloads import ImuMessage, WheelSpeedMessage, PoseMessage, DetectionMessage
from core_msgs.instance_aggregate.mission_handshake import MissionEnvelope
from core_msgs.instance_aggregate.instantiate_handshake import InstantiateEnvelope
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

    # (instance <-> agent)
    ACTION = "action"
    DETECTIONS = "detections"
    IMU = "imu"
    WHEEL_ODOM = "wheel_odom"
    POSE = "pose"

    # simulation <-> aggregate twin
    SPAWN = "spawn"
    STARTUP = "startup"

    # global channel (GLOBAL_SCOPE)
    DISCOVERY = "discovery"  # (agent -> system/aggregate)
    INSTANTIATE = "instantiate"  # (aggregate -> instance)

    # GUI <-> aggregate (the fleet/mission-level GUI one layer up - NOT
    # this sim node's own local control GUI, see simulation_gui.py)
    GUI_COMMAND = "gui_command"  # (GUI -> AggregateDTTwin)
    SIM_CONTROL = "sim_control"  # (GUI -> AggregateSimTwin)
    SIM_STATUS = "sim_status"  # (AggregateSimTwin -> GUI)
    FLEET_SNAPSHOT = "fleet_snapshot"  # (AggregateDTTwin -> GUI)


# Pure data definitions so core_msgs doesn't depend on flexNode
TOPIC_SPECS = {

    # --- instance <-> aggregate

    MessageType.MISSION: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": MissionEnvelope.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 2},
    },
    MessageType.OBSTACLE: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": ObstacleObservation.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 2},
    },
    MessageType.TWIN_STATE: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": TwinStatePayload.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },

    # --- agent <-> instance ----

    MessageType.ACTION: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": VelocityCommandMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },
    MessageType.DETECTIONS: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": DetectionMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },
    MessageType.IMU: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": ImuMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },
    MessageType.WHEEL_ODOM: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": WheelSpeedMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },
    MessageType.POSE: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": PoseMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },

    # --- sim (not used) ----

    MessageType.SPAWN: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": RobotSpawnMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },
    MessageType.STARTUP: {
        "name": "{agent_id}_{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": SimStartupMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{agent_id}/{message_type}", "QoS": 1},
    },

    # --- global messages ----
    MessageType.DISCOVERY: {
        "name": "{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": DiscoveryMessage.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{message_type}", "QoS": 1},
    },
    MessageType.INSTANTIATE: {
        "name": "{message_type}",
        "description": "Channel for {message_type} communication",
        "type": "message",
        "class": InstantiateEnvelope.__name__,
        "protocol": "mqtt",
        "mqtt": {"topic": "{namespace}/{message_type}", "QoS": 1},
    },
}


def get_data_name(agent_id: str, msg_type: MessageType):
    """ Get data name from ´´agent_id´´ and ´´msg_type´´. wrapper of ´´get_comm_matrix_property´´ """

    rendered_spec = get_comm_matrix_property(msg_type=msg_type, agent_id=agent_id, namespace="")

    return rendered_spec.get("name")


def get_comm_matrix_property(msg_type: MessageType, namespace: str, agent_id: str) -> dict:
    """Retrieves and dynamically renders a property spec for any protocol."""

    spec = TOPIC_SPECS.get(msg_type)
    if not spec:
        raise ValueError(f"No topic spec defined for {msg_type}")

    rendered_spec = format_nested_strings(
        spec,
        message_type=msg_type.value,
        namespace=namespace,
        agent_id=agent_id,
    )
    return rendered_spec


def load_topic_config(path: str) -> dict:
    """Load a topic_config.yaml (list of ``{name, dir}`` entries) to a ``{name: dir}`` dict"""

    with open(path, "r") as f:
        entries = yaml.safe_load(f) or []

    return {entry["name"]: entry["dir"] for entry in entries}


def register_node_topics(node, topic_dict: dict, namespace: str, agent_id: str,
                         in_callbacks: dict | None = None) -> dict:
    """
    Register every topic in ´´topic_dict´´ against ``node.property_registry``.
    """
    in_callbacks = in_callbacks or {}
    published: dict = {}

    logger.debug(
        "[register_node_topics] node=%s namespace=%s agent_id=%s topics=%s",
        getattr(node, "name", node), namespace, agent_id, topic_dict,
    )

    for msg_name, direction in topic_dict.items():
        try:
            msg_type = MessageType(msg_name)
            dir_ = Direction(direction)
        except ValueError:
            print(
                "skipping unrecognized topic entry: name= %s dir= %s", msg_name, direction
            )
            continue

        rendered = get_comm_matrix_property(msg_type=msg_type, namespace=namespace, agent_id=agent_id)
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

    return published
