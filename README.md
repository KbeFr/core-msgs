# core_msgs

The shared contract of the framework. Every node (aggregate, instance, simulator, wrapper) depends on
this package, and nothing in it depends on a node. It holds pure data definitions, the transport-free
handshake protocol and small math utilities.

```bash
pip install -e core_msgs      # uses PyYAML and scipy at runtime
```

## Contents

| Module | Purpose |
|---|---|
| `topic_contract.py` | `MessageType` enum, `TOPIC_SPECS` (topic name, payload class and QoS per type), `register_node_topics()` |
| `agents_contract.py` | `AgentKind` (UGV/UAV), `State2D`, `Velocity2D` |
| `global_msgs/` | `AgentDiscoveryMessage`, `InstanceDiscoveryMessage`, `RegisteredMessage`, `HeartBeatMessage` |
| `instance_aggregate/payloads.py` | `TwinStatePayload`, `ObstacleObservation` |
| `instance_aggregate/mission.py` | `Mission`, `MissionType`, `MissionStatus`, `MissionPosture` and planner weights |
| `instance_aggregate/handshake*.py` | Generic assignment handshake, plus the mission extension (bidding, completion) |
| `instance_agent/` | Agent ↔ instance payloads: pose, IMU, wheel speed, battery, GPS, detections, motion commands |
| `simulation_aggregate/` | Simulator spawn and startup messages, plus GUI command payloads that are not wired up yet |
| `utils/frames.py` | `Frame`: 3D pose and rigid transform, with `apply`, `compose`, `inverse` |
| `utils/dispatch.py` | `@handles(MessageType)` and `MessageDispatcher` for routing topics to methods |
| `gui/theme.css` | Shared stylesheet for the twin GUIs |

## Topic contract

A node declares its topics as `{name: direction}` (usually in YAML) and registers them in one call:

```python
published = register_node_topics(
    node, {"mission": "inout", "twin_state": "out"},
    namespace="hdt", node_id="InstanceTwin1",
    in_callbacks={MessageType.MISSION: on_mission},
)
```

To add a message type:
1. Define the dataclass.
2. Add a `MessageType` member.
3. Add its entry to `TOPIC_SPECS`.

Nodes pick it up through their topic config.

## Handshake protocol

One transport-free (sans-I/O) state machine covers every conversation where the aggregate assigns a
*subject* to an instance: pairing an agent, or awarding a mission. It only returns envelopes; the
caller owns the transport.

| Flow | Sequence | Used for |
|---|---|---|
| Pooled | `REQUEST` (no target) → first `ACK` wins → `ACK`; `CANCEL` to late replies | Broadcast pairing |
| Directed | `REQUEST(target)` → `ACK` → `ACK` | `first_free` / `gui` pairing, direct mission assignment |
| Election | `REQUEST(target, hint)` → `BID` → `ACK` to the winner, `CANCEL` to the losers | Auction pairing, mission bidding |
| Release | `CANCEL` → `CANCEL_ACK` | Abort, eviction, lost agent |
| Completion | `COMPLETE` → `COMPLETE_ACK` | Missions only (`mission_handshake.py`) |

- `HandshakeInitiator` is the aggregate side (one per subject). `HandshakeResponder` is the instance side (one per slot).
- `HandshakeElection` sends the request to all candidates, picks a winner through a callback, and hands over the winner's initiator.
- Every attempt carries an **epoch**, so replies to older attempts are dropped. Timeouts and bounded cancel retries handle lost messages.
- New statuses are added by subclassing (`expand_states`). See `MissionInitiator` and `MissionResponder`.

## Frames

A `Frame` is a pose that also works as a transform from its own frame into its parent frame.
Quaternions are `(x, y, z, w)`, in ROS/OpenCV order.

```python
sensor_in_world = Frame.from_2d(x, y, yaw).compose(mount)   # body pose ∘ sensor mount
marker_in_world = sensor_in_world.compose(marker_in_sensor)
```

## Tests

```bash
pytest core_msgs/tests
```