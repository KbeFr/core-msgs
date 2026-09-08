import time
from dataclasses import dataclass, field
from typing import Optional

from core_msgs.agents_contract import AgentKind


@dataclass
class DiscoveryMessage:
    """
    DiscoveryMessage send from agent to aggregate twin in system

    args:
        :param shape: dict with shape of agent : (only name is required, the rest will be defaulted if none given)

        - {'name' : 'circle', 'radius': 0.2, 'center': None, 'random_shape': False, 'radius_range': None, 'wheelbase': None}
	    - {'name' : 'polygon', 'vertices': None, 'random_shape': False, 'is_convex': False}
		- {'name' : 'rectangle', 'length': 1.0, 'width': 1.0, 'wheelbase': None}
		- {'name' : 'linestring', 'vertices': None, 'random_shape': False, 'is_convex': True}

        :param radius: minimum_bounding_radius of the shape given, required for aggregate's estimation of shape

        :param kinematics: dict with kinematics of agent : (only name required)

        - {'name' : 'omni' , 'noise': False, 'alpha': None}
		- {'name' : 'diff' , 'noise': False, 'alpha': None}
		- {'name' : 'acker', 'noise': False, 'alpha': None}

        :param perception_sensors: !list! with dicts of perception sensors of agent :
        - {'name' : 'aruco', 'offset' : {'position' : {'x' : '' , 'y' : '' , 'z' : '' },
                                         'orientation': {'x' : '' , 'y' : '' , 'z' : '' , 'w' : ''}}}

        - {'name' : 'sim2d_object' , 'offset' : {'position' : {'x' : '' , 'y' : '' , 'z' : '' },
                                         'orientation': {'x' : '' , 'y' : '' , 'z' : '' , 'w' : ''}}}


    """

    namespace: Optional[str] = None

    agent_id: Optional[int] = None
    agent_name: Optional[str] = None

    kind : Optional[AgentKind] = None
    kinematics: Optional[dict] = None
    shape: Optional[dict] = None
    radius : Optional[float] = None # estimated robot radius for aggregate

    perception_sensors : Optional[list] = None
    state_sensors : Optional[dict] = None

    mass : Optional[float] = None       #kg
    friction : Optional[float] = None
    avg_speed : Optional[float] = None  #m/s
    max_speed : Optional[float] = None  #m/s

    topics: Optional[dict] = None

    timestamp: Optional[float] = field(default_factory=time.time)


