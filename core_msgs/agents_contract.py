from dataclasses import dataclass
from enum import Enum


# Centralized types, can be used to tie functionality to them?
class AgentKind(str, Enum):
    UGV = "UGV"
    UAV = "UAV"

# Default comes from config prob, so str
def parse_agent_kind(val: AgentKind | str | None, default: str = "ugv") -> AgentKind:
    if isinstance(val, AgentKind):
        return val
    if not val:
        return AgentKind(str(default).upper())
    try:
        # Tries value lookup first (e.g. "ugv" -> AgentKind.UGV)
        return AgentKind(str(val).upper())
    except (ValueError, KeyError):
        try:
            # Fallback to name lookup (e.g. "UGV" -> AgentKind['UGV'])
            return AgentKind(str(val).upper())
        except KeyError:
            return default

@dataclass
class State2D:
    x: float = 0.0
    y: float = 0.0
    theta: float = 0.0  # Omni robots just report 0.0 here

@dataclass
class Velocity2D:
    linear: float = 0.0
    angular: float = 0.0
