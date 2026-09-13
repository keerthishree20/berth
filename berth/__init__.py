"""Berth: an HTTP reverse proxy and load balancer on asyncio.

Health checks, consistent hashing for sticky routing, connection pooling, and a
circuit breaker per backend. No dependencies outside the standard library.
"""

from .backend import Backend
from .balancer import Balancer, NoBackendAvailable
from .circuit import CircuitBreaker, State
from .config import BackendConfig, Config
from .hashring import HashRing
from .health import HealthChecker
from .proxy import Proxy

__version__ = "1.0.0"
__all__ = ["Proxy", "Config", "BackendConfig", "Backend", "Balancer", "HashRing",
           "CircuitBreaker", "State", "HealthChecker", "NoBackendAvailable"]
