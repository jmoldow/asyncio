"""The asyncio package, tracking PEP 3156."""

# This relies on each of the submodules having an __all__ variable.
from .base_events import *
from .coroutines import *
from .events import *
from .futures import *
from .tasks import *

__all__ = (base_events.__all__ +
           coroutines.__all__ +
           events.__all__ +
           futures.__all__ +
           tasks.__all__
           )
