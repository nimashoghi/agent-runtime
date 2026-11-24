"""The monitor worker of an agy session.

It is the same worker under a module name that agent-runtime 0.2 does not
have: the isolated interpreter that starts it (`python -I`) could find an older
installation, whose worker would give the monitored command the session's
credentials. With an older installation the worker does not start.
"""

import asyncio
import os
import sys
from pathlib import Path

from agent_runtime.monitor_worker import run

if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
