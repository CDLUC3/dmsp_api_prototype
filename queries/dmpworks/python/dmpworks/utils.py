import logging
import shlex
import subprocess
from functools import wraps

import pendulum

log = logging.getLogger(__name__)


def timed(func):
    """Log execution time of a function"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = pendulum.now()
        try:
            return func(*args, **kwargs)
        finally:
            end = pendulum.now()
            diff = end - start
            log.info(f"Execution time: {diff.in_words()}")

    return wrapper


def run_process(args):
    """Run a shell script"""

    log.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            log.info(line)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)
