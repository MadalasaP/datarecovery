from subprocess import Popen, PIPE
from logging import debug


def run_cmd(cmd):
    """Runs a Linux system command
    Args:
    cmd: Command to be executed
    Returns:
    A tuple: return code, stderr, stdout
    """
    debug("Running the command: " + cmd)
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout.decode('utf-8'), stderr.decode('utf-8')
