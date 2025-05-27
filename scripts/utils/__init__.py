import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import config
import file
import net
import output
import proc


class BreakingLoops(Exception):
    """Helper exception for breaking out of nested loops."""

    pass
