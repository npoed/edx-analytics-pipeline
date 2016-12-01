"""
Luigi target that marks the completion of a task which has no output.
"""
import luigi


class SentinelTarget(luigi.Target):
    """
    A target that is a sentinel value for tasks that have no output.  This target will never exist.
    """

    def __init__(self):
        """
        Initializes a SentinelTarget instance.
        """
        return

    def exists(self):
        """
        Whether or not this target exists.
        """
        return False
