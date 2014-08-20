from qiutil.which import which

DISTRIBUTABLE = not not which('qsub')
"""
Flag indicating whether the workflow can be distributed over a cluster.
This flag is True if ``qsub`` is in the execution path, False otherwise.
"""
