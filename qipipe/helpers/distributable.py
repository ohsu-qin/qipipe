from qiutil.which import which

DISTRIBUTABLE = which('qsub') != None
"""
Flag indicating whether the workflow can be distributed over a cluster.
This flag is True if ``qsub`` is in the execution path, False otherwise.
"""
