from ..helpers.which import which

DISTRIBUTABLE = not not which('qsub')
"""Flag indicating whether the workflow can be distributed over a cluster."""
