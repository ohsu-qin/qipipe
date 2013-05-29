VOL_CLUSTER_TEST_OPTS = dict(
    max_thresh=1,
    min_thresh=1)
"""MinVolCluster parameters which effectively disable masking for test purposes."""

ANTS_REG_TEST_OPTS = dict(
    number_of_iterations=[[15, 2], [10, 5, 3]])
"""ANTS parameters which throttle down registration for test purposes."""
