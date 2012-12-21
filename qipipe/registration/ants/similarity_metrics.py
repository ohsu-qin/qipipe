class SimilarityMetric(object):
    _FMT = "{name}[{fixed}, {moving}, {opts}]"

    def __init__(self, name, *opts):
        self.name = name
        self.opts = opts

    def format(self, fixed, moving, weight=1):
        """
        Formats the ANTS similiarity metric argument.

        :param reference: the fixed reference file
        :param moving: the moving file to register
        :param weight: the weight to assign this metric (default 1)
        :rtype: str
        """
        opt_arg = ', '.join([weight] + self.opts)
        return SimilarityMetric._FMT.format(name=self.name, fixed=fixed, moving=moving, opts=opt_arg)

    def __str__(self):
        return self.name


class PR(SimilarityMetric):
    """
    The probability mapping metric.
    """

    def __init__(self):
        super(PR, self).__init__('PR')


class CC(SimilarityMetric):
    """
    The cross-correlation metric.
    """

    def __init__(self, radius=4):
        super(CC, self).__init__('CC', radius)


class MI(SimilarityMetric):
    """
    The mutual information metric.
    """

    def __init__(self, bins=32):
        super(MI, self).__init__('MI', bins)


class MSQ(SimilarityMetric):
    """
    The mean-squared metric.
    """

    def __init__(self):
        super(MSQ, self).__init__('MSQ', 0)


class JTB(SimilarityMetric):
    """
    The B-spline metric.
    """

    def __init__(self, radius=32):
        super(JTB, self).__init__('JTB', radius)
