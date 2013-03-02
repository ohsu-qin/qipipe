class SimilarityMetric(object):
    """ANTS similiarity metric."""
    
    
    _FMT = "{name}[{fixed}, {moving}, {opts}]"

    def __init__(self, name, *opts):
        """
        Initializes this metric.
        
        @param name: the metric name
        @param opts: the metric options
        """
        self.name = name
        self.opts = opts
    
    def format(self, fixed, moving, weight=1):
        """
        Formats the ANTS similiarity metric argument.

        @param fixed: the fixed reference file
        @param moving: the moving file to register
        @rtype: str
        """
        opts = [weight]
        opts.extend(self.opts)
        opt_arg = ', '.join([str(opt) for opt in opts])
        return SimilarityMetric._FMT.format(name=self.name, fixed=fixed, moving=moving, opts=opt_arg)
    
    def __str__(self):
        return self.name


class PR(SimilarityMetric):
    """
    The probability mapping metric.
    """

    def __init__(self):
        super(PR, self).__init__('PR')


class PSE(SimilarityMetric):
    """
    The point set expectation metric.
    """

    def __init__(self, sampling=0.1, sigma=10, k=10):
        # The 0 argument sets the boundary points only flag to false.
        super(PSE, self).__init__('PSE', sampling, sigma, 0, k)


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
