class BolusArrivalError(Exception):
    """Error calculating the bolus arrival."""
    pass


def bolus_arrival_index(time_series):
    """
    Determines the DCE bolus arrival time point index. The bolus arrival
    is the first occurence of a difference in average signal larger than
    double the difference from first two points.
    
    :param time_series: the 4D NIfTI scan image file path
    :return: the bolus arrival time point index
    :raise BolusArrivalError: if the bolus arrival could not be determined
    """
    import nibabel as nb
    import numpy as np
    
    nii = nb.load(time_series)
    data = nii.get_data()
    n_vols = data.shape[-1]
    signal_means = np.array([np.mean(data[:,:,:, idx])
                             for idx in xrange(n_vols)])
    signal_diffs = np.diff(signal_means)
    
    # If we see a difference in average signal larger than double the
    # difference from first two points, take that as bolus arrival.
    base_diff = np.abs(signal_diffs[0])
    for idx, diff_val in enumerate(signal_diffs[1:]):
        if diff_val > 2 * base_diff:
            return idx + 1
    else:
        raise BolusArrivalError("Unable to determine bolus arrival")
