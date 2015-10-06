"""Imaging utility functions."""
import math
from collections import defaultdict
import nibabel as nib
import numpy as np
from scipy.spatial.kdtree import minkowski_distance
from pyhull.convex_hull import ConvexHull


def load(location):
    """
    :param location: the ROI mask file location
    :return: the :class:`ROI`
    """
    return ROI(location)


class ROI(object):
    """Summary information for a 3D ROI mask."""

    def __init__(self, location):
        """
        :param location: the ROI mask file location
        """
        points = self._load(location)
        self.extent = Extent(points)
        """The 3D :class:`Extent`."""
        
        slice_pts = defaultdict(list)
        for i, j, k in points:
            slice_pts[i].append(i, j))
        slice_exts = {z: Extent(pts) for z, pts in slice_pts.iteritems()}
        self.slice_extents = slice_exts
        """The 2D {slice: :class:`Extent`} dictionary."""

    def _load(self, location):
        """
        Loads the given 3D ROI mask file.
    
        :param location: the ROI mask file location
        :return: the ROI
        :rtype: :class:`ROI`
        """
        # The mask image object.
        img = nib.load(location)
        # The mask data array.
        data = img.get_data()
        # The non-zero indexes grouped by dimension.
        non_zero = data.nonzero()
        # The non-zero points as a ndarray with a shape.
        return np.transpose(non_zero)

class Extent(tuple):
    """
    The line segments which span the largest volume or area
    between a set of points.
    """

    def __new__(klass, points, scale=None):
        """
        :param points: the points array
        :param tuple scale: the anatomical dimension scaling factors
            (default unit scale)
        """
        # Cast the points to an ndarray, if necessary.
        points = np.array(points)
        # Scale the points, if necessary.
        scaled = points * scale if scale else points
        # The point dimension.
        point_cnt, dim = points.shape
        # The area or volume.
        unit_area_or_volume = scale.prod() if scale else 1
        area_or_volume = point_cnt * unit_area_or_volume
        if dim == 2:
            self._area = area_or_volume
        elif dim == 3:
            self._volume = area_or_volume
        else:
            raise ExtentError("%d-dimensional extent is not supported" % dim)
        # The distances between points.
        dists = self._manhattan_distances(scaled)
        # Start with the longest segment.
        longest = self._longest_segment(scaled, dists)
        # The longest segments in the orthogonal planes.
        orthogonal = self._orthogonal_extents(scaled, longest)
        dist = lambda segment: self._manhattan_distance(*segment)
        widest, deepest = sorted(orthogonal, key=dist)
        super(Extent, klass).__new__((longest, widest, deepest))

    @property
    def area(self):
        if not self._area:
            raise ExtentError("This 3D extent has a volume rather than an area")
        return self._area

    @property
    def volume(self):
        if not self._volume:
            raise ExtentError("This 2D extent has an area rather than a volume")
        return self._volume

    def _boundary(self, points):
        """
        :param ndarray points: the points array
        :return: the vertex points
        :rtype: ndarray
        """
        # The hull vertices as a [[from, to]] list of indexes
        # into the points array.
        vertex_edge_indexes = ConvexHull(points).vertices
        # Pluck the first dimension from the vertex indexes array,
        # since we only need the points, not the edges, i.e.
        # convert from [[from, to]] to [from].
        vertex_indexes = np.array(vertex_edge_indexes)[:, 0]

        # Convert the vertex indexes to points.
        return points[vertex_indexes][:, 0]

    def _longest_segment(self, points, distances):
        """
        Returns the maximal distance segment tuple for the given
        points, where the segment tuple consists of two points.

        :param points: the point index tuples to consider
        :param distances: the point-to-point distance N x N array,
            where N is the number of points
        :return: the maximal segment point pair tuple
        """
        max_dist_flat_ndx = distances.argmax()
        p1_ndx, p2_ndx = np.unravel_index(flat_ndx, dists.shape)

        return (points[p1_ndx], points[p2_ndx])

    def _orthogonal_extents(self, points, reference):
        """
        Returns the maximal segments orthogonal to the given reference
        segment.
        
        :param points: the point index tuples
        :param reference: the segment to query against
        :return: the orthogonal segment pair tuple
        """
        # The point furthest from the reference end points
        # is the first orthogonal point.
        p1 = self._furthest_point(points, reference)
        # The point opposite to p1 with respect to the reference
        # segment is the second orthogonal point.
        p2 = self._opposite_point(points, reference, p1)

    def _manhattan_distances(self, points):
        """
        :param points: the point pairs
        :return: a M x N matrix of the (i,j) point distances,
            where M is floor(len(points)), N is ceiling(len(points)),
            i in range(M) and j in range(M, N)
        :rtype ndarray
        """
        mid = len(points)/2
        dists = [[minkowski_distance(a, b, p=1) for a in points[:mid]]
                 for b in points[mid:]]

        return np.array(dists)

    def _manhattan_distance(self, p, q):
        """
        Returns the Manhattan distance between the given points.

        :param p: the first point index tuple
        :param q: the second point index tuple
        :return: the distance between the points
        """
        return sum(abs(p[i] - q[i])) for i in range(len(p1)))

    def _furthest_point(self, points, reference):
        """
        Returns the point furthest from the given reference point pair.

        :param points: the points to query
        :param reference: the segment point pair to query against
        :return: the point furthest from the given reference points
        """
        ref1, ref2 = reference

        # The mininmal delta between a point and the reference end points
        # is initialized to the reference segment length, since the
        # furthest point delta must at least be less than that length.
        min_delta = self._manhattan_distance(ref1, ref2)
        furthest = None
        for p in points:
            d1 = self._manhattan_distance(p, ref1)
            d2 = self._manhattan_distance(p, ref2)
            delta = abs(d2 - d1)
            if delta < min_delta:
                min_delta = min_delta
                furthest = p

        return p

    def _opposite_point(self, points, axis, reference):
        """
        Returns the point in the given points that is opposite to
        the reference with respect to the axis segment.
        
        :param points: the points to query
        :param axis: the segment point pair
        :param reference: the point to compare against
        """
        # Rotate the points about the axis.
        





    def _dist_squared(self, p1, p2):
        """
        Returns the distance squared between the given points.

        :param p1: the first point index tuple
        :param p2: the second point index tuple
        :return: the distance squared between the points
        """
        return sum((p1[i] - p2[i])**2 for i in range(0, len(p1)))


    def roi_maximal_slice(roi):
        """
        :param: the ROI mask
        :return: the slice number with maximal planar extent
        """
    

    def roi_extent(roi):
        """
        :param: the ROI mask
        :return: the maximal (length, width, breadth)
        """


class Segment(object):
    """Encapsulation of a (begin, end) point pair."""

    def __init__(self, begin, end, scale=None):
        """
        :param begin: the starting point tuple
        :param end: the ending point tuple
        :param scale: the anatomical dimension scaling factors
            (default unit array)
        """
        self.points = np.array(begin, end)
        """The segment end points pair."""
        
        self.scale = scale or np.ones(self.points.shape, np.int8)
        """The anatomical dimension scaling factors."""

    def __len__(self):
        """
        :return: the anatomical Cartesian distance between this
            segment's end points
        """
        p, q = self.points * scale

        return minkowski_distance(p, q)
