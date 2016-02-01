"""ROI utility functions."""
import itertools
from collections import defaultdict
import nibabel as nib
import numpy as np
from scipy.spatial import ConvexHull
from scipy.spatial.kdtree import minkowski_distance
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from qiutil.collections import concat


def load(location, scale=None):
    """
    Loads a ROI mask file.
    
    :param location: the ROI mask file location
    :param tuple scale: the (x, y, z) scaling factors
    :return: the :class:`ROI` encapsulation
    :rtype: ROI
    """
    # The mask image object.
    img = nib.load(location)
    # The mask data array.
    data = img.get_data()
    # The non-zero points.
    non_zero = data.nonzero()
    points = np.transpose(non_zero)
    
    # Return the ROI summary.
    return ROI(points, scale)


class ROI(object):
    """Summary information for a 3D ROI mask."""
    
    def __init__(self, points, scale=None):
        """
        :param points: the ROI mask points
        :param tuple scale: the (x, y, z) scaling factors
        """
        self.extent = Extent(points, scale)
        """The 3D :class:`Extent`."""
        
        sliced = defaultdict(list)
        for x, y, z in points:
            sliced[z].append((x, y))
        slice_scale = scale[:-1] if scale else None
        self.slices = [(z, Extent(sliced[z], slice_scale))
                       for z in sorted(sliced.iterkeys())]
        """
        The {z: :class:`Extent`} dictionary for the 2D (x, y) points
        grouped by z value.
        """
    
    def maximal_slice_index(self):
        """
        :return: the zero-based slice index with maximal planar extent
        """
        index = 0
        max_area = 0
        for i, item in enumerate(self.slices):
            _, extent = item
            if extent.area > max_area:
                index = i
                max_area = extent.area
        
        return index


class Extent(object):
    """
    The line segments which span the largest volume or area
    between a set of points.
    """
    
    def __init__(self, points, scale=None):
        """
        :param points: the points array
        :param tuple scale: the anatomical dimension scaling
            factors (default unit scale)
        """
        self.scale = scale
        """
        The anatomical dimension scaling factors (default unit scale).
        """
        # Cast the points to a ndarray, if necessary.
        points = np.asarray(points)
        # Scale the points, if necessary.
        scaled = points * scale if scale else points
        # The point dimension.
        point_cnt, dim = points.shape
        # The area or volume.
        unit_area_or_volume = np.prod(scale) if self.scale else 1
        area_or_volume = point_cnt * unit_area_or_volume
        if dim == 2:
            self._area = area_or_volume
        elif dim == 3:
            self._volume = area_or_volume
        else:
            raise ExtentError("%d-dimensional extent is not supported" % dim)
        vertices = ConvexHull(points).vertices
        self.boundary = points[vertices]
        """The convex hull boundary in image space."""
        
        # The boundary in anatomical space.
        scaled_bnd = scaled[vertices]
        factory = ExtentSegmentFactory(scaled_bnd)
        segment_indexes = np.asarray(factory.create())
        self.segments = self.boundary[segment_indexes]
        """The orthogonal extent segments."""
    
    @property
    def area(self):
        if not self._area:
            raise ExtentError("This 3D extent has a volume rather than an"
                              " area")
        return self._area
    
    @property
    def volume(self):
        if not self._volume:
            raise ExtentError("This 2D extent has an area rather than a"
                              " volume")
        return self._volume
    
    def show(self):
        """Displays the ROI boundary points and extent segments."""
        # The boundary points.
        bnd_pts = np.asarray(concat(self.boundary))
        # Scale the points, if necessary.
        if self.scale:
            bnd_pts = bnd_pts * self.scale
        # The number of axes.
        _, dim = bnd_pts.shape
        # The figure to plot.
        fig = plt.figure()
        # The figure axes.
        projection = "%dd" % dim
        axes = fig.gca(projection=projection)
        
        # Plot the boundary points as small clear circles.
        bnd_axes = [bnd_pts[:, i] for i in range(dim)]
        axes.scatter(*bnd_axes, c='w')
        
        # Plot the segment lines.
        seg_colors = ['r', 'b', 'g']
        for i, seg in enumerate(self.segments):
            seg_pts = np.asarray(concat(seg))
            if self.scale:
                seg_pts = seg_pts * self.scale
            seg_axes = [seg_pts[:, j] for j in range(dim)]
            seg_color = seg_colors[i]
            axes.plot(*seg_axes, c=seg_color)
        
        # Plot the bounding box faces.
        bb = np.asarray(self.bounding_box())
        if self.scale:
            bb = bb * self.scale
        # The following obscure code converts the bounding box axes
        # into six faces for 3D, or 4 faces for 2D.
        # TODO - find a simpler way of doing this.
        bbt = np.transpose(bb)
        prod = list(itertools.product(*bbt))
        faces = np.asarray([[[p for p in prod if p[i] == x] for x in bbt[i]]
                            for i in range(dim)]).reshape(dim*2, 4, dim)
        for face in faces:
            # Flip the last 2 vertices to get a continuous shape.
            face = face.tolist()
            face = face[:2] + list(reversed(face[2:]))
            face.append(face[0])
            face = np.asarray(face)
            face_axes = [face[:, i] for i in range(dim)]
            axes.plot(*face_axes, c='Moccasin')
        
        # Display the boundary, segments and bounding box faces.
        plt.show()
    
    def bounding_box(self):
        """
        Returns the (least, most) points of a rectangle
        circumscribing the extent.
        
        :return: the (least, most) rectangle points
        :rtype: tuple
        """
        # The smallest axis values.
        least = self.boundary.min(0)
        # The largest axis values.
        most = self.boundary.max(0)
        
        # The bounding axes.
        return (least, most)


class ExtentSegmentFactory(object):
    """
    A utility factory class that computes the extent line segments from
    a set of convex hull vertex points.
    """
    
    def __init__(self, points):
        """
        :param points: the convex hull vertex points
        """
        self.points = np.asarray(points)
        """The ndarray of boundary points."""
        
        self.distances = self._distances(self.points)
        """
        The N x N point distance array, where N is the number
        of points and ``self.distances[i][j]`` is the distance
        from ``self.points[i]`` to ``self.points[j]``.
        """
    
    def create(self):
        """
        Returns the orthogonal segments end point indexes as the
        tuple (longest, widest, deepest), where each of the tuple
        elements is a (from, to) segment end point pair of indexes
        into the :attr:`points`, e.g.::
            
            >>> points.shape
            (128, 3)
            >>> factory = ExtentSegmentFactory(points)
            >>> segment_indexes = factory.create()
            >>> segment_indexes
            ((34, 12), (122, 14), (48, 111))
            >>> segments = points[segments]
            >>> np.all(np.equal(segments[0][0], points[34]))
            True
            >>> np.all(np.equal(segments[0][1], points[12]))
            True
        
        The bounding segments procedure is as follows:
        
        * Find the length segment *(r1, r2)* which maximizes the
          Cartesian distance between points.
        
        * Find the point *r3* furthest from *r1* and *r2*.
        
        * Compute the point *o* orthogonal to *r3* on the segment
          *(r1, r2)*.
        
        * The width segment is then *(r3, r4)*, where the point
          *r4* minimizes the angle between the segments *(r3, p)*
          and *(r3, o)* for all points p.
        
        * Iterate on a generalization of the above algorithm to
          find the depth segment *(r5, r6)*, where:
          
          - *r5* maximizes the distance to the plane formed by the
             length segment *(r1, r2)* and the width segment
             *(r3, r4)*.
          
          - *r6* is the point which is most orthogonal to the length
             and width segments.
        
        :return: the orthogonal segment end point index tuples
        :rtype: list
        """
        # The point dimension.
        _, dim = self.points.shape
        segments = []
        for i in range(0, dim):
            longest = self._longest_segment(*segments)
            segments.append(longest)
        
        return segments
    
    def _longest_segment(self, *reference):
        """
        Returns the maximal distance segment index tuple (i, j),
        where i and j are indexes into self.points. If there are
        reference segments, then the segments are adjusted to be
        as orthogonal to the reference segments as the point space
        allows.
        
        :param reference: the segment point index tuples to
            constrain the query against
        :return: the maximal segment point index pair
        :rtype: tuple
        """
        if not reference:
            flat_ndx = self.distances.argmax()
            return np.unravel_index(flat_ndx, self.distances.shape)
        
        # The number of reference segments.
        ref_cnt = len(reference)
        # The point count and dimension
        points_cnt, dim = self.points.shape
        # The reference segment point pairs.
        rs = self.points[reference, :]
        # The reference segments starting points are the offsets.
        offsets = rs[:, 0]
        # The offset indexes.
        offset_ndxs = [seg[0] for seg in reference]
        # The reference end point indexes.
        ref_end_ndxs = [seg[1] for seg in reference]
        # Flatten the reference segment indexes into the offset
        # and end point indexes in segment order.
        ref_ndxs = concat(*reference)
        # The reference unit vectors.
        ruvs = self._units(rs[:, 1], offsets)
        # The reference unit vector axes.
        ruvt = ruvs.transpose()
        
        # Mask out references in the points and distances for the
        # calculations below.
        masked_pts = np.ma.asarray(self.points)
        masked_pts[ref_ndxs] = np.ma.masked
        masked_dists = np.ma.asarray(self.distances)
        masked_dists[ref_ndxs, :] = np.ma.masked
        # Mask out distances of a point to itself.
        np.fill_diagonal(masked_dists, np.ma.masked)
        if ref_cnt == 1:
            # The point furthest from the reference points is the first
            # orthogonal segment end point.
            furthest_ndx = self._furthest_point(masked_dists, *ref_ndxs)
        elif ref_cnt == 2:
            # The furthest point of the third segment maximizes the
            # distance to the plane formed by the reference points.
            cross = np.cross(*ruvs)
            cuv = cross / np.sqrt(np.dot(cross, cross))
            prdists = [np.abs(np.dot(cuv, p)) for p in self.points]
            furthest_ndx = np.argmax(prdists)
        else:
            raise NotImplementedError("More than two reference segments is"
                                      "not supported: %d" % ref_cnt)
        
        # The furthest point.
        furthest = self.points[furthest_ndx]
        # The distances from the furthest point to the reference
        # segments starting point offsets.
        fdist = self.distances[furthest_ndx][offset_ndxs]
        # The furthest vectors relative to the offsets.
        fvs = furthest - offsets
        # The furthest unit vectors relative to the offsets.
        fuvs = self._units(furthest, offsets)
        # The cosines of the angles between the furthest and
        # reference vectors.
        fcos = [np.dot(fuv, ruvs[i]) for i, fuv in enumerate(fuvs)]
        # The point on each reference segment orthogonal to
        # the furthest point.
        odist = fcos * fdist
        ovs = np.transpose(ruvt * odist)
        # The (furthest, orthogonal) unit vectors.
        ouvs = self._units(ovs, fvs)
        
        # Mask out the furthest point, since we are looking
        # for a point besides the furthest point.
        masked_pts[furthest_ndx] = np.ma.masked
        # Translate the points relative to the offsets.
        shifted = [p - offsets for p in masked_pts]
        # The (furthest, point) unit vectors relative to the
        # offsets as a P x R x D array, where:
        # * P is the number of points
        # * R is the number of reference vectors (or segments)
        # * D is the number of point dimensions (3 is supported,
        #   but 2 probably works)
        puvs = self._units(shifted, fvs)
        
        # The (point, orthogonal) cosines in a R x P matrix.
        pcost = [np.dot(puvs[:, i, :], ouv) for i, ouv in enumerate(ouvs)]
        # Flip the R x P matrix into a P x R matrix.
        pcos_unmasked = np.transpose(pcost)
        # Work around the following numpy bug:
        # * numpy dot product passes through a masked argument, e.g.:
        #   >>> ma = np.ma.asarray([1, 1])
        #   >>> ma[0] = np.ma.masked
        #   >>> np.dot(ma, [2, 2])
        #   4
        #
        #   The work-around is to remask the dot product, e.g.:
        #   >>> ma = np.ma.asarray([[1, 1], [3, 3]])
        #   >>> dp = np.dot(ma, [2, 2])
        #   >>> ma[0][0] = np.ma.masked
        #   >>> np.dot(ma, [2, 2])
        #   >>> dp[0] = np.ma.masked
        #
        # Mask the cosine array.
        pcos = np.ma.asarray(pcos_unmasked)
        # The point array mask collapsed from one boolean per
        # point x/y/z value to one boolean per point.
        amask = [np.all(m) for m in masked_pts.mask]
        # Duplicate the boolean mask values to conform to the
        # cosine array shape.
        mask = np.repeat(amask, ref_cnt).reshape(*pcos.shape)
        # Apply the mask.
        pcos[mask] = np.ma.masked
        
        # For reference vector r, furthest vector f, point vector p,
        # orthogonal intersection vector o, (f, o) angle theta and
        # (p, o) angle gamma, (f, p) is orthogonal to r if and only
        # if theta == gamma.
        #
        # Therefore, the preferred orthogonal point is that which
        # minimizes the pcos array values.
        #
        # The cosine differences from 1 (1 = colinearity = maximum).
        deltas = np.subtract(1, pcos)
        # The cumulative cosine differences. Note that we don't need
        # to take the absolute value, since each delta is non-negative.
        delta = np.sum(deltas, axis=1)
        # The target segment end point opposite to the furthest point
        # minimizes the difference between the deltas.
        other_ndx = np.argmin(delta)
        
        # Return the best match point indexes.
        return (furthest_ndx, other_ndx)
    
    def _units(self, point, offset):
        """
        Transforms the given point array into a unit vector array
        translated to the offset.
        
        :param point: an array whose last dimension holds one or
            points to transform
        :param offset: an array whose last dimension holds one or
            offset points
        :return: the unit vector(s) translated to the offset
        """
        pv = np.subtract(point, offset)
        if pv.ndim == 1:
            # Apply the unit transformation directly to the point.
            return self._unit(pv)
        else:
            # Apply the unit transformation to each point,
            # preserving the shape of the input.
            return np.apply_along_axis(self._unit, pv.ndim - 1, pv)
    
    def _unit(self, vector):
        """
        :return: the given vector scaled to unit length
        """
        return vector / np.linalg.norm(vector)
    
    def _furthest_point(self, distances, *reference):
        """
        Returns the point furthest from the given reference point
        indexes.
        
        :param distances: the N x N point distance matrix
        :param reference: the point indexes to query against
        :return: the index of the point furthest from the
            reference points
        """
        # The point-to-reference distance matrix. Since the
        # reference rows are masked out, take the reference
        # columns of the symmetrical matrix and transpose
        # the columns to rows.
        ref_dists = distances[:, reference].transpose()
        # The distance from all reference points.
        tot_dists = np.sum(ref_dists, axis=0)
        
        return np.argmax(tot_dists)
    
    def _distances(self, points):
        """
        Returns a N x N matrix of the (i,j) point distances, where:
        * N = the number of points
        * i, j = 0, ..., N
        
        :param points: the subject points
        :return: the distance array
        :rtype ndarray
        """
        dists = [[distance(p, q) for p in self.points]
                 for q in self.points]
        
        return np.asarray(dists)


# Convenient distance alias.
distance = minkowski_distance
