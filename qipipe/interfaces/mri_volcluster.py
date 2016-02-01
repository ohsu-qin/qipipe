import os
from os import path
import traits.api as traits
from nipype.interfaces.base import (TraitedSpec, CommandLine,
                                    CommandLineInputSpec)
from nipype.interfaces.traits_extension import Undefined


class MriVolClusterInputSpec(CommandLineInputSpec):
    in_file = traits.File(desc='Input file', mandatory=True, exists=True,
                          argstr='--in %s')
    
    min_thresh = traits.Float(0.0, usedefault=True,
                              desc="Minimum threshold value",
                              argstr='--thmin %s')
    
    max_thresh = traits.Float(desc="Maximum threshold value",
                              argstr='--thmax %s')
    
    min_size = traits.Float(desc="Minimum cluster size in mm^3",
                            argstr='--minsize %s')
    
    min_voxels = traits.Int(desc="Minimum number of voxels in cluster",
                            argstr='--minsizevox %s')
    
    out_clusters_name = traits.Str('clusters.nii.gz', usedefault=True,
                                   desc="Name of output image with cluster labels",
                                   argstr="--ocn %s")
    
    out_masked_name = traits.Str('masked.nii.gz', usedefault=True,
                                 desc="Name of output masked image",
                                 argstr="--out %s")


class MriVolClusterOutputSpec(TraitedSpec):
    out_cluster_file = traits.File(desc="Output image containing cluster labels")
    
    out_masked_file = traits.File(desc="Input image masked by clusters")


class MriVolCluster(CommandLine):
    """
    MriVolCluster encapsulates the FSL mri_volcluster_ command.
    
    .. _mri_volcluster: http://ftp.nmr.mgh.harvard.edu/fswiki/mri_volcluster
    """
    
    input_spec = MriVolClusterInputSpec
    
    output_spec = MriVolClusterOutputSpec
    
    cmd = 'mri_volcluster'
    
    def _list_outputs(self):
        cwd = os.getcwd()
        outputs = self.output_spec().get()
        if not self.inputs.out_clusters_name is Undefined:
            outputs['out_cluster_file'] = path.join(cwd,
                                                    self.inputs.out_clusters_name)
        if not self.inputs.out_masked_name is Undefined:
            outputs['out_masked_file'] = path.join(cwd,
                                                   self.inputs.out_masked_name)
        return outputs
