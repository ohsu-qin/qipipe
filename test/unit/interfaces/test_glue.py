import os, sys, re
from nose.tools import *
from nipype.interfaces.base import Undefined

import logging
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from qipipe.interfaces.glue import Glue, GlueError

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
"""The test parent directory."""

RESULTS = os.path.join(ROOT, 'results', 'interfaces', 'glue')
"""The test results directory."""

from nipype import config
cfg = dict(logging=dict(workflow_level='DEBUG', log_directory=RESULTS, log_to_file=True),
    execution=dict(crashdump_dir=RESULTS, create_report=False))
config.update_config(cfg)

class TestGlue:
    """Glue interface unit tests."""
    
    def test_map_by_index(self):
        glue = Glue(input_names=['foo'], output_names=['bar'], foo='foobar')
        result = glue.run()
        Glue(input_names=['foo'], output_names=['bar'], foo='foobar')
        result = glue.run()
        assert_equals('foobar', result.outputs.bar, "Output field bar incorrect: %s" % result.outputs.bar)
    
    def test_aggregate(self):
        glue = Glue(input_names=['foo', 'bar'], output_names=['baz'])
        glue.inputs.foo = 'foo'
        glue.inputs.bar = 'bar'
        result = glue.run()
        assert_equals(['foo', 'bar'], result.outputs.baz, "Output field baz incorrect: %s" % result.outputs.baz)
    
    def test_disaggregate(self):
        glue = Glue(input_names=['foo'], output_names=['bar', 'baz'])
        glue.inputs.foo = ['foobar', 'foobaz']
        result = glue.run()
        assert_equals('foobar', result.outputs.bar, "Output field bar incorrect: %s" % result.outputs.bar)
        assert_equals('foobaz', result.outputs.baz, "Output field baz incorrect: %s" % result.outputs.baz)
    
    def test_mismatched_fields(self):
        assert_raises(GlueError, Glue, ['foo', 'bar', 'baz'], ['oof', 'rab'])
    
    def test_missing_field(self):
        glue = Glue(input_names=['foo'], output_names=['bar'])
        assert_raises(ValueError, glue.run)
    
    def test_optional(self):
        glue = Glue(input_names=['foo'], output_names=['bar'], mandatory_inputs=False)
        result = glue.run()
        assert_equals(Undefined, result.outputs.bar)
    
    def test_translate_transform(self):
        glue = Glue(input_names=['point', 'x', 'y'],
            output_names=['moved'],
            function=_translate, point=[1,1], x=2, y=3)
        result = glue.run()
        assert_equals([3, 4], result.outputs.moved, "Output of the translate incorrect: %s" % result.outputs.moved)
    
    def test_ambiguous_result(self):
        glue = Glue(input_names=['point', 'x', 'y'],
            output_names=['bing', 'bang', 'boom'],
            function=_translate, point=[1,1], x=2, y=3)
        assert_raises(GlueError, glue.run)
    
    def test_parse_name_transform(self):
        glue = Glue(input_names=['name'],
            output_names=['title', 'first', 'last'],
            function=_parse_name)
        glue.inputs.name = 'Samuel T. Brainsample'
        result = glue.run()
        assert_equals('Samuel', result.outputs.first, "Output first name incorrect: %s" % result.outputs.first)
        assert_equals('T.', result.outputs.middle, "Output middle name incorrect: %s" % result.outputs.middle)
        assert_equals('Brainsample', result.outputs.last, "Output last name incorrect: %s" % result.outputs.last)

def _translate(point, x, y, scale=1):
    return [scale * (point[0] + x), scale * (point[0] + y)]

def _parse_name(name):
    words = name.split()
    result = dict(last=words.pop(), first=words.pop(0))
    if words:
        result['middle'] = ' '.join(words)
    return result

if __name__ == "__main__":
    import nose
    
    nose.main(defaultTest=__name__)
