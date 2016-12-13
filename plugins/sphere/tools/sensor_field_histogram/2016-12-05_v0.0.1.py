# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
import numpy as np

class SensorFieldHistogram(Tool):
    """
    For each document assumed to be a list of numbers, calculate the histogram
    """
    def __init__(self, field_specific_params=dict()):
        super(SensorFieldHistogram, self).__init__(field_specific_params=field_specific_params)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        field = sources[0].stream_id.as_dict()['meta_data'][2][1] # ['env_field']
        params = self.field_specific_params[field]
        if params['breaks'] is not None:
            breaks = params['breaks']
        else:
            breaks = [params['first_break']+i*params['break_width'] for i in range(params['n_breaks'])]
        breaks = [-np.inf]+breaks+[np.inf]
        for t, d in sources[0].window(interval, force_calculation=True):
            yield StreamInstance(t, np.histogram(d, breaks)[0].tolist())
