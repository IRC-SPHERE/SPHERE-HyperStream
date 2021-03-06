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
from hyperstream.utils import utcnow

import pandas as pd


class DallanDataFrameBuilder(Tool):
    """
    Converts the value part of the stream instances to json format
    """
    def __init__(self, time_interval):
        super(DallanDataFrameBuilder, self).__init__(time_interval=time_interval)
        self.time_interval = time_interval

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        data = list(sources[0].window(interval, force_calculation=True))
        flattened = map(lambda x: dict(dict(
            timestamp=x.timestamp,
            fold=next(iter(x.value['annotations']['Experiment']), None),
            location=next(iter(x.value['annotations']['Location']), None)
        ), **(x.value['rssi'])), data)
        df = pd.DataFrame(flattened)
        yield StreamInstance(interval.end, df)
