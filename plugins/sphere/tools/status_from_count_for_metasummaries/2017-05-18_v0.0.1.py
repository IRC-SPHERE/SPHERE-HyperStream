"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""

from hyperstream import TimeInterval
from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from copy import deepcopy


class StatusFromCountForMetasummaries(Tool):
    def __init__(self):
        super(StatusFromCountForMetasummaries, self).__init__()

    # noinspection PyCompatibility
    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        expected_status_stream = sources[0].window(interval, force_calculation=True)
        counts_stream = (x for x in sources[1].window(interval, force_calculation=False))
        counts_doc = None
        for time,expected_status in expected_status_stream:
            if counts_doc is None:
                try:
                    counts_doc = next(counts_stream)
                except:
                    pass
            if (counts_doc is not None) and (counts_doc.timestamp==time):
                ok = False
                try:
                    ok = counts_doc.value > 0
                except:
                    try:
                        ok = sum(counts_doc.value) > 0
                    except:
                        ok = sum(counts_doc.value.values())
                if expected_status==1:
                    res = 1 + ok
                else:
                    res = 0
                counts_doc = None
            else:
                if expected_status==1:
                    res = 1
                else:
                    res = 0
            yield StreamInstance(time,res)

