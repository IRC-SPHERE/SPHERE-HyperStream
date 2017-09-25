# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from hyperstream import Tool, StreamInstance
from hyperstream.utils import check_input_stream_count

from dateutil.parser import parse
from datetime import datetime, timedelta
from pytz import UTC
import numpy


class ChallengeWearableLoader(Tool):
    def __init__(self, data_path, script_ID):
        super(ChallengeWearableLoader, self).__init__(data_path=data_path, script_ID=script_ID)

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):

        start_time = datetime(2017,1,1,0,0).replace(tzinfo=UTC)

        raw = numpy.genfromtxt(self.data_path + self.script_ID + '/acceleration.csv',
                               delimiter=',', skip_header=1)

        for line in raw:
            dt = start_time + timedelta(seconds=line[0])
            if dt in interval:
                yield StreamInstance(dt, line[1:])
