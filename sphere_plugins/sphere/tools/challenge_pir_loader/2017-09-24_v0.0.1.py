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

from hyperstream import MultiOutputTool, StreamInstance, StreamMetaInstance
from hyperstream.utils import check_input_stream_count

from dateutil.parser import parse
from datetime import datetime, timedelta
from pytz import UTC
import numpy


class ChallengePirLoader(MultiOutputTool):
    def __init__(self, data_path, script_ID):
        super(ChallengePirLoader, self).__init__(data_path=data_path, script_ID=script_ID)

    def _execute(self, source, splitting_stream, interval, meta_data_id, output_plate_values):

        start_time = datetime(2017,1,1,0,0).replace(tzinfo=UTC)

        raw = numpy.genfromtxt(self.data_path + self.script_ID + '/pir.csv',
                               delimiter=',', skip_header=1)

        for pv in output_plate_values:

            if len(pv) > 1:
                raise NotImplementedError("Nested plates not supported for this tool")

            ((meta_data_id, plate_value),) = pv

            for line in raw:

                if str(int(line[3])) == plate_value:

                    dt_1 = start_time + timedelta(seconds=line[0])

                    dt_0 = start_time + timedelta(seconds=line[1])

                    if dt_1 in interval:

                        instance_1 = StreamInstance(dt_1, 1)

                        yield StreamMetaInstance(instance_1, (meta_data_id, plate_value))

                    if dt_0 in interval:

                        instance_0 = StreamInstance(dt_0, 0)

                        yield StreamMetaInstance(instance_0, (meta_data_id, plate_value))
