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
from datetime import timedelta


class Detect5Taps(Tool):
    """
    Detects 5 consecutive taps from the list of wearable timestamped magnitudes.
    The required pattern:
    2 sec of below 1.8 magnitude, then
    within 4 sec at least 3 magnitudes each above 1.8 and each higher than neighbours
    then 2 sec of below 1.8 magnitude;
    Outputs the same as input if has the required pattern.
    """

    def __init__(self):
        super(Detect5Taps, self).__init__()

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for time, data in sources[0].window(interval, force_calculation=True):
            t6 = time - timedelta(seconds=6)
            t2 = time - timedelta(seconds=2)
            tap_list = []
            ok = True
            for i in range(len(data)):
                (t,magnitude) = data[i]
                if magnitude<1.8:
                    continue
                if (t<t6) or (t>t2):
                    ok = False
                    break # too high magnitude outside of the 4 sec window
                if magnitude<1.8:
                    continue
                if (i==0) or (i==len(data)):
                    ok = False
                    break # no taps counted at the ends of the window
                if (magnitude > data[i-1][1]) and (magnitude > data[i+1][1]):
                    tap_list.append(data[i])
            if ok and (len(tap_list)>=3):
                res = ''
                wearable = [w for (s,w) in sources[0].stream_id.meta_data if s=='wearable'][0]
                res = '\n'.join(['{0} {1:.2} {2:%Y-%m-%d %H:%M:%S.%f}'.format(wearable,tap.value,tap.timestamp) for tap in tap_list])+'\n'
                print(res)
                yield StreamInstance(time, dict(tap_list=tap_list,all_10_sec=data))
