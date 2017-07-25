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

# ignore seqnums before this one: accept that they may be lost because of network starting up
MIN_SEQNUM = 5
# accept out-of-order seqnums as long as the difference is smaller than this
MAX_OO_SEQNUM = 50
# treat seqnums <= this one as signalizing that the node has rebooted (not for wearables!)
MAX_STARTUP_SEQNUM = 100


def get_pdr(seqnums, max_startup_seqnum = MAX_STARTUP_SEQNUM):
    total = 0
    present = 0

    if len(seqnums):
        current_set = set()
        max_seqnum = None
        for s in seqnums:
            if s < MIN_SEQNUM:
                continue
            if max_seqnum is None or s > max_seqnum:
                max_seqnum = s
            delta = max_seqnum - s
            if delta > MAX_OO_SEQNUM and (max_startup_seqnum is None or s <= MAX_STARTUP_SEQNUM):
                # reboot detected
                if len(current_set):
                    total += max_seqnum - min(current_set) + 1
                    present += len(current_set)
                # reinitialize the variables
                current_set = set()
                max_seqnum = s
            # always add the s unless it's too small
            current_set.add(s)

        if len(current_set):
            total += max_seqnum - min(current_set) + 1
            present += len(current_set)

    pdr = float(present) / total if total else 0.0

    # TODO Meelis: check the format and change if it has to be something else
    return (present, total, pdr)


class PacketDeliveryRate(Tool):
    """
    For each document assumed to be a list of sequence numbers, calculate the packet delivery rate
    """
    def __init__(self, field_specific_params=dict()):
        super(PacketDeliveryRate, self).__init__(field_specific_params=field_specific_params)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for t, d in sources[0].window(interval, force_calculation=True):
            # TODO: if this is a wearable, should set pass "None" instead of MAX_STARTUP_SEQNUM
            yield StreamInstance(t, get_pdr(d, MAX_STARTUP_SEQNUM))


# --- testing code

def test(msg, result, expected_result):
    if result != expected_result:
        print("test failed:", msg, result, expected_result)
        return False
    return True


def test_all():
    test("in order", get_pdr([11, 12, 13, 14]),  (4, 4, 1.0))
    test("mixed", get_pdr([11, 12, 14, 13]),  (4, 4, 1.0))
    test("reversed", get_pdr([14, 13, 12, 11]),  (4, 4, 1.0))
    test("duplicates", get_pdr([11, 12, 13, 13, 14]),  (4, 4, 1.0))
    test("missing one", get_pdr([11, 12, 14]),  (3, 4, 0.75))
    test("missing two", get_pdr([11, 14]),  (2, 4, 0.5))
    test("empty sequence", get_pdr([]),  (0, 0, 0.0))
    test("small only", get_pdr([1, 2, 3]),  (0, 0, 0.0))
    test("with reboot", get_pdr([100, 101, 102, 103, 5, 6, 7, 8]),  (8, 8, 1.0))
    test("with reboot and missing", get_pdr([100, 101, 6, 8]),  (4, 5, 0.8))
    test("with reboot two times", get_pdr([1000, 56, 5]),  (3, 3, 1.0))
    print("all tests done")

# test_all()
