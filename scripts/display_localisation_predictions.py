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

from __future__ import print_function

import os
import arrow
import logging
from time import sleep
import signal

globs = {'hyperstream': None}


def display_predictions(hyperstream, time_interval, house, wearables):
    from hyperstream.utils import StreamNotFoundError
    M = hyperstream.channel_manager.mongo

    for wearable in wearables:
        try:
            predictions = M.find_stream(name='predicted_locations_broadcasted', house=house, wearable=wearable)\
                .window(time_interval).last()
        except StreamNotFoundError:
            print("No predictions in interval {} for wearable {}".format(time_interval, wearable))
            continue

        if predictions:
            print("Wearable {}:\t{}\t({})".format(
                wearable,
                predictions.timestamp,
                arrow.get(predictions.timestamp).humanize()))
            for k in sorted(predictions.value):
                print("{:>20}:\t{:0.2f}".format(k, predictions.value[k]))
        else:
            print("No predictions in interval {} for wearable {}".format(time_interval, wearable))


def run(house, wearables, loglevel=logging.CRITICAL):
    from hyperstream import HyperStream, TimeInterval

    if not globs['hyperstream']:
        globs['hyperstream'] = HyperStream(loglevel=loglevel, file_logger=None)

    display_predictions(globs['hyperstream'], TimeInterval.now_minus(minutes=1), house, wearables)
    print()

    from display_access_points import display_access_points

    display_access_points(house=house)
    print()
    # db.getCollection('WEARABLE-ISO-TIME').distinct('aid',{wts:{$gt:ISODate("2016-11-17T16:40")})


if __name__ == '__main__':
    import sys
    from os import path

    sys.path.insert(0, path.dirname(path.dirname(path.abspath(__file__))))
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, path)

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.wearable_list_parser(wearables="ABCD", default_loglevel=logging.INFO)

    def signal_handler(signal, frame):
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        run(house=args.house, wearables=args.wearables, loglevel=args.loglevel)
        sleep(1)
