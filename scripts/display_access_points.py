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
import logging
from datetime import timedelta
from time import sleep
import signal

globs = { 'sphere_connector': None }


def display_access_points(house):
    from hyperstream.utils import utcnow
    from sphere_connector_package.sphere_connector import SphereConnector, DataWindow

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    t2 = utcnow()
    t1 = t2 - timedelta(seconds=5)

    sphere_connector = globs['sphere_connector']
    window = DataWindow(sphere_connector, t1, t2)
    docs = window.wearable.get_data(elements={'rss'}, rename_keys=False)
    aids = set(d['aid'] for d in filter(lambda x: x['hid'] == house if 'hid' in x else True, docs))

    if aids:
        print("Access points: ")
        for i, aid in enumerate(aids):
            print("{}: {}".format(i, aid))
    else:
        print("No access points found")


def run(house):
    display_access_points(house=house)
    print()


if __name__ == '__main__':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.append(path)

    def signal_handler(signal, frame):
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.house_parser(default_loglevel=logging.INFO)

    while True:
        run(args.house)
        sleep(1)
