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
import pandas as pd
import numpy as np

globs = { 'sphere_connector': None }


def display_diagnostics(house):
    from hyperstream.utils import utcnow
    from sphere_connector_package.sphere_connector import SphereConnector, DataWindow

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    t2 = utcnow()
    t1 = t2 - timedelta(seconds=60)

    sphere_connector = globs['sphere_connector']
    window = DataWindow(sphere_connector, t1, t2)
    rss = window.wearable.get_data(elements={'rss'}, rename_keys=False)
    # aids = set(d['aid'] for d in filter(lambda x: x['hid'] == house if 'hid' in x else True, docs))

    #if aids:
    #    print("Access points: ")
    #    for i, aid in enumerate(aids):
    #        print("{}: {}".format(i, aid))

    df = pd.DataFrame(rss)
    if not df.empty:
        print(df.groupby(['aid', 'uid']).agg({'wearable-rss': np.max}))
        print()
    else:
        print("No access points found")
        print()

    env = window.environmental.get_data(rename_keys=False)
    df = pd.DataFrame(env)
    if not df.empty:
        for sensor in ['electricity', 'humidity', 'light', 'pressure', 'temperature', 'water']:
            if sensor in df:
                print(df.groupby('uid').agg({sensor: [np.min, np.median, np.max]}).dropna())
            else:
                print("no {} data".format(sensor))
            print()
    else:
        print("No environmental data found")
        print()

    vid = window.video.get_data(rename_keys=False)
    df = pd.DataFrame(vid)
    if not df.empty:
        for sensor in ['video-2DCen', 'video-2Dbb', 'video-3Dbb', 'video-3Dcen', 'video-Activity', 'video-FeaturesREID', 'video-Intensity']:
            if sensor in df:
                print(sensor[6:])
                print(df.groupby('uid')[sensor].describe())
            else:
                print("no {} data".format(sensor))
            print()

        for sensor in ['video-silhouette']:
            if sensor in df:
                print(sensor[6:])
                print(df.groupby('uid').size())
            else:
                print("no {}s".format(sensor[6:]))

        for sensor in ['video-userID']:
            if sensor in df:
                print("Unique {}s: {}".format(sensor[6:], list(set(df[sensor].dropna()))))
            else:
                print("no {}s".format(sensor[6:]))

        print()
    else:
        print("No video data found")
        print()


def run(house):
    display_diagnostics(house=house)
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

    # while True:
    run(args.house)
    #    sleep(5)
