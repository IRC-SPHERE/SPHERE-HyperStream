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

import arrow
import logging
import pandas as pd


def run(house, sync_approx_time, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from hyperstream.utils import duration2str
    from workflows.display_wearable_sync_events import create_workflow_list_wearable_sync_events
    from workflows.asset_splitter import split_sphere_assets
    from dateutil.parser import parse
    from datetime import timedelta

    hyperstream = HyperStream(loglevel=loglevel, file_logger=None)

    # Various channels
    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    X = hyperstream.channel_manager.summary
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    # if delete_existing_workflows:
    #     hyperstream.workflow_manager.delete_workflow("asset_splitter")

    # split_sphere_assets(hyperstream, house)

    hyperstream.plate_manager.delete_plate("H")
    hyperstream.plate_manager.create_plate(
        plate_id="H",
        description="All houses",
        meta_data_id="house",
        values=[],
        complement=True,
        parent_plate=None
    )

    workflow_id = "list_wearable_sync_events"

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)

    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        w = create_workflow_list_wearable_sync_events(hyperstream, house, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)
    time_interval = TimeInterval.now_minus(minutes=1)
    time_interval = TimeInterval(
        parse("2017-04-29 19:04:55.000Z"),
        parse("2017-04-29 19:05:10.000Z"))
    time_interval = TimeInterval( # DS350055.DS2 recording started 16:26:11, sync in 2s
        parse("2017-04-29 15:26:00.000Z"),
        parse("2017-04-29 15:27:00.000Z"))
    # A 2.1 2017-04-29 15:26:41.091000 --- +28s
    # A 2.2 2017-04-29 15:26:41.251000
    # A 2.5 2017-04-29 15:26:41.601000
    # A 2.7 2017-04-29 15:26:41.761000
    # A 2.0 2017-04-29 15:26:42.041000
    # A 2.8 2017-04-29 15:26:42.631000
    # A 2.0 2017-04-29 15:26:43.049001
    # A 3.8 2017-04-29 15:26:43.209000
    # A 2.9 2017-04-29 15:26:43.289000
    time_interval = TimeInterval( # DS350055.DS2 recording ended 16:34:09, sync in -5s
        parse("2017-04-29 15:34:25.000Z"),
        parse("2017-04-29 15:34:45.000Z"))
    # too gentle taps
    time_interval = TimeInterval(  # DS350054.DS2 recording started 11:55:47, sync in 2s
        parse("2017-04-29 10:56:00.000Z"),
        parse("2017-04-29 10:56:30.000Z"))
    # A 2.1 2017-04-29 10:55:24.084000 --- -25s --- WRONG, should be ~ +25s
    # A 2.8 2017-04-29 10:55:24.244000
    # A 3.1 2017-04-29 10:55:24.514000
    # A 3.1 2017-04-29 10:55:24.654000
    # A 3.2 2017-04-29 10:55:25.044000
    # A 3.5 2017-04-29 10:55:25.174000
    # A 3.4 2017-04-29 10:55:25.524000
    # A 3.9 2017-04-29 10:55:25.604000
    # A 3.3 2017-04-29 10:55:25.684000
    # A 3.8 2017-04-29 10:55:25.964001
    # A 2.3 2017-04-29 10:55:26.124001
    # A 2.8 2017-04-29 10:55:26.294000
    # A 2.0 2017-04-29 10:55:26.374000
    # time_interval = TimeInterval(  # DS350054.DS2 recording ended 12:11:39, sync by wear D put hanging ~ -8s..-2s
    #     parse("2017-04-29 11:10:40.000Z"),
    #     parse("2017-04-29 11:12:00.000Z"))
    # C put hanging ~ 2017-04-29 11:11:41
    # D put hanging ~ 2017-04-29 11:11:52 --- +25s..+15s
    time_interval = TimeInterval(  # DS350054.DS2 recording started 11:55:47, sync in 2s
        parse("2017-04-30 09:38:00.000Z"),
        parse("2017-04-30 09:39:40.000Z"))

    time_centre = parse(sync_approx_time)
    time_interval = TimeInterval(  # DS350054.DS2 recording started 11:55:47, sync in 2s
        time_centre - timedelta(seconds=40),
        time_centre + timedelta(seconds=40))


    w.execute(time_interval)

    return True
    
    print('number of sphere non_empty_streams: {}'.format(len(S.non_empty_streams)))
    print('number of memory non_empty_streams: {}'.format(len(M.non_empty_streams)))
    
    # df = M.find_stream(name='experiments_dataframe', house=house).window().values()[0]

    # if len(df) > 0:
    if False:
        # arrow.get(x).humanize()
        # df['start'] = df['start'].map('{:%Y-%m-%d %H:%M:%S}'.format)
        df['duration'] = df['end'] - df['start']
        df['start'] = map(lambda x: '{:%Y-%m-%d %H:%M:%S}'.format(x), df['start'])
        df['end'] = map(lambda x: '{:%Y-%m-%d %H:%M:%S}'.format(x), df['end'])
        # df['duration'] = map(lambda x:'{:%Mmin %Ssec}'.format(x),df['duration'])

        df['start_as_text'] = map(lambda x: arrow.get(x).humanize(), df['start'])
        df['duration_as_text'] = map(lambda x: duration2str(x), df['duration'])

        pd.set_option('display.width', 1000)
        print(df[['id', 'start_as_text', 'duration_as_text', 'start', 'end', 'annotator']].to_string(index=False))
        return True
    else:
        print("DataFrame is empty")
        return False

if __name__ == '__main__':
    import sys
    from os import path
    sys.path.insert(0, path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.wearable_tap_sync_parser(default_loglevel=logging.INFO)
    run(args.house, args.time, delete_existing_workflows=True, loglevel=args.loglevel)
