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


def run(house, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, StreamId, TimeInterval
    from hyperstream.utils import duration2str
    from workflows.display_experiments import create_workflow_list_technicians_walkarounds
    from workflows.asset_splitter import split_sphere_assets

    hyperstream = HyperStream(loglevel=loglevel)

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    A = hyperstream.channel_manager.assets

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow("asset_splitter")
        A.purge_node("wearables_by_house")
        A.purge_node("access_points_by_house")

    split_sphere_assets(hyperstream)

    hyperstream.plate_manager.delete_plate("H")
    hyperstream.plate_manager.create_plate(
        plate_id="H",
        description="All houses",
        meta_data_id="house",
        values=[],
        complement=True,
        parent_plate=None
    )

    workflow_id = "list_technicians_walkarounds"

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)

    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        w = create_workflow_list_technicians_walkarounds(hyperstream, house, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)
    time_interval = TimeInterval.up_to_now()
    w.execute(time_interval)
    
    print('number of sphere non_empty_streams: {}'.format(len(S.non_empty_streams)))
    print('number of memory non_empty_streams: {}'.format(len(M.non_empty_streams)))
    
    df = M.find_stream(name='experiments_dataframe', house=house).window().values()[0]

    if len(df) > 0:
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
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.house_parser(default_loglevel=logging.INFO)
    run(args.house, delete_existing_workflows=True, loglevel=args.loglevel)
