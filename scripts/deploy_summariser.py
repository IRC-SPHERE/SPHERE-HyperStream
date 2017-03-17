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

import logging
from dateutil.parser import parse

globs = {'sphere_connector': None}


def run(delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from workflows.deploy_summariser import create_workflow_coord_plate_creation, create_workflow_summariser
    from sphere_connector_package.sphere_connector import SphereConnector
    from workflows.asset_splitter import split_sphere_assets

    hyperstream = HyperStream(loglevel=loglevel)

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    split_sphere_assets(hyperstream)

    workflow_id = "coord3d_plate_creation"
    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)
    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        w = create_workflow_coord_plate_creation(hyperstream, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    time_interval = TimeInterval.now_minus(minutes=1)
    w.execute(time_interval)

    workflow_id = "periodic_summaries"
    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)
    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:

        w = create_workflow_summariser(hyperstream,
                                       env_window_size=  1 * 60 * 60.0,
                                       rss_window_size=  4 * 60 * 60.0,
                                       acc_window_size=  4 * 60 * 60.0,
                                       vid_window_size=  4 * 60 * 60.0,
                                       pred_window_size= 4 * 60 * 60.0,
                                       safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    time_interval = TimeInterval.now_minus(minutes=1)
    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import ArgumentParser
    args = ArgumentParser.logging_parser(default_loglevel=logging.INFO)
    run(delete_existing_workflows=True, loglevel=args.loglevel)

