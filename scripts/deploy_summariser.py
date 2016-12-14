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
import json
import os

globs = { 'sphere_connector': None }


def run(house, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from workflows.deploy_summariser import create_workflow_coord_plate_creation, create_workflow_summariser
    from sphere_connector_package.sphere_connector import SphereConnector, DataWindow
    from sphere_connector_package.sphere_connector.modalities.environmental import SENSOR_MAPPINGS
    from workflows.asset_splitter import split_sphere_assets

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    hyperstream = HyperStream(loglevel=loglevel)
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets
    S = hyperstream.channel_manager.sphere

    split_sphere_assets(hyperstream)

    hyperstream.plate_manager.create_plate(
        plate_id="H.W.Coords3d",
        description="3-d coordinates",
        meta_data_id="coord",
        values=[],
        complement=True,
        parent_plate="H.W"
    )

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
        w = create_workflow_summariser(hyperstream, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    t1 = parse("2016-11-28T11:50Z")
    t2 = parse("2016-11-28T11:55Z")
    t1 = parse("2016-12-06T09:00Z")
    t2 = parse("2016-12-06T09:01Z")
    t_1_2 = TimeInterval(start=t1,end=t2)
    # w.factors[0].execute(t_1_2)
    w.execute(t_1_2)

    time_interval = TimeInterval.now_minus(minutes=1)
    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))


if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from plugins.sphere.utils import get_default_parser
    args = get_default_parser(default_loglevel=logging.INFO)
    run(args.house, delete_existing_workflows=True, loglevel=args.loglevel)

