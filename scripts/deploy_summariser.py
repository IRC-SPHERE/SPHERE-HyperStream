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

globs = {
    'house': 1,
    'wearables': 'ABCD',
    'sphere_connector': None
}


def run(house, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, TimeInterval
    from workflows.deploy_summariser import create_workflow_coord_plate_creation, create_workflow_summariser
    from sphere_connector_package.sphere_connector import SphereConnector, DataWindow
    from sphere_connector_package.sphere_connector.modalities.environmental import SENSOR_MAPPINGS

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)


    house_str = str(house)

    hyperstream = HyperStream(loglevel=loglevel)
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets
    S = hyperstream.channel_manager.sphere

    assets = json.load(open(os.path.join('data', 'assets.json')))

    if False: # find and insert meta-data about all environmental sensors (should come from assets instead)
        # MONGO: db.getCollection('ENV').distinct("uid")
        sphere_connector = globs['sphere_connector']
        t1 = parse("2016-11-28T11:50Z") # note that these are here just to get a handle on the collection
        t2 = parse("2016-11-28T11:55Z")
        window = DataWindow(sphere_connector, t1, t2)
        collection = window.modalities['environmental'].collection
        sensor_uids = collection.distinct('uid')
        sensor_uid_mappings = dict()
        sensor_uid_field_mappings = dict()
        # aids = set(d['aid'] for d in docs)
        for sensor_uid in sensor_uids:
            tag= 'env_sensor'
            identifier = '{}.{}'.format(globs['house'],sensor_uid)
            parent = str(globs['house'])
            data = sensor_uid
            sensor_uid_mappings[data] = data
            try:
                hyperstream.plate_manager.meta_data_manager.insert(tag=tag,identifier=identifier,parent=parent,data=data)
            except KeyError:
                pass
            sensor_fields = collection.find({'uid':sensor_uid}).distinct('e.n')
            for sensor_field in sensor_fields:
                tag2 = 'sensor_field'
                mapped_field = SENSOR_MAPPINGS[sensor_field]
                identifier2 = '{}.{}'.format(identifier,mapped_field)
                parent2 = identifier
                data2 = mapped_field
                # sensor_uid_field_mappings[data2] = data2
                try:
                    hyperstream.plate_manager.meta_data_manager.insert(tag=tag2,identifier=identifier2,parent=parent2,data=data2)
                except KeyError:
                    pass
#        for key in ['temperature','humidity','door','hot-water','light','cold-water','noise','dust','motion','electricity','water','pressure']:
#            sensor_uid_field_mappings[key] = key
        for key in SENSOR_MAPPINGS.keys():
           sensor_uid_field_mappings[SENSOR_MAPPINGS[key]] = SENSOR_MAPPINGS[key]
        # sensor_uid_field_mappings = SENSOR_MAPPINGS

        for ap in assets['houses'][house_str]['access_points']:
            tag = 'access_point'
            identifier = '{}.{}'.format(globs['house'], ap)
            parent = str(globs['house'])
            data = assets['houses'][house_str]['access_points'][ap]
            try:
                hyperstream.plate_manager.meta_data_manager.insert(tag=tag, identifier=identifier, parent=parent,
                                                                   data=data)
            except KeyError:
                pass
        # for coord in ['x','y','z']:
        #     tag = 'coord'
        #     identifier = tag
        #     parent = "root"
        #     data = tag
        #     try:
        #         hyperstream.plate_manager.meta_data_manager.insert(tag=tag, identifier=identifier, parent=parent,
        #                                                            data=data)
        #     except KeyError:
        #         pass

        hyperstream.plate_manager.create_plate(
            plate_id="H.EnvSensors",
            description="Environmental sensors in each house",
            meta_data_id="env_sensor",
            values=[],
            complement=True,
            parent_plate="H"
        )
        hyperstream.plate_manager.create_plate(
            plate_id="H.EnvSensors.Fields",
            description="Fields of all environmental sensors in each house",
            meta_data_id="sensor_field",
            values=[],
            complement=True,
            parent_plate="H.EnvSensors"
        )
        hyperstream.plate_manager.create_plate(
            plate_id="H.APs",
            description="Access points in each house",
            meta_data_id="access_point",
            values=[],
            complement=True,
            parent_plate="H"
        )
        hyperstream.plate_manager.create_plate(
            plate_id="H.W.Coords3d",
            description="3-d coordinates",
            meta_data_id="coord",
            values=[],
            complement=True,
            parent_plate="H.W"
        )
        env_assets = dict(
            sensor_uid_mappings = sensor_uid_mappings,
            sensor_uid_field_mappings = sensor_uid_field_mappings
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
        w = create_workflow_summariser(hyperstream, house=house, env_assets=env_assets, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    t1 = parse("2016-11-28T11:50Z")
    t2 = parse("2016-11-28T11:55Z")
    t1 = parse("2016-12-06T09:00Z")
    t2 = parse("2016-12-06T09:05Z")
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
    run(globs['house'])
