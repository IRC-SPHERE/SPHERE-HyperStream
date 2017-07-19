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
"""
Unit tests for aggregating
"""
from __future__ import print_function

import sys
import unittest

from hyperstream.itertools2 import online_average
from hyperstream import TimeInterval
from tests.utils import *
from tests.test_workflow_persistence import RSS_DEV_AVG

from contextlib import contextmanager


def basic_workflow(w):
    nodes = (
        ("rss_raw", M, ["H1"]),  # Raw RSS data
        ("rss_aid", M, ["H1.L"]),  # RSS by access point id
        ("rss_aid_uid", M, ["H1.L.W"]),  # RSS by access point id and device id
        ("rss", M, ["H1.L.W"]),  # RSS values only (by access point id and device id)
        ("rss_dev_avg", M, ["H1.L"]),  # Averaged RSS values by device, per location
        ("rss_loc_avg", M, ["H1.W"]),  # Averaged RSS values by location, per device
        ("rss_kitchen", M, ["H1.K.W"]),  # Averaged RSS values by location, per device
    )

    # Create all of the nodes
    for stream_name, channel, plate_ids in nodes:
        w.create_node(stream_name, channel, plate_ids)


# noinspection PyMethodMayBeStatic
class HyperStreamWorkflowApiTests(unittest.TestCase):
    def test_save_workflow(self):
        workflow_id = sys._getframe().f_code.co_name
        workflow_params = dict(name="Test", owner="Tests", description="API test", delete_existing=True)

        @contextmanager
        def create_workflow(name, owner, description, delete_existing=False):
            if delete_existing:
                hyperstream.workflow_manager.delete_workflow(workflow_id)
            w = hyperstream.create_workflow(workflow_id=workflow_id, name=name, owner=owner, description=description)
            yield w
            hyperstream.workflow_manager.commit_workflow(workflow_id)

        with create_workflow(**workflow_params) as w1:
            basic_workflow(w1)
            time_interval = TimeInterval(scripted_experiments[0].start, scripted_experiments[0].start + 2 * minute)
            w1.execute(time_interval)

        hyperstream.plate_manager.create_plate(
            plate_id="H1.K", description="Kitchen in SPHERE house", meta_data_id="location", values=["kitchen"],
            complement=False, parent_plate="H1")

        hyperstream.plate_manager.create_plate(
            plate_id="H1.K.W", description="All wearables in kitchen in SPHERE house", meta_data_id="wearable",
            values=[], complement=True, parent_plate="H1.K")

        class ToolHelper(object):
            def __init__(self, w):
                self.w = w

            def __getattr__(self, item):
                tool_getter = lambda parameters: hyperstream.channel_manager.get_tool(name=item, parameters=parameters)
                return lambda sources, **parameters: self.w.create_multi_output_factor(
                    tool=tool_getter(parameters),
                    source=sources,
                    splitting_node=None,
                    sink=None)

        T = ToolHelper()

        # TODO: make this work
        for house in houses:
            M.rss_raw[house] = sphere(None, modality="wearable", elements={"rss"})
            for location in house.locations:
                M.rss_aid[house][location] = T.splitter_from_stream(M.rss_raw[house], element="aid")
                for wearable in location.wearables:
                    M.rss_aid_uid[house][location][wearable] = T.splitter_from_stream(M.rss_aid[house][location], element="uid")
                    M.rss[house][location][wearable] = T.component(M.rss_aid_uid[house][location][wearable], key="wearable-rss")
                M.rss_avg[house][location] = T.aggregate(M.rss[house], func=online_average, aggregation_meta_data="wearable")







        # hyperstream.logger.setLevel(logging.WARN)

        # Now remove it from the workflow manager
        del hyperstream.workflow_manager.workflows[workflow_id]

        # And then reload it
        w2 = hyperstream.workflow_manager.load_workflow(workflow_id)

        # print_head(w, "rss", h1 + wA, locs, time_interval, 10, print)
        # print_head(w, "rss_dev_avg", h1, locs, time_interval, 10, print)

        assert all(list(w1.nodes["rss_dev_avg"].streams[k].window(time_interval).head(10)) == v
                   for k, v in RSS_DEV_AVG.items())

        assert all(list(w2.nodes["rss_dev_avg"].streams[k].window(time_interval).head(10)) == v
                   for k, v in RSS_DEV_AVG.items())

if __name__ == '__main__':
    unittest.main()


