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
from hyperstream.stream import StreamInstance
from hyperstream import TimeInterval
from tests.utils import *


def basic_workflow(workflow_id):
    w = hyperstream.create_workflow(workflow_id=workflow_id, name="Test", owner="Tests",
                                    description="Nose test generated workflow")
    N = w.nodes

    aggregate_dev = channels.get_tool(
        name="aggregate",
        parameters=dict(func=online_average, aggregation_meta_data="wearable")
    )

    nodes = (
        ("rss_raw", M, ["H1"]),  # Raw RSS data
        ("rss_aid", M, ["H1.L"]),  # RSS by access point id
        ("rss_aid_uid", M, ["H1.L.W"]),  # RSS by access point id and device id
        ("rss", M, ["H1.L.W"]),  # RSS values only (by access point id and device id)
        ("rss_dev_avg", M, ["H1.L"]),  # Averaged RSS values by device, per location
        ("rss_loc_avg", M, ["H1.W"]),  # Averaged RSS values by location, per device
        ("rss_kitchen", M, ["H1.K.W"]),  # Averaged RSS values by location, per device
    )

    hyperstream.plate_manager.create_plate(
        plate_id="H1.K",
        description="Kitchen in SPHERE house",
        meta_data_id="location",
        values=["kitchen"],
        complement=False,
        parent_plate="H1"
    )

    hyperstream.plate_manager.create_plate(
        plate_id="H1.K.W",
        description="All wearables in kitchen in SPHERE house",
        meta_data_id="wearable",
        values=[],
        complement=True,
        parent_plate="H1.K"
    )

    # Create all of the nodes
    for stream_name, channel, plate_ids in nodes:
        w.create_node(stream_name, channel, plate_ids)

    w.create_multi_output_factor(
        tool=tools.wearable_rss,
        source=None,
        splitting_node=None,
        sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=tools.split_aid,
        source=N["rss_raw"],
        splitting_node=None,
        sink=N["rss_aid"])

    w.create_multi_output_factor(
        tool=tools.split_uid,
        source=N["rss_aid"],
        splitting_node=None,
        sink=N["rss_aid_uid"])

    w.create_factor(
        tool=tools.wearable_rss_values,
        sources=[N["rss_aid_uid"]],
        sink=N["rss"])

    w.create_factor(
        tool=aggregate_dev,
        sources=[N["rss"]],
        sink=N["rss_dev_avg"]
    )

    return w


def print_head(w, node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
    print_func("Node: {}".format(node_id))
    w.nodes[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)


# Some plate values for display purposes
h1 = (('house', '1'),)
wA = (('wearable', 'A'),)
locs = tuple(("location", loc) for loc in ["kitchen", "hallway", "lounge"])

RSS_DEV_AVG = {
    (('house', '1'), ('location', 'kitchen')): [
        StreamInstance(datetime(2015, 8, 6, 13, 35, 43,   2000, tzinfo=UTC), -85.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 45, 404000, tzinfo=UTC), -99.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 46, 806000, tzinfo=UTC), -89.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 47, 406000, tzinfo=UTC), -90.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 47, 606000, tzinfo=UTC), -90.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 47, 807000, tzinfo=UTC), -93.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 49,   8000, tzinfo=UTC), -93.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 49, 208000, tzinfo=UTC), -93.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 49, 408000, tzinfo=UTC), -93.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 49, 608000, tzinfo=UTC), -95.0)
    ],

    (('house', '1'), ('location', 'hallway')): [
        StreamInstance(datetime(2015, 8, 6, 13, 35, 36, 196000, tzinfo=UTC), -82.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 36, 396000, tzinfo=UTC), -82.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 36, 596000, tzinfo=UTC), -83.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 36, 796000, tzinfo=UTC), -82.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 36, 996000, tzinfo=UTC), -92.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 37, 197000, tzinfo=UTC), -83.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 37, 397000, tzinfo=UTC), -83.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 37, 597000, tzinfo=UTC), -84.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 37, 797000, tzinfo=UTC), -82.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 37, 997000, tzinfo=UTC), -82.0)
    ],

    (('house', '1'), ('location', 'lounge')): [
        StreamInstance(datetime(2015, 8, 6, 13, 35, 45, 604000, tzinfo=UTC), -88.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 54, 413000, tzinfo=UTC), -86.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 57, 416000, tzinfo=UTC), -85.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 57, 616000, tzinfo=UTC), -86.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 57, 816000, tzinfo=UTC), -86.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 58,  16000, tzinfo=UTC), -89.0),
        StreamInstance(datetime(2015, 8, 6, 13, 35, 58, 217000, tzinfo=UTC), -89.0),
        StreamInstance(datetime(2015, 8, 6, 13, 36,  2,  20000, tzinfo=UTC), -85.0),
        StreamInstance(datetime(2015, 8, 6, 13, 36,  7, 225000, tzinfo=UTC), -77.0),
        StreamInstance(datetime(2015, 8, 6, 13, 36,  7, 826000, tzinfo=UTC), -84.0)
    ]
}

RSS_LOC_AVG = {
    (('house', '1'), ('wearable', 'A')): [
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 36, 196000, tzinfo=UTC), value=-82.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 36, 396000, tzinfo=UTC), value=-82.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 36, 596000, tzinfo=UTC), value=-83.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 36, 796000, tzinfo=UTC), value=-82.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 36, 996000, tzinfo=UTC), value=-92.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 37, 197000, tzinfo=UTC), value=-83.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 37, 397000, tzinfo=UTC), value=-83.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 37, 597000, tzinfo=UTC), value=-84.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 37, 797000, tzinfo=UTC), value=-82.0),
        StreamInstance(timestamp=datetime(2015, 8, 6, 13, 35, 37, 997000, tzinfo=UTC), value=-82.0)
    ],
    (('house', '1'), ('wearable', 'B')): [],
    (('house', '1'), ('wearable', 'C')): [],
    (('house', '1'), ('wearable', 'D')): []
}


# noinspection PyMethodMayBeStatic
class HyperStreamWorkflowPersistenceTests(unittest.TestCase):
    def test_save_workflow(self):
        workflow_id = sys._getframe().f_code.co_name

        # hyperstream.logger.setLevel(logging.WARN)

        # First delete the workflow if it's there
        hyperstream.workflow_manager.delete_workflow(workflow_id)

        w1 = basic_workflow(workflow_id)

        time_interval = TimeInterval(scripted_experiments[0].start, scripted_experiments[0].start + 2 * minute)
        w1.execute(time_interval)

        hyperstream.workflow_manager.commit_workflow(workflow_id)

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


