# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
"""
Module containing some SPHERE specific helpers:
    - useful constants
    - predefined tools (with their parameters)
"""
from hyperstream.itertools2 import online_average
from hyperstream.time_interval import parse_time_tuple, TimeIntervals

from datetime import timedelta
from scipy import integrate
import numpy as np
import sys
import json
import os

second = timedelta(seconds=1)
minute = timedelta(minutes=1)
hour = timedelta(hours=1)

environmental_aggregators = {
    'humidity': online_average,
    'dust': online_average,
    'noise': online_average,
    'temp': online_average,
    'pir': max,
    'coldwater': integrate.quad,
    'hotwater': integrate.quad,
    'electricity_tv': max,
    'electricity_total': max,
}

motion_sensors = {
    "bedroom 1": "motion-S1_B1",
    "study": "motion-S1_B0",
    "bedroom 2": "motion-S1_B2",
    "bathroom": "motion-S1_BR",
    "hallway": "motion-S1_H",
    "kitchen": "motion-S1_K",
    "lounge": "motion-S1_L",
    "stairs": "motion-S1_S",
    "toilet": "motion-S1_WC"
}


scripted_experiments = TimeIntervals(
    parse_time_tuple(*x) for x in json.load(open(os.path.join('data', 'scripted_experiments.json'))))


# assets = json.load(open(os.path.join('data', 'assets.json')))
#
# # TODO: Hard coded for SPHERE house at the moment
# mappings = {
#     "aid": assets["houses"]["1"]["access_points"],
#     "uid": assets["houses"]["1"]["wearables"]
# }

# # Also include versions without colons
# for key in mappings['uid'].keys():
#     mappings['uid'][key.replace(':', '')] = mappings['uid'][key]


def diff(x):
    x = list(x)
    if not x:
        return None
    return np.diff(x) if len(x) > 1 else None

eps = sys.float_info.epsilon


class PredefinedTools(object):
    def __init__(self, hyperstream):
        channel_manager = hyperstream.channel_manager

        # get a dict of experiment_id => annotator_id mappings
        # TODO: Reinsert Scripted experiments into meta data
        self.experiment_id_to_annotator_ids = \
            dict((n.identifier.split('.')[1].split('_')[1], n.data)
                 for n in hyperstream.plate_manager.meta_data_manager.global_plate_definitions.nodes.values()
                 if n.tag == 'annotator')

        # ENVIRONMENTAL
        self.environmental = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental"))

        self.environmental_relative_apply = channel_manager.get_tool(
            name="relative_apply2",
            parameters=dict(func=lambda kk, vv: environmental_aggregators[kk](vv))
        )

        self.environmental_humidity = channel_manager.get_tool(
            name="component",
            parameters=dict(key="humidity")
        )

        self.environmental_motion = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental", elements={"motion"})
        )

        # WEARABLE
        self.wearable = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable", elements={"rss"}))

        self.wearable_rss = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable", elements={"rss"}))

        self.wearable_rss_values = channel_manager.get_tool(
            name="component",
            parameters=dict(key="wearable-rss"),
        )

        annotator_ids = set(a for i in range(len(scripted_experiments))
                            for a in self.experiment_id_to_annotator_ids[str(i + 1)])


        # ANNOTATIONS
        self.annotations_location = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=annotator_ids,
                            elements={"Location"}, filters={"trigger": 1})
        )

        self.annotations_label = channel_manager.get_tool(
            name="component",
            parameters=dict(key="label")
        )

        self.annotations_components = channel_manager.get_tool(
            name="component_set",
            parameters=dict(keys={"annotator", "label"})
        )

        # CLOCKS
        self.clock_10s = channel_manager.get_tool(
            name="clock",
            parameters=dict(stride=10 * second)
        )

        self.clock_5m = channel_manager.get_tool(
            name="clock",
            parameters=dict(stride=5 * minute)
        )

        # RELATIVE WINDOWS and APPLY
        self.relative_window_minus10_0 = channel_manager.get_tool(
            name="relative_window",
            parameters=dict(relative_start=-10 - eps, relative_end=0)
        )

        self.relative_apply_diff = channel_manager.get_tool(
            name="relative_apply",
            parameters=dict(func=diff)
        )

        self.relative_apply_mean = channel_manager.get_tool(
            name="relative_apply",
            parameters=dict(func=online_average)
        )

        # SPLITTERS
        self.split_annotator = channel_manager.get_tool(
            name="splitter",
            parameters=dict(element="annotator", mapping=dict((x, x) for x in annotator_ids))
        )

        # self.split_aid = channel_manager.get_tool(
        #     name="splitter",
        #     parameters=dict(element="aid", mapping=mappings["aid"])
        # )
        #
        # self.split_uid = channel_manager.get_tool(
        #     name="splitter",
        #     parameters=dict(element="uid", mapping=mappings["uid"])
        # )

        self.split_aid = channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="aid")
        )

        self.split_uid = channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid")
        )

        self.split_time = channel_manager.get_tool(
            name="splitter_time_aware",
            parameters=dict(time_intervals=scripted_experiments, meta_data_id="scripted")
        )

        # AGGREGATORS
        self.index_of_1 = channel_manager.get_tool(
            name="index_of",
            parameters=dict(index="1", selector_meta_data="scripted")
        )

        self.index_of_2 = channel_manager.get_tool(
            name="index_of",
            parameters=dict(index="2", selector_meta_data="scripted")
        )
