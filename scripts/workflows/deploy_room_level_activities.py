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

from hyperstream.utils import utcnow
from hyperstream import StreamId, StreamInstance

def create_workflow_room_level_activities(hyperstream, name, safe=True):

    workflow_id = name

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere

    house_plate = hyperstream.plate_manager.plates["H"]
    camera_plate = hyperstream.plate_manager.plates["H.Cameras"]

    cameras = [
        ("b8aeede9d37e", "living room 1:A"),
        ("b8aeede9d2f5", "hall 1:A"),
        ("b8aeede9d3eb", "kitchen 1:A")
    ]

    with hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Camera_Activity_2",
            owner="MH",
            description="Activity classification per camera",
            safe=safe) as w:

        nodes = [
            ("cameras_raw", S, [house_plate]),
            ("video_splitter", M, [camera_plate]),
            ("alignment", M, None),
            ("aligned_activity", M, [camera_plate]),
            ("average_activity", M, [camera_plate]),
        ]

        N = dict()
        for stream_name, channel, plate_ids in nodes:
            print stream_name
            N[stream_name] =  w.create_node(stream_name, channel, plate_ids)

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sphere",
                parameters=dict(modality="video", elements={"uid","Activity"})
            ),
            source=None,
            splitting_node=None,
            sink=N["cameras_raw"])

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="splitter",
                parameters=dict(element="uid", mapping=dict((x, y) for x,y in cameras))
            ),
            source=N["cameras_raw"],
            splitting_node=None,
            sink=N["video_splitter"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_window",
                parameters=dict(lower=-900.0, upper=0.0, increment=900.0)
            ),
            sources=None,
            sink=N["alignment"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="chunk_activity",
                parameters=dict()
            ),
            sources=[N["alignment"], N["video_splitter"]],
            sink=N["aligned_activity"])

        def count_moving_sitting(data):
            aggregate = dict()
            for val in data:
                if val in aggregate.keys():
                    aggregate[val] += 1.0
                else:
                    aggregate[val] = 1.0

            values = dict()
            for key in aggregate.keys():
                values[key] = aggregate[key] / sum(aggregate.values())
            return values

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="apply",
                parameters=dict(func=count_moving_sitting)
            ),
            sources=[N["aligned_activity"]],
            sink=N["average_activity"])

        return w