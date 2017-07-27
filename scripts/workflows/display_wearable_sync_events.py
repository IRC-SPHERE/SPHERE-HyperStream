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


def create_workflow_list_wearable_sync_events(hyperstream, house, safe=True):
    # Various channels
    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    X = hyperstream.channel_manager.summary
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

    houses = hyperstream.plate_manager.plates["H"]
    wearables = hyperstream.plate_manager.plates["H.W"]

    # Create a simple one step workflow for querying
    workflow_id = "list_wearable_sync_events"
    with hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="list multi-tap events which potentially synchronise with the annotating sound recorder",
            owner="MK",
            description="list multi-tap events which potentially synchronise with the annotating sound recorder",
            safe=safe
            ) as w:

        nodes = (
            ("wearables_by_house",               A, [houses]),
            ("acc_raw",                          S, [houses]),
            ("acc_per_uid",                      S, [wearables]),
            ("acc_per_uid_acclist",              S, [wearables]),
            ("acc_per_uid_magnitude",            D, [wearables]),
            ("acc_per_uid_windows",              S, [wearables]),
            ("acc_per_uid_magnitude_agg",        S, [wearables]),
            ("acc_per_uid_magnitude_agg_5_taps", D, [wearables]),
            ("experiments_list",                 M, [houses]),  # Current annotation data in 2s windows
            ("experiments_dataframe",            M, [houses]),  # Current annotation data in 2s windows
        )

        # Create all of the nodes
        N = dict((stream_name, w.create_node(stream_name, channel, plate_ids))
                 for stream_name, channel, plate_ids in nodes)

        w.create_multi_output_factor(
           tool=hyperstream.channel_manager.get_tool(
               name="sphere",
               parameters=dict(modality="wearable", elements={"xl"})
           ),
           source=None,
           splitting_node=None,
           sink=N["acc_raw"])

        w.create_multi_output_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="splitter_from_stream",
                parameters=dict(element="uid")
            ),
            source=N["acc_raw"],
            splitting_node=N["wearables_by_house"],
            sink=N["acc_per_uid"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="component",
                parameters=dict(key='wearable-xl1')
            ),
            sources=[N["acc_per_uid"]],
            sink=N["acc_per_uid_acclist"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="calc_acc_magnitude",
                parameters=dict()
            ),
            sources=[N["acc_per_uid_acclist"]],
            sink=N["acc_per_uid_magnitude"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_window",
                # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
                parameters=dict(lower=-8, upper=0.0, increment=1)
            ),
            sources=None,
            sink=N["acc_per_uid_windows"])

        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="sliding_listify",
                parameters=dict(include_time=True)
            ),
            sources=[N["acc_per_uid_windows"], N["acc_per_uid_magnitude"]],
            sink=N["acc_per_uid_magnitude_agg"])

        # now need to find the 5 taps.
        # tap: magnitude above 2.0 and higher than neighbours
        # find all 4 sec intervals with at least 3 taps and no taps in the surrounding 3 sec
        # print them out
        w.create_factor(
            tool=hyperstream.channel_manager.get_tool(
                name="detect_5_taps",
                parameters=dict()
            ),
            sources=[N["acc_per_uid_magnitude_agg"]],
            sink=N["acc_per_uid_magnitude_agg_5_taps"])

        return w
