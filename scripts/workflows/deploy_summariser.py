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
from plugins.sphere.utils.sphere_helpers import mappings


def create_workflow_summariser(hyperstream, house, env_assets, safe=True):
    from hyperstream import TimeInterval

    workflow_id = "periodic_summaries"

    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    M = hyperstream.channel_manager.memory

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Live Summaries",
            owner="MK",
            description="Live summaries of all modalities",
            online=True)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    nodes = (
        ("env_raw",                                 S, ["H"]),
        ("env_per_uid",                             S, ["H.EnvSensors"]),
        ("env_per_uid_field",                       S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg",                   S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_perc",              S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_hist",              S, ["H.EnvSensors.Fields"]),
        ("rss_raw",                                 S, ["H"]),
        ("every_hour",                              M, ["H.EnvSensors.Fields"]),
        # ("every_hour",                              M, ["H.W"]),
        ("rss_per_uid",                             M, ["H.W"]),
        ("rss_per_uid_hour",                          M, ["H.W"]),
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental")
        ),
        source=None,
        splitting_node=None,
        sink=N["env_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter",
            parameters=dict(
                element="uid",
                mapping=env_assets['sensor_uid_mappings']
            )
        ),
        source=N["env_raw"],
        splitting_node=None,
        sink=N["env_per_uid"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_of_dict",
            parameters=dict(
                mapping=env_assets['sensor_uid_field_mappings']
            )
        ),
        source=N["env_per_uid"],
        splitting_node=None,
        sink=N["env_per_uid_field"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-60.0, upper=0.0, increment=60.0)
        ),
        sources=None,
        sink=N["every_hour"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["every_hour"], N["env_per_uid_field"]],
        sink=N["env_per_uid_field_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="percentiles_from_list",
            parameters=dict(n_segments=4,percentiles=None)
        ),
        sources=[N["env_per_uid_field_agg"]],
        sink=N["env_per_uid_field_agg_perc"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="histogram_from_list",
            parameters=dict(first_break=0,break_width=1,n_breaks=20,breaks=None)
        ),
        sources=[N["env_per_uid_field_agg"]],
        sink=N["env_per_uid_field_agg_hist"])


    #  w.create_multi_output_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sphere",
    #         parameters=dict(modality="wearable3")
    #     ),
    #     source=None,
    #     splitting_node=None,
    #     sink=N["rss_raw"])
    #
    # w.create_multi_output_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="splitter",
    #         parameters=dict(
    #             element="uid",
    #             mapping=mappings['uid']
    #         )
    #     ),
    #     source=N["rss_raw"],
    #     splitting_node=None,
    #     sink=N["rss_per_uid"])
    #


    # def component_wise_max(init_value=None, id_field='aid', value_field='rss'):
    #     if init_value is None:
    #         init_value = {}
    #
    #     def func(data):
    #         result = init_value.copy()
    #         for (time, value) in data:
    #             if value[id_field] in result:
    #                 result[value[id_field]] = max(result[value[id_field]], value[value_field])
    #             else:
    #                 result[value[id_field]] = value[value_field]
    #         return result
    #
    #     return func
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sliding_apply",
    #         parameters=dict(func=component_wise_max())
    #     ),
    #     sources=[N["every_hour"], N["rss_per_uid"]],
    #     sink=N["rss_per_uid_hour"])

    return w
