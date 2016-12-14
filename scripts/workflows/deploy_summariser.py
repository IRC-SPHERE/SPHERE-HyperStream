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
import json
import os

def create_workflow_coord_plate_creation(hyperstream, safe=True):
    from hyperstream import TimeInterval

    workflow_id = "coord3d_plate_creation"

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="Coord3d plate creation",
            owner="MK",
            description="Coord3d plate creation",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="meta_instance_from_list",
            parameters=dict(values=['x','y','z'])
        ),
        source=None,
        output_plate=dict(
            plate_id="H.W.Coords3d",
            meta_data_id="coord",
            description="3d coordinates x y z",
            use_provided_values=False,
            parent_plate="H.W"
        ),
        plate_manager=hyperstream.plate_manager
    )

    return w




def create_workflow_summariser(hyperstream, safe=True):
    from hyperstream import TimeInterval

    workflow_id = "periodic_summaries"

    S = hyperstream.channel_manager.sphere
    D = hyperstream.channel_manager.mongo
    M = hyperstream.channel_manager.memory
    A = hyperstream.channel_manager.assets

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
        ("env_per_uid_field_windows",               M, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg",                   S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_perc",              S, ["H.EnvSensors.Fields"]),
        ("env_per_uid_field_agg_hist",              S, ["H.EnvSensors.Fields"]),
        ("rss_raw",                                 S, ["H"]),
        ("rss_per_uid",                             S, ["H.W"]),
        ("rss_per_uid_aid",                         S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value",                   S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_windows",           M, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg",               S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg_perc",          S, ["H.W","H.APs"]),
        ("rss_per_uid_aid_value_agg_hist",          S, ["H.W","H.APs"]),
        ("acc_raw",                                 S, ["H"]),
        ("acc_per_uid",                             S, ["H.W"]),
        ("acc_per_uid_acclist",                     S, ["H.W"]),
        ("acc_per_uid_acclist_coord",               S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_windows",       S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg",           S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg_perc",      S, ["H.W.Coords3d"]),
        ("acc_per_uid_acclist_coord_agg_hist",      S, ["H.W.Coords3d"]),
        ("env_sensors_by_house",                    A, ["H"]),
        ("fields_by_env_sensor",                   A, ["H.EnvSensors"]),
        ("wearables_by_house",                      A, ["H"]),
        ("access_points_by_house",                  A, ["H"])
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
            name="splitter_from_stream",
            parameters=dict(element="uid",use_mapping_keys_only=True)
        ),
        source=N["env_raw"],
        splitting_node=N["env_sensors_by_house"],
        sink=N["env_per_uid"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict()
        ),
        source=N["env_per_uid"],
        splitting_node=N["fields_by_env_sensor"],
        sink=N["env_per_uid_field"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-20.0, upper=0.0, increment=20.0)
        ),
        sources=None,
        sink=N["env_per_uid_field_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["env_per_uid_field_windows"], N["env_per_uid_field"]],
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
            name="sensor_field_histogram",
            parameters=dict(field_specific_params=dict(
                humidity   =dict(first_break=0,break_width=1,n_breaks=101,breaks=None),
                pressure   =dict(first_break=950,break_width=1,n_breaks=101,breaks=None),
                temperature=dict(first_break=0,break_width=0.5,n_breaks=101,breaks=None),
                water      =dict(first_break=0,break_width=1,n_breaks=11,breaks=None),
                light      =dict(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(-5,15)]),
                motion     =dict(first_break=0,break_width=1,n_breaks=2 ,breaks=None),
                electricity=dict(first_break=None,break_width=None,n_breaks=None,breaks=[0]+[2**i for i in range(15)])
            ))
        ),
        sources=[N["env_per_uid_field_agg"]],
        sink=N["env_per_uid_field_agg_hist"])

    w.create_multi_output_factor(
       tool=hyperstream.channel_manager.get_tool(
           name="sphere",
           parameters=dict(modality="wearable",elements={"rss"})
       ),
       source=None,
       splitting_node=None,
       sink=N["rss_raw"])

    w.create_multi_output_factor(
       tool=hyperstream.channel_manager.get_tool(
           name="sphere",
           parameters=dict(modality="wearable",elements={"xl"})
       ),
       source=None,
       splitting_node=None,
       sink=N["acc_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="uid")
        ),
        source=N["rss_raw"],
        splitting_node=N["wearables_by_house"],
        sink=N["rss_per_uid"])

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

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_from_stream",
            parameters=dict(element="aid")
        ),
        source=N["rss_per_uid"],
        splitting_node=N["access_points_by_house"],
        sink=N["rss_per_uid_aid"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_of_list",
            parameters=dict(mapping=['x','y','z'])
        ),
        source=N["acc_per_uid_acclist"],
        splitting_node=None,
        sink=N["acc_per_uid_acclist_coord"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="component",
            parameters=dict(key='wearable-rss')
        ),
        sources=[N["rss_per_uid_aid"]],
        sink=N["rss_per_uid_aid_value"])

    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sliding_window",
    #         # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
    #         parameters=dict(lower=-20.0, upper=0.0, increment=20.0)
    #     ),
    #     sources=None,
    #     sink=N["rss_per_uid_aid_value_windows"])

    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sliding_listify",
    #         parameters=dict()
    #     ),
    #     sources=[N["rss_per_uid_aid_value_windows"], N["rss_per_uid_aid_value"]],
    #     sink=N["rss_per_uid_aid_value_agg"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="percentiles_from_list",
    #         parameters=dict(n_segments=4,percentiles=None)
    #     ),
    #     sources=[N["rss_per_uid_aid_value_agg"]],
    #     sink=N["rss_per_uid_aid_value_agg_perc"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="histogram_from_list",
    #         parameters=dict(first_break=-120,break_width=1,n_breaks=101,breaks=None)
    #     ),
    #     sources=[N["rss_per_uid_aid_value_agg"]],
    #     sink=N["rss_per_uid_aid_value_agg_hist"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            # parameters=dict(lower=-3600.0, upper=0.0, increment=3600.0)
            parameters=dict(lower=-20.0, upper=0.0, increment=20.0)
        ),
        sources=None,
        sink=N["acc_per_uid_acclist_coord_windows"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_listify",
            parameters=dict()
        ),
        sources=[N["acc_per_uid_acclist_coord_windows"], N["acc_per_uid_acclist_coord"]],
        sink=N["acc_per_uid_acclist_coord_agg"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="percentiles_from_list",
            parameters=dict(n_segments=4,percentiles=None)
        ),
        sources=[N["acc_per_uid_acclist_coord_agg"]],
        sink=N["acc_per_uid_acclist_coord_agg_perc"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="histogram_from_list",
            parameters=dict(first_break=-5,break_width=0.1,n_breaks=101,breaks=None)
        ),
        sources=[N["acc_per_uid_acclist_coord_agg"]],
        sink=N["acc_per_uid_acclist_coord_agg_hist"])

    return w
